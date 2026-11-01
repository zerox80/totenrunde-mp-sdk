//! Client des Totenrunde-Multiplayers, unabhängig von der Spiel-Engine.
//!
//! [`Connection`] baut die WebSocket-Verbindung zum Server auf, meldet sich mit
//! Protokollversion und Sitzungstoken an (`Hello`), hält die Verbindung mit Pings offen und
//! misst dabei die Laufzeit. Das Spiel ruft einmal pro Frame [`Connection::poll`] auf und
//! bekommt die Nachrichten des Servers als [`Event`]s.
//!
//! Läuft auf dem Desktop (eigener Netzwerk-Thread mit tungstenite, TLS über rustls) und im
//! Browser (WebSocket des Browsers). Die Uhrzeit gibt der Aufrufer vor, weil es im Browser
//! keine `std::time::Instant` gibt.
//!
//! ```no_run
//! use totenrunde_mp_client::{Connection, Event, ws_url};
//! use totenrunde_mp_client::protocol::{ClientMsg, ServerMsg};
//!
//! let mut conn = Connection::connect(&ws_url("https://spiel.example.de"), None);
//! let mut events = Vec::new();
//! # let now = 0.0;
//! // in jedem Frame:
//! conn.poll(now, &mut events);
//! for event in events.drain(..) {
//!     match event {
//!         Event::Message(ServerMsg::Welcome { account }) => {
//!             if account.is_none() {
//!                 conn.send(&ClientMsg::LinkStart);
//!             }
//!         }
//!         Event::Closed(reason) => eprintln!("getrennt: {reason}"),
//!         _ => {}
//!     }
//! }
//! ```

mod transport;

pub use totenrunde_mp_protocol as protocol;
pub use transport::{Socket, SocketEvent};

use protocol::{ACCOUNT_PATH, ClientMsg, ServerMsg, Target, VERSION, WS_PATH, decode, encode};

/// Abstand der Pings in Sekunden (der Server trennt nach 30 Sekunden Stille).
pub const PING_EVERY: f64 = 4.0;

/// Was seit dem letzten `poll` passiert ist.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Verbindung steht, die Anmeldung (`Hello`) ist gesendet.
    Connected,
    /// Nachricht des Servers (Antworten auf Pings ausgenommen).
    Message(ServerMsg),
    /// Verbindung beendet oder nicht zustande gekommen (Grund für das Protokoll).
    Closed(String),
}

/// Verbindung zum Multiplayer-Server.
pub struct Connection {
    socket: Socket,
    token: Option<String>,
    open: bool,
    closed: bool,
    next_ping: f64,
    ping_id: u32,
    ping_sent: f64,
    rtt: Option<f64>,
    scratch: Vec<SocketEvent>,
}

impl Connection {
    /// Verbindung zu `url` (`ws://…` oder `wss://…`, siehe [`ws_url`]) aufbauen. Mit `token`
    /// meldet sich das Spiel mit einem gespeicherten Konto an.
    pub fn connect(url: &str, token: Option<String>) -> Self {
        Self {
            socket: Socket::connect(url),
            token,
            open: false,
            closed: false,
            next_ping: 0.0,
            ping_id: 0,
            ping_sent: 0.0,
            rtt: None,
            scratch: Vec::new(),
        }
    }

    /// Token, mit dem diese Verbindung angemeldet wird.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Verbunden und angemeldet (bis die Verbindung endet).
    pub fn is_open(&self) -> bool {
        self.open && !self.closed
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Nachricht an den Server; vor dem Verbindungsaufbau wird sie verworfen.
    pub fn send(&mut self, msg: &ClientMsg) {
        if self.is_open() {
            self.socket.send(encode(msg));
        }
    }

    /// Spieldaten über den Server an andere Mitglieder der laufenden Partie.
    pub fn relay(&mut self, to: Target, data: Vec<u8>) {
        self.send(&ClientMsg::Relay { to, data });
    }

    /// Laufzeit zum Server und zurück in Millisekunden (nach dem ersten Ping).
    pub fn rtt_ms(&self) -> Option<f32> {
        self.rtt.map(|s| (s * 1000.0) as f32)
    }

    /// Neue Ereignisse abholen und bei Bedarf pingen. `now`: fortlaufende Zeit in Sekunden.
    pub fn poll(&mut self, now: f64, out: &mut Vec<Event>) {
        if self.closed {
            return;
        }
        let mut events = std::mem::take(&mut self.scratch);
        self.socket.poll(&mut events);
        for event in events.drain(..) {
            match event {
                SocketEvent::Open => {
                    self.open = true;
                    self.socket.send(encode(&ClientMsg::Hello {
                        version: VERSION,
                        token: self.token.clone(),
                    }));
                    self.next_ping = now + 0.5;
                    out.push(Event::Connected);
                }
                SocketEvent::Message(bytes) => match decode::<ServerMsg>(&bytes) {
                    Some(ServerMsg::Pong { id }) => {
                        if id == self.ping_id {
                            self.rtt = Some(now - self.ping_sent);
                        }
                    }
                    Some(msg) => out.push(Event::Message(msg)),
                    None => {}
                },
                SocketEvent::Closed(reason) => {
                    self.closed = true;
                    out.push(Event::Closed(reason));
                    break;
                }
            }
        }
        events.clear();
        self.scratch = events;
        if self.is_open() && now >= self.next_ping {
            self.next_ping = now + PING_EVERY;
            self.ping_id = self.ping_id.wrapping_add(1);
            self.ping_sent = now;
            self.socket
                .send(encode(&ClientMsg::Ping { id: self.ping_id }));
        }
    }
}

/// WebSocket-Adresse zur Basisadresse des Servers: `https://host` → `wss://host/api/mp/ws`.
pub fn ws_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let ws = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_owned()
    };
    format!("{ws}{WS_PATH}")
}

/// Konto-Seite des Servers, optional mit Verknüpfungscode für dieses Gerät.
pub fn account_url(base: &str, link: Option<&str>) -> String {
    let base = base.trim_end_matches('/');
    match link {
        Some(code) => format!("{base}{ACCOUNT_PATH}?link={code}"),
        None => format!("{base}{ACCOUNT_PATH}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        assert_eq!(
            ws_url("https://spiel.example.de/"),
            "wss://spiel.example.de/api/mp/ws"
        );
        assert_eq!(
            ws_url("http://127.0.0.1:8080"),
            "ws://127.0.0.1:8080/api/mp/ws"
        );
        assert_eq!(
            account_url("https://spiel.example.de", Some("ABCD2345")),
            "https://spiel.example.de/konto/?link=ABCD2345"
        );
    }
}
