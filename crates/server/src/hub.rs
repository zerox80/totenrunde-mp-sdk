//! Lobbys und Weiterleitung der Spieldaten.
//!
//! Der Hub kennt alle verbundenen Spiele, ihre Konten und Lobbys. Er arbeitet synchron unter
//! einer Sperre und ohne Datenbankzugriff; was gespeichert werden muss, meldet er als
//! [`Effect`] an die Verbindung zurück. Jede Verbindung besitzt eine Warteschlange mit
//! fertig kodierten Nachrichten; läuft sie über (zu langsamer Empfänger), gibt der Hub ihren
//! Sender frei. Die Verbindung endet dann, statt den Server mit Daten zu füllen.
//!
//! Die Partie selbst berechnet das Spiel des Hosts. Der Server reicht nur weiter, prüft aber,
//! dass Absender und Empfänger in derselben laufenden Partie sind.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use totenrunde_mp_protocol::{
    Account, CODE_ALPHABET, ClientMsg, CloseReason, ErrorCode, LINK_CODE_LEN, LOBBY_CODE_LEN,
    LobbyInfo, LobbyMember, LobbyView, MAX_PLAYERS, MatchPlayer, ServerMsg, Target, encode,
    normalize_code,
};

use super::db::AccountRow;

pub type ConnId = u64;
pub type Outbox = mpsc::Sender<Bytes>;

/// Größe der Sende-Warteschlange je Verbindung (Nachrichten).
pub const OUTBOX_SIZE: usize = 512;
const LINK_TTL: Duration = Duration::from_secs(600);
const LIST_LIMIT: usize = 50;
/// Erlaubte Nachrichten pro Sekunde (Dauer) und kurzfristig (Spitze)
const RATE: f64 = 240.0;
const BURST: f64 = 480.0;

/// Was nach einer Nachricht noch außerhalb der Sperre zu tun ist.
#[derive(Debug, PartialEq)]
pub enum Effect {
    None,
    /// Sitzung in der Datenbank löschen
    Logout(String),
    /// Ergebnis speichern: Runde und Konto-ID je Spieler
    Record(u32, Vec<(i64, MatchPlayer)>),
    /// Verbindung trennen (z. B. Flut von Nachrichten)
    Disconnect,
}

struct Conn {
    /// `None`, sobald die Warteschlange übergelaufen ist (Verbindung wird getrennt)
    tx: Option<Outbox>,
    account: Option<AccountRow>,
    token: Option<String>,
    lobby: Option<String>,
    link: Option<String>,
    allowance: f64,
    last: Instant,
}

struct Member {
    conn: ConnId,
    slot: u8,
    account: i64,
    name: String,
    ready: bool,
}

struct Lobby {
    code: String,
    public: bool,
    host: ConnId,
    members: Vec<Member>,
    playing: bool,
}

impl Lobby {
    fn member(&self, conn: ConnId) -> Option<&Member> {
        self.members.iter().find(|m| m.conn == conn)
    }

    fn host_slot(&self) -> u8 {
        self.member(self.host).map(|m| m.slot).unwrap_or(0)
    }

    fn free_slot(&self) -> Option<u8> {
        (0..MAX_PLAYERS as u8).find(|s| self.members.iter().all(|m| m.slot != *s))
    }

    fn view_for(&self, you: u8) -> LobbyView {
        let mut members: Vec<LobbyMember> = self
            .members
            .iter()
            .map(|m| LobbyMember {
                slot: m.slot,
                name: m.name.clone(),
                ready: m.ready,
            })
            .collect();
        members.sort_by_key(|m| m.slot);
        LobbyView {
            code: self.code.clone(),
            public: self.public,
            host: self.host_slot(),
            you,
            playing: self.playing,
            members,
        }
    }
}

#[derive(Default)]
struct State {
    next_id: ConnId,
    conns: HashMap<ConnId, Conn>,
    lobbies: HashMap<String, Lobby>,
    links: HashMap<String, (ConnId, Instant)>,
}

#[derive(Default)]
pub struct Hub {
    state: Mutex<State>,
}

fn random_code(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    getrandom::fill(&mut bytes).expect("Betriebssystem liefert keine Zufallszahlen");
    bytes
        .iter()
        .map(|b| CODE_ALPHABET[*b as usize % CODE_ALPHABET.len()] as char)
        .collect()
}

impl State {
    fn send_bytes(&mut self, conn: ConnId, bytes: Bytes) {
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let full = c.tx.as_ref().is_some_and(|tx| {
            matches!(tx.try_send(bytes), Err(mpsc::error::TrySendError::Full(_)))
        });
        if full {
            c.tx = None;
        }
    }

    fn send(&mut self, conn: ConnId, msg: &ServerMsg) {
        self.send_bytes(conn, Bytes::from(encode(msg)));
    }

    fn error(&mut self, conn: ConnId, code: ErrorCode) {
        self.send(conn, &ServerMsg::Error { code });
    }

    fn broadcast_lobby(&mut self, code: &str) {
        let Some(lobby) = self.lobbies.get(code) else {
            return;
        };
        let views: Vec<(ConnId, Bytes)> = lobby
            .members
            .iter()
            .map(|m| {
                (
                    m.conn,
                    Bytes::from(encode(&ServerMsg::Lobby(lobby.view_for(m.slot)))),
                )
            })
            .collect();
        for (conn, bytes) in views {
            self.send_bytes(conn, bytes);
        }
    }

    fn new_lobby_code(&self) -> String {
        loop {
            let code = random_code(LOBBY_CODE_LEN);
            if !self.lobbies.contains_key(&code) {
                return code;
            }
        }
    }

    /// Verbindung verlässt ihre Lobby. Geht der Host, endet die Lobby für alle.
    fn leave_lobby(&mut self, conn: ConnId, reason_for_self: Option<CloseReason>) {
        let Some(code) = self.conns.get_mut(&conn).and_then(|c| c.lobby.take()) else {
            return;
        };
        if let Some(reason) = reason_for_self {
            self.send(conn, &ServerMsg::LobbyClosed { reason });
        }
        let Some(lobby) = self.lobbies.get_mut(&code) else {
            return;
        };
        if lobby.host == conn {
            let lobby = self.lobbies.remove(&code).expect("Lobby existiert");
            for m in lobby.members.iter().filter(|m| m.conn != conn) {
                if let Some(c) = self.conns.get_mut(&m.conn) {
                    c.lobby = None;
                }
                self.send(
                    m.conn,
                    &ServerMsg::LobbyClosed {
                        reason: CloseReason::HostLeft,
                    },
                );
            }
            return;
        }
        let slot = lobby.member(conn).map(|m| m.slot);
        lobby.members.retain(|m| m.conn != conn);
        let playing = lobby.playing;
        let others: Vec<ConnId> = lobby.members.iter().map(|m| m.conn).collect();
        if let (true, Some(slot)) = (playing, slot) {
            let bytes = Bytes::from(encode(&ServerMsg::PlayerLeft { slot }));
            for o in others {
                self.send_bytes(o, bytes.clone());
            }
        }
        self.broadcast_lobby(&code);
    }

    fn list(&self) -> Vec<LobbyInfo> {
        let mut list: Vec<LobbyInfo> = self
            .lobbies
            .values()
            .filter(|l| l.public && l.members.len() < MAX_PLAYERS)
            .map(|l| LobbyInfo {
                code: l.code.clone(),
                host: l.member(l.host).map(|m| m.name.clone()).unwrap_or_default(),
                players: l.members.len() as u8,
                playing: l.playing,
            })
            .collect();
        list.sort_by(|a, b| a.playing.cmp(&b.playing).then(b.players.cmp(&a.players)));
        list.truncate(LIST_LIMIT);
        list
    }

    fn join(&mut self, conn: ConnId, account: AccountRow, code: &str) {
        let Some(lobby) = self.lobbies.get_mut(code) else {
            return self.error(conn, ErrorCode::LobbyNotFound);
        };
        if lobby.playing {
            return self.error(conn, ErrorCode::AlreadyPlaying);
        }
        // Dasselbe Konto ein zweites Mal (z. B. zweites Fenster) belegt keinen neuen Platz
        if lobby.members.iter().any(|m| m.account == account.id) {
            return self.error(conn, ErrorCode::LobbyFull);
        }
        let Some(slot) = lobby.free_slot() else {
            return self.error(conn, ErrorCode::LobbyFull);
        };
        lobby.members.push(Member {
            conn,
            slot,
            account: account.id,
            name: account.name,
            ready: false,
        });
        if let Some(c) = self.conns.get_mut(&conn) {
            c.lobby = Some(code.to_owned());
        }
        self.broadcast_lobby(code);
    }

    fn relay(&mut self, conn: ConnId, to: Target, data: Vec<u8>) {
        let Some(code) = self.conns.get(&conn).and_then(|c| c.lobby.clone()) else {
            return;
        };
        let Some(lobby) = self.lobbies.get(&code) else {
            return;
        };
        if !lobby.playing {
            return;
        }
        let Some(from) = lobby.member(conn).map(|m| m.slot) else {
            return;
        };
        let targets: Vec<ConnId> = match to {
            Target::Host => vec![lobby.host],
            Target::Others => lobby.members.iter().map(|m| m.conn).collect(),
            Target::Player(slot) => lobby
                .members
                .iter()
                .filter(|m| m.slot == slot)
                .map(|m| m.conn)
                .collect(),
        };
        let bytes = Bytes::from(encode(&ServerMsg::Relay { from, data }));
        for t in targets.into_iter().filter(|t| *t != conn) {
            self.send_bytes(t, bytes.clone());
        }
    }

    /// Nachrichtenbudget (Token-Bucket); `false` bei Überschreitung.
    fn spend(&mut self, conn: ConnId) -> bool {
        let Some(c) = self.conns.get_mut(&conn) else {
            return false;
        };
        let now = Instant::now();
        let dt = now.duration_since(c.last).as_secs_f64();
        c.last = now;
        c.allowance = (c.allowance + dt * RATE).min(BURST) - 1.0;
        c.allowance >= 0.0
    }
}

impl Hub {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Neue Verbindung (nach erfolgreichem `Hello`); schickt `Welcome`.
    pub fn connect(
        &self,
        tx: Outbox,
        account: Option<AccountRow>,
        token: Option<String>,
    ) -> ConnId {
        let mut s = self.lock();
        s.next_id += 1;
        let id = s.next_id;
        let welcome = ServerMsg::Welcome {
            account: account.as_ref().map(|a| Account {
                name: a.name.clone(),
            }),
        };
        let token = token.filter(|_| account.is_some());
        s.conns.insert(
            id,
            Conn {
                tx: Some(tx),
                account,
                token,
                lobby: None,
                link: None,
                allowance: BURST,
                last: Instant::now(),
            },
        );
        s.send(id, &welcome);
        id
    }

    pub fn disconnect(&self, conn: ConnId) {
        let mut s = self.lock();
        s.leave_lobby(conn, None);
        if let Some(c) = s.conns.remove(&conn)
            && let Some(link) = c.link
        {
            s.links.remove(&link);
        }
    }

    pub fn online(&self) -> (usize, usize) {
        let s = self.lock();
        (s.conns.len(), s.lobbies.len())
    }

    /// Verknüpfungscode auf der Webseite bestätigt: Gerät anmelden.
    pub fn complete_link(&self, code: &str, account: AccountRow, token: String) -> bool {
        let mut s = self.lock();
        let code = normalize_code(code);
        let Some((conn, created)) = s.links.remove(&code) else {
            return false;
        };
        if created.elapsed() > LINK_TTL {
            return false;
        }
        let Some(c) = s.conns.get_mut(&conn) else {
            return false;
        };
        c.link = None;
        c.account = Some(account.clone());
        c.token = Some(token.clone());
        s.send(
            conn,
            &ServerMsg::Linked {
                token,
                account: Account { name: account.name },
            },
        );
        true
    }

    /// Ist der Code gerade gültig (für die Webseite)?
    pub fn link_pending(&self, code: &str) -> bool {
        let s = self.lock();
        s.links
            .get(&normalize_code(code))
            .is_some_and(|(_, created)| created.elapsed() <= LINK_TTL)
    }

    pub fn handle(&self, conn: ConnId, msg: ClientMsg) -> Effect {
        let mut s = self.lock();
        if !s.spend(conn) {
            return Effect::Disconnect;
        }
        let account = s.conns.get(&conn).and_then(|c| c.account.clone());
        match msg {
            ClientMsg::Hello { .. } => {}
            ClientMsg::Ping { id } => s.send(conn, &ServerMsg::Pong { id }),
            ClientMsg::LinkStart => {
                s.links
                    .retain(|_, (_, created)| created.elapsed() <= LINK_TTL);
                let old = s.conns.get_mut(&conn).and_then(|c| c.link.take());
                if let Some(old) = old {
                    s.links.remove(&old);
                }
                let code = loop {
                    let code = random_code(LINK_CODE_LEN);
                    if !s.links.contains_key(&code) {
                        break code;
                    }
                };
                s.links.insert(code.clone(), (conn, Instant::now()));
                if let Some(c) = s.conns.get_mut(&conn) {
                    c.link = Some(code.clone());
                }
                s.send(conn, &ServerMsg::LinkCode { code });
            }
            ClientMsg::Logout => {
                s.leave_lobby(conn, Some(CloseReason::Left));
                let token = s.conns.get_mut(&conn).and_then(|c| {
                    c.account = None;
                    c.token.take()
                });
                s.send(conn, &ServerMsg::SignedOut);
                if let Some(token) = token {
                    return Effect::Logout(token);
                }
            }
            ClientMsg::ListLobbies => {
                let list = s.list();
                s.send(conn, &ServerMsg::Lobbies { list });
            }
            ClientMsg::CreateLobby { public } => {
                let Some(account) = account else {
                    s.error(conn, ErrorCode::NotSignedIn);
                    return Effect::None;
                };
                s.leave_lobby(conn, None);
                let code = s.new_lobby_code();
                s.lobbies.insert(
                    code.clone(),
                    Lobby {
                        code: code.clone(),
                        public,
                        host: conn,
                        members: Vec::new(),
                        playing: false,
                    },
                );
                s.join(conn, account, &code);
            }
            ClientMsg::JoinLobby { code } => {
                let Some(account) = account else {
                    s.error(conn, ErrorCode::NotSignedIn);
                    return Effect::None;
                };
                let code = normalize_code(&code);
                if s.conns.get(&conn).and_then(|c| c.lobby.as_deref()) == Some(code.as_str()) {
                    s.broadcast_lobby(&code);
                    return Effect::None;
                }
                if !s.lobbies.contains_key(&code) {
                    s.error(conn, ErrorCode::LobbyNotFound);
                    return Effect::None;
                }
                s.leave_lobby(conn, None);
                s.join(conn, account, &code);
            }
            ClientMsg::LeaveLobby => s.leave_lobby(conn, Some(CloseReason::Left)),
            ClientMsg::SetReady { ready } => {
                let Some(code) = s.conns.get(&conn).and_then(|c| c.lobby.clone()) else {
                    s.error(conn, ErrorCode::NotInLobby);
                    return Effect::None;
                };
                if let Some(m) = s
                    .lobbies
                    .get_mut(&code)
                    .and_then(|l| l.members.iter_mut().find(|m| m.conn == conn))
                {
                    m.ready = ready;
                }
                s.broadcast_lobby(&code);
            }
            ClientMsg::StartGame => {
                let Some(code) = s.conns.get(&conn).and_then(|c| c.lobby.clone()) else {
                    s.error(conn, ErrorCode::NotInLobby);
                    return Effect::None;
                };
                let Some(lobby) = s.lobbies.get_mut(&code) else {
                    return Effect::None;
                };
                if lobby.host != conn {
                    s.error(conn, ErrorCode::NotHost);
                    return Effect::None;
                }
                if lobby.playing {
                    s.error(conn, ErrorCode::AlreadyPlaying);
                    return Effect::None;
                }
                if !lobby.members.iter().all(|m| m.ready || m.conn == conn) {
                    s.error(conn, ErrorCode::NotReady);
                    return Effect::None;
                }
                lobby.playing = true;
                for m in lobby.members.iter_mut() {
                    m.ready = false;
                }
                let mut seed = [0u8; 8];
                let _ = getrandom::fill(&mut seed);
                let start = Bytes::from(encode(&ServerMsg::Start {
                    seed: u64::from_le_bytes(seed),
                }));
                let members: Vec<ConnId> = lobby.members.iter().map(|m| m.conn).collect();
                s.broadcast_lobby(&code);
                for m in members {
                    s.send_bytes(m, start.clone());
                }
            }
            ClientMsg::GameOver { result } => {
                let Some(code) = s.conns.get(&conn).and_then(|c| c.lobby.clone()) else {
                    return Effect::None;
                };
                let Some(lobby) = s.lobbies.get_mut(&code) else {
                    return Effect::None;
                };
                if lobby.host != conn || !lobby.playing {
                    return Effect::None;
                }
                lobby.playing = false;
                let players: Vec<(i64, MatchPlayer)> = result
                    .players
                    .into_iter()
                    .filter_map(|p| {
                        let account = lobby.members.iter().find(|m| m.slot == p.slot)?.account;
                        Some((account, p))
                    })
                    .collect();
                s.broadcast_lobby(&code);
                return Effect::Record(result.round, players);
            }
            ClientMsg::Relay { to, data } => s.relay(conn, to, data),
        }
        Effect::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use totenrunde_mp_protocol::{MatchResult, decode};

    struct Client {
        id: ConnId,
        rx: mpsc::Receiver<Bytes>,
    }

    impl Client {
        fn recv_all(&mut self) -> Vec<ServerMsg> {
            let mut out = Vec::new();
            while let Ok(b) = self.rx.try_recv() {
                out.push(decode(&b).expect("gültige Servernachricht"));
            }
            out
        }

        fn last_lobby(&mut self) -> Option<LobbyView> {
            self.recv_all().into_iter().rev().find_map(|m| match m {
                ServerMsg::Lobby(v) => Some(v),
                _ => None,
            })
        }
    }

    fn connect(hub: &Hub, id: i64, name: &str) -> Client {
        let (tx, rx) = mpsc::channel(OUTBOX_SIZE);
        let account = AccountRow {
            id,
            name: name.to_owned(),
        };
        let id = hub.connect(tx, Some(account), Some("t".repeat(64)));
        Client { id, rx }
    }

    #[test]
    fn lobby_lifecycle_and_relay() {
        let hub = Hub::default();
        let mut anna = connect(&hub, 1, "Anna");
        let mut bernd = connect(&hub, 2, "Bernd");
        assert!(matches!(anna.recv_all()[0], ServerMsg::Welcome { .. }));
        bernd.recv_all();

        hub.handle(anna.id, ClientMsg::CreateLobby { public: true });
        let view = anna.last_lobby().unwrap();
        assert!(view.is_host());
        assert_eq!(view.members.len(), 1);

        hub.handle(bernd.id, ClientMsg::ListLobbies);
        let list = bernd.recv_all();
        assert!(matches!(&list[0], ServerMsg::Lobbies { list } if list.len() == 1));

        hub.handle(
            bernd.id,
            ClientMsg::JoinLobby {
                code: view.code.to_lowercase(),
            },
        );
        let bview = bernd.last_lobby().unwrap();
        assert_eq!(bview.you, 1);
        assert_eq!(anna.last_lobby().unwrap().members.len(), 2);

        // Start erst, wenn alle bereit sind
        hub.handle(anna.id, ClientMsg::StartGame);
        assert!(anna.recv_all().contains(&ServerMsg::Error {
            code: ErrorCode::NotReady
        }));
        hub.handle(bernd.id, ClientMsg::SetReady { ready: true });
        hub.handle(bernd.id, ClientMsg::StartGame);
        assert!(bernd.recv_all().contains(&ServerMsg::Error {
            code: ErrorCode::NotHost
        }));
        anna.recv_all();
        hub.handle(anna.id, ClientMsg::StartGame);
        assert!(
            anna.recv_all()
                .iter()
                .any(|m| matches!(m, ServerMsg::Start { .. }))
        );
        assert!(
            bernd
                .recv_all()
                .iter()
                .any(|m| matches!(m, ServerMsg::Start { .. }))
        );

        // Weiterleiten
        hub.handle(
            bernd.id,
            ClientMsg::Relay {
                to: Target::Host,
                data: vec![7, 8],
            },
        );
        assert_eq!(
            anna.recv_all(),
            vec![ServerMsg::Relay {
                from: 1,
                data: vec![7, 8]
            }]
        );
        hub.handle(
            anna.id,
            ClientMsg::Relay {
                to: Target::Others,
                data: vec![1],
            },
        );
        assert_eq!(
            bernd.recv_all(),
            vec![ServerMsg::Relay {
                from: 0,
                data: vec![1]
            }]
        );
        assert!(anna.recv_all().is_empty());

        // Ergebnis melden
        let effect = hub.handle(
            anna.id,
            ClientMsg::GameOver {
                result: MatchResult {
                    round: 9,
                    players: vec![MatchPlayer {
                        slot: 1,
                        kills: 3,
                        headshots: 1,
                        downs: 0,
                        revives: 0,
                        points: 900,
                    }],
                },
            },
        );
        assert!(matches!(effect, Effect::Record(9, ref p) if p[0].0 == 2));
        assert!(!anna.last_lobby().unwrap().playing);

        // Host geht: Lobby endet für alle
        hub.disconnect(anna.id);
        assert!(bernd.recv_all().contains(&ServerMsg::LobbyClosed {
            reason: CloseReason::HostLeft
        }));
        assert_eq!(hub.online(), (1, 0));
    }

    #[test]
    fn player_leaving_a_running_game_is_announced() {
        let hub = Hub::default();
        let mut anna = connect(&hub, 1, "Anna");
        let bernd = connect(&hub, 2, "Bernd");
        hub.handle(anna.id, ClientMsg::CreateLobby { public: false });
        let code = anna.last_lobby().unwrap().code;
        hub.handle(bernd.id, ClientMsg::JoinLobby { code });
        hub.handle(bernd.id, ClientMsg::SetReady { ready: true });
        hub.handle(anna.id, ClientMsg::StartGame);
        anna.recv_all();
        hub.disconnect(bernd.id);
        let msgs = anna.recv_all();
        assert!(msgs.contains(&ServerMsg::PlayerLeft { slot: 1 }));
        // Private Lobbys stehen nicht in der Liste
        hub.handle(anna.id, ClientMsg::ListLobbies);
        assert!(
            anna.recv_all()
                .contains(&ServerMsg::Lobbies { list: Vec::new() })
        );
    }

    #[test]
    fn link_codes_sign_in_the_waiting_game() {
        let hub = Hub::default();
        let (tx, mut rx) = mpsc::channel(OUTBOX_SIZE);
        let conn = hub.connect(tx, None, None);
        hub.handle(conn, ClientMsg::CreateLobby { public: true });
        hub.handle(conn, ClientMsg::LinkStart);
        let mut code = None;
        while let Ok(b) = rx.try_recv() {
            match decode::<ServerMsg>(&b).unwrap() {
                ServerMsg::LinkCode { code: c } => code = Some(c),
                ServerMsg::Error { code } => assert_eq!(code, ErrorCode::NotSignedIn),
                _ => {}
            }
        }
        let code = code.unwrap();
        assert!(hub.link_pending(&code.to_lowercase()));
        let account = AccountRow {
            id: 5,
            name: "Clara".into(),
        };
        assert!(hub.complete_link(&code, account.clone(), "x".repeat(64)));
        assert!(!hub.complete_link(&code, account, "x".repeat(64)));
        let linked = decode::<ServerMsg>(&rx.try_recv().unwrap()).unwrap();
        assert!(matches!(linked, ServerMsg::Linked { account, .. } if account.name == "Clara"));
        assert_eq!(
            hub.handle(conn, ClientMsg::Logout),
            Effect::Logout("x".repeat(64))
        );
    }

    #[test]
    fn floods_are_cut_off() {
        let hub = Hub::default();
        let (tx, _rx) = mpsc::channel(OUTBOX_SIZE);
        let conn = hub.connect(tx, None, None);
        let mut cut = false;
        for i in 0..2000 {
            if hub.handle(conn, ClientMsg::Ping { id: i }) == Effect::Disconnect {
                cut = true;
                break;
            }
        }
        assert!(cut);
    }
}
