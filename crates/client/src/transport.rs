//! WebSocket-Verbindung mit binären Nachrichten, auf dem Desktop und im Browser gleich zu
//! benutzen: `connect`, `send`, `poll`.
//!
//! * Desktop: eigener Thread mit tungstenite, TLS über rustls (Wurzelzertifikate von
//!   webpki-roots, damit es ohne Systembibliotheken unter Windows und Linux läuft).
//! * Browser: die WebSocket-Schnittstelle des Browsers.
//!
//! Beide Varianten sind nicht `Sync`; die Verbindung liegt daher als Non-Send-Ressource im
//! Spiel.

/// Was seit dem letzten `poll` passiert ist.
#[derive(Debug)]
pub enum SocketEvent {
    Open,
    Message(Vec<u8>),
    /// Verbindung beendet oder nicht zustande gekommen (Grund für das Protokoll)
    Closed(String),
}

pub use imp::Socket;

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::io::{self, Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    use std::sync::Arc;
    use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
    use std::time::Duration;

    use tungstenite::Message;
    use tungstenite::client::IntoClientRequest;

    use super::SocketEvent;

    const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);
    /// Wie lange ein Lesen höchstens wartet, bevor ausgehende Nachrichten drankommen
    const POLL: Duration = Duration::from_millis(4);

    pub struct Socket {
        out: Sender<Vec<u8>>,
        events: Receiver<SocketEvent>,
    }

    impl Socket {
        pub fn connect(url: &str) -> Self {
            let (out, out_rx) = channel::<Vec<u8>>();
            let (events_tx, events) = channel::<SocketEvent>();
            let url = url.to_owned();
            let spawned = std::thread::Builder::new()
                .name("totenrunde-net".into())
                .spawn(move || {
                    let reason = match run(&url, &out_rx, &events_tx) {
                        Ok(()) => "geschlossen".to_owned(),
                        Err(error) => error,
                    };
                    let _ = events_tx.send(SocketEvent::Closed(reason));
                });
            let socket = Self { out, events };
            if let Err(error) = spawned {
                eprintln!("Netzwerk-Thread konnte nicht starten: {error}");
            }
            socket
        }

        pub fn send(&mut self, bytes: Vec<u8>) {
            let _ = self.out.send(bytes);
        }

        pub fn poll(&mut self, out: &mut Vec<SocketEvent>) {
            loop {
                match self.events.try_recv() {
                    Ok(event) => out.push(event),
                    Err(TryRecvError::Empty) => return,
                    Err(TryRecvError::Disconnected) => {
                        out.push(SocketEvent::Closed("Thread beendet".into()));
                        return;
                    }
                }
            }
        }
    }

    enum Stream {
        Plain(TcpStream),
        Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
    }

    impl Read for Stream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self {
                Stream::Plain(s) => s.read(buf),
                Stream::Tls(s) => s.read(buf),
            }
        }
    }

    impl Write for Stream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match self {
                Stream::Plain(s) => s.write(buf),
                Stream::Tls(s) => s.write(buf),
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            match self {
                Stream::Plain(s) => s.flush(),
                Stream::Tls(s) => s.flush(),
            }
        }
    }

    fn tls_config() -> Arc<rustls::ClientConfig> {
        static CONFIG: std::sync::OnceLock<Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
        CONFIG
            .get_or_init(|| {
                let roots = rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                };
                Arc::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                )
            })
            .clone()
    }

    fn run(url: &str, out: &Receiver<Vec<u8>>, events: &Sender<SocketEvent>) -> Result<(), String> {
        let request = url.into_client_request().map_err(|e| e.to_string())?;
        let uri = request.uri().clone();
        let secure = uri.scheme_str() == Some("wss");
        let host = uri.host().ok_or("Adresse ohne Host")?.to_owned();
        let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
        let address = (host.trim_matches(['[', ']']), port)
            .to_socket_addrs()
            .map_err(|e| format!("{host}: {e}"))?
            .next()
            .ok_or_else(|| format!("{host}: keine Adresse"))?;
        let tcp = TcpStream::connect_timeout(&address, CONNECT_TIMEOUT)
            .map_err(|e| format!("{address}: {e}"))?;
        tcp.set_nodelay(true).ok();
        tcp.set_read_timeout(Some(CONNECT_TIMEOUT)).ok();
        let control = tcp.try_clone().map_err(|e| e.to_string())?;
        let stream = if secure {
            let name =
                rustls::pki_types::ServerName::try_from(host.clone()).map_err(|e| e.to_string())?;
            let conn =
                rustls::ClientConnection::new(tls_config(), name).map_err(|e| e.to_string())?;
            Stream::Tls(Box::new(rustls::StreamOwned::new(conn, tcp)))
        } else {
            Stream::Plain(tcp)
        };
        let (mut ws, _) = tungstenite::client(request, stream).map_err(|e| e.to_string())?;
        control.set_read_timeout(Some(POLL)).ok();
        let _ = events.send(SocketEvent::Open);

        loop {
            loop {
                match out.try_recv() {
                    Ok(bytes) => ws
                        .send(Message::Binary(bytes.into()))
                        .map_err(|e| e.to_string())?,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        let _ = ws.close(None);
                        let _ = ws.flush();
                        return Ok(());
                    }
                }
            }
            match ws.read() {
                Ok(Message::Binary(bytes)) => {
                    if events.send(SocketEvent::Message(bytes.to_vec())).is_err() {
                        return Ok(());
                    }
                }
                Ok(Message::Close(_)) => return Ok(()),
                Ok(_) => {}
                Err(tungstenite::Error::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) => {}
                Err(tungstenite::Error::ConnectionClosed) => return Ok(()),
                Err(e) => return Err(e.to_string()),
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;
    use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

    use super::SocketEvent;

    type Queue = Rc<RefCell<VecDeque<SocketEvent>>>;

    pub struct Socket {
        ws: Option<WebSocket>,
        queue: Queue,
        _on_open: Option<Closure<dyn FnMut(Event)>>,
        _on_message: Option<Closure<dyn FnMut(MessageEvent)>>,
        _on_close: Option<Closure<dyn FnMut(CloseEvent)>>,
    }

    impl Socket {
        pub fn connect(url: &str) -> Self {
            let queue: Queue = Rc::default();
            let ws = match WebSocket::new(url) {
                Ok(ws) => ws,
                Err(error) => {
                    queue
                        .borrow_mut()
                        .push_back(SocketEvent::Closed(format!("{error:?}")));
                    return Self {
                        ws: None,
                        queue,
                        _on_open: None,
                        _on_message: None,
                        _on_close: None,
                    };
                }
            };
            ws.set_binary_type(BinaryType::Arraybuffer);
            let q = queue.clone();
            let on_open = Closure::<dyn FnMut(Event)>::new(move |_| {
                q.borrow_mut().push_back(SocketEvent::Open);
            });
            let q = queue.clone();
            let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
                if let Ok(buffer) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                    let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
                    q.borrow_mut().push_back(SocketEvent::Message(bytes));
                }
            });
            let q = queue.clone();
            let on_close = Closure::<dyn FnMut(CloseEvent)>::new(move |e: CloseEvent| {
                let reason = if e.reason().is_empty() {
                    format!("Code {}", e.code())
                } else {
                    e.reason()
                };
                q.borrow_mut().push_back(SocketEvent::Closed(reason));
            });
            ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
            ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
            ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
            Self {
                ws: Some(ws),
                queue,
                _on_open: Some(on_open),
                _on_message: Some(on_message),
                _on_close: Some(on_close),
            }
        }

        pub fn send(&mut self, bytes: Vec<u8>) {
            if let Some(ws) = &self.ws
                && ws.ready_state() == WebSocket::OPEN
            {
                let _ = ws.send_with_u8_array(&bytes);
            }
        }

        pub fn poll(&mut self, out: &mut Vec<SocketEvent>) {
            out.extend(self.queue.borrow_mut().drain(..));
        }
    }

    impl Drop for Socket {
        fn drop(&mut self) {
            if let Some(ws) = self.ws.take() {
                ws.set_onopen(None);
                ws.set_onmessage(None);
                ws.set_onclose(None);
                let _ = ws.close();
            }
        }
    }
}
