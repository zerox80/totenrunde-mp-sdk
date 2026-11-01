//! WebSocket-Verbindung eines Spiels: `Hello` prüfen, dann Nachrichten an den Hub geben und
//! dessen Warteschlange an das Spiel senden.

use std::time::Duration;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use totenrunde_mp_protocol::{
    ClientMsg, ErrorCode, MAX_MESSAGE, ServerMsg, VERSION, decode, encode,
};

use super::Multiplayer;
use super::hub::{Effect, OUTBOX_SIZE};

/// Ohne `Hello` innerhalb dieser Zeit wird getrennt.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Das Spiel schickt alle paar Sekunden ein `Ping`; bleibt alles still, ist es weg.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn upgrade(ws: WebSocketUpgrade, State(mp): State<Multiplayer>) -> Response {
    ws.max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |socket| run(socket, mp))
}

async fn send_now(socket: &mut WebSocket, msg: &ServerMsg) {
    let _ = socket.send(Message::Binary(Bytes::from(encode(msg)))).await;
}

async fn run(mut socket: WebSocket, mp: Multiplayer) {
    let hello = match tokio::time::timeout(HELLO_TIMEOUT, socket.recv()).await {
        Ok(Some(Ok(Message::Binary(bytes)))) => decode::<ClientMsg>(&bytes),
        _ => None,
    };
    let Some(ClientMsg::Hello { version, token }) = hello else {
        send_now(
            &mut socket,
            &ServerMsg::Error {
                code: ErrorCode::NoHello,
            },
        )
        .await;
        return;
    };
    if version != VERSION {
        send_now(&mut socket, &ServerMsg::Outdated { server: VERSION }).await;
        return;
    }
    let account = match token.clone() {
        Some(token) => {
            let db = mp.db.clone();
            tokio::task::spawn_blocking(move || db.account_for_token(&token))
                .await
                .ok()
                .flatten()
        }
        None => None,
    };

    let (tx, mut rx) = mpsc::channel::<Bytes>(OUTBOX_SIZE);
    let conn = mp.hub.connect(tx, account, token);
    let (mut sink, mut stream) = socket.split();
    let mut writer = tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if sink.send(Message::Binary(bytes)).await.is_err() {
                return;
            }
        }
        let _ = sink.close().await;
    });

    loop {
        tokio::select! {
            _ = &mut writer => break,
            next = tokio::time::timeout(IDLE_TIMEOUT, stream.next()) => {
                let bytes = match next {
                    Ok(Some(Ok(Message::Binary(bytes)))) => bytes,
                    Ok(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => continue,
                    // Text, Schließen, Fehler oder Zeitüberschreitung
                    _ => break,
                };
                let Some(msg) = decode::<ClientMsg>(&bytes) else { break };
                match mp.hub.handle(conn, msg) {
                    Effect::None => {}
                    Effect::Disconnect => break,
                    Effect::Logout(token) => {
                        let db = mp.db.clone();
                        let _ = tokio::task::spawn_blocking(move || db.delete_session(&token)).await;
                    }
                    Effect::Record(round, players) => {
                        let db = mp.db.clone();
                        let _ = tokio::task::spawn_blocking(move || db.record_match(round, &players)).await;
                    }
                }
            }
        }
    }
    mp.hub.disconnect(conn);
    writer.abort();
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;
    use totenrunde_mp_protocol::{ClientMsg, ServerMsg, Target, VERSION, WS_PATH, decode, encode};

    use crate::db::Device;
    use crate::{Db, Multiplayer};

    type Socket = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn send(socket: &mut Socket, msg: ClientMsg) {
        socket
            .send(Message::Binary(encode(&msg).into()))
            .await
            .unwrap();
    }

    /// Nächste Servernachricht, die `want` erfüllt (andere werden übersprungen).
    async fn expect(socket: &mut Socket, want: impl Fn(&ServerMsg) -> bool) -> ServerMsg {
        loop {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
                .await
                .expect("Antwort innerhalb von 5 s")
                .unwrap()
                .unwrap();
            if let Message::Binary(bytes) = msg {
                let msg: ServerMsg = decode(&bytes).unwrap();
                if want(&msg) {
                    return msg;
                }
            }
        }
    }

    #[tokio::test]
    async fn two_games_meet_in_a_lobby_over_websocket() {
        let db = Db::in_memory();
        let anna = db.register("Anna", "geheim123").unwrap();
        let bernd = db.register("Bernd", "geheim123").unwrap();
        let anna_token = db.create_session(anna.id, Device::Game).unwrap();
        let bernd_token = db.create_session(bernd.id, Device::Game).unwrap();
        let app = Multiplayer::new(db).routes();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let url = format!("ws://{address}{WS_PATH}");

        let (mut a, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut b, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut outdated, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        send(
            &mut outdated,
            ClientMsg::Hello {
                version: VERSION + 1,
                token: None,
            },
        )
        .await;
        expect(&mut outdated, |m| matches!(m, ServerMsg::Outdated { .. })).await;

        send(
            &mut a,
            ClientMsg::Hello {
                version: VERSION,
                token: Some(anna_token),
            },
        )
        .await;
        let welcome = expect(&mut a, |m| matches!(m, ServerMsg::Welcome { .. })).await;
        assert!(matches!(welcome, ServerMsg::Welcome { account: Some(acc) } if acc.name == "Anna"));
        send(
            &mut b,
            ClientMsg::Hello {
                version: VERSION,
                token: Some(bernd_token),
            },
        )
        .await;
        expect(&mut b, |m| {
            matches!(m, ServerMsg::Welcome { account: Some(_) })
        })
        .await;

        send(&mut a, ClientMsg::CreateLobby { public: true }).await;
        let ServerMsg::Lobby(view) = expect(&mut a, |m| matches!(m, ServerMsg::Lobby(_))).await
        else {
            unreachable!()
        };
        send(
            &mut b,
            ClientMsg::JoinLobby {
                code: view.code.clone(),
            },
        )
        .await;
        expect(
            &mut b,
            |m| matches!(m, ServerMsg::Lobby(v) if v.members.len() == 2),
        )
        .await;
        send(&mut b, ClientMsg::SetReady { ready: true }).await;
        expect(
            &mut a,
            |m| matches!(m, ServerMsg::Lobby(v) if v.all_ready()),
        )
        .await;
        send(&mut a, ClientMsg::StartGame).await;
        expect(&mut a, |m| matches!(m, ServerMsg::Start { .. })).await;
        expect(&mut b, |m| matches!(m, ServerMsg::Start { .. })).await;

        send(
            &mut b,
            ClientMsg::Relay {
                to: Target::Host,
                data: vec![42; 300],
            },
        )
        .await;
        let relayed = expect(&mut a, |m| matches!(m, ServerMsg::Relay { .. })).await;
        assert_eq!(
            relayed,
            ServerMsg::Relay {
                from: 1,
                data: vec![42; 300]
            }
        );

        drop(b);
        expect(&mut a, |m| matches!(m, ServerMsg::PlayerLeft { slot: 1 })).await;
        server.abort();
    }
}
