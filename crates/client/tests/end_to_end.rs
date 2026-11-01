//! Client und Server zusammen: zwei Spiele melden sich an, treffen sich in einer Lobby,
//! starten die Partie und tauschen Spieldaten aus.

use std::time::{Duration, Instant};

use totenrunde_mp_client::protocol::{ClientMsg, ServerMsg, Target};
use totenrunde_mp_client::{Connection, Event, ws_url};
use totenrunde_mp_server::db::Device;
use totenrunde_mp_server::{Db, Multiplayer};

/// Fragt die Verbindung ab, bis eine passende Nachricht kommt (höchstens 5 Sekunden).
async fn wait_for(
    conn: &mut Connection,
    start: Instant,
    want: impl Fn(&ServerMsg) -> bool,
) -> ServerMsg {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        conn.poll(start.elapsed().as_secs_f64(), &mut events);
        for event in events.drain(..) {
            match event {
                Event::Message(msg) if want(&msg) => return msg,
                Event::Closed(reason) => panic!("Verbindung beendet: {reason}"),
                _ => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("keine passende Nachricht innerhalb von 5 s");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_clients_play_together() {
    let db = Db::in_memory();
    let anna = db.register("Anna", "geheim123").unwrap();
    let bernd = db.register("Bernd", "geheim123").unwrap();
    let anna_token = db.create_session(anna.id, Device::Game);
    let bernd_token = db.create_session(bernd.id, Device::Game);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Multiplayer::new(db).routes();
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let url = ws_url(&format!("http://{address}"));
    let start = Instant::now();

    let mut a = Connection::connect(&url, anna_token);
    let mut b = Connection::connect(&url, bernd_token);
    let mut guest = Connection::connect(&url, None);

    let welcome = wait_for(&mut a, start, |m| matches!(m, ServerMsg::Welcome { .. })).await;
    assert!(matches!(welcome, ServerMsg::Welcome { account: Some(acc) } if acc.name == "Anna"));
    wait_for(&mut b, start, |m| matches!(m, ServerMsg::Welcome { .. })).await;
    let welcome = wait_for(&mut guest, start, |m| {
        matches!(m, ServerMsg::Welcome { .. })
    })
    .await;
    assert_eq!(welcome, ServerMsg::Welcome { account: None });

    // Ohne Konto keine Lobby, aber ein Code zum Verknüpfen
    guest.send(&ClientMsg::CreateLobby { public: true });
    wait_for(&mut guest, start, |m| matches!(m, ServerMsg::Error { .. })).await;
    guest.send(&ClientMsg::LinkStart);
    wait_for(&mut guest, start, |m| {
        matches!(m, ServerMsg::LinkCode { .. })
    })
    .await;

    a.send(&ClientMsg::CreateLobby { public: false });
    let ServerMsg::Lobby(view) =
        wait_for(&mut a, start, |m| matches!(m, ServerMsg::Lobby(_))).await
    else {
        unreachable!()
    };
    b.send(&ClientMsg::JoinLobby {
        code: view.code.to_lowercase(),
    });
    wait_for(
        &mut b,
        start,
        |m| matches!(m, ServerMsg::Lobby(v) if v.members.len() == 2),
    )
    .await;
    b.send(&ClientMsg::SetReady { ready: true });
    wait_for(
        &mut a,
        start,
        |m| matches!(m, ServerMsg::Lobby(v) if v.all_ready()),
    )
    .await;
    a.send(&ClientMsg::StartGame);
    wait_for(&mut a, start, |m| matches!(m, ServerMsg::Start { .. })).await;
    wait_for(&mut b, start, |m| matches!(m, ServerMsg::Start { .. })).await;

    a.relay(Target::Others, vec![1, 2, 3]);
    let msg = wait_for(&mut b, start, |m| matches!(m, ServerMsg::Relay { .. })).await;
    assert_eq!(
        msg,
        ServerMsg::Relay {
            from: 0,
            data: vec![1, 2, 3]
        }
    );

    // Pings laufen von selbst; nach ein paar Sekunden ist die Laufzeit bekannt
    let deadline = Instant::now() + Duration::from_secs(6);
    let mut events = Vec::new();
    while a.rtt_ms().is_none() && Instant::now() < deadline {
        a.poll(start.elapsed().as_secs_f64(), &mut events);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(a.rtt_ms().is_some());

    drop(b);
    wait_for(&mut a, start, |m| {
        matches!(m, ServerMsg::PlayerLeft { slot: 1 })
    })
    .await;
    server.abort();
}
