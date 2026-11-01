# Totenrunde Multiplayer SDK

Bausteine für den Koop-Multiplayer von [Totenrunde](https://github.com/zerox80/Totenrunde):
Konten, Lobbys und das Weiterleiten der Spieldaten über WebSocket – für Desktop und Browser.

| Crate | Inhalt |
|---|---|
| `totenrunde-mp-protocol` | Nachrichten zwischen Spiel und Server (binär mit postcard, `no_std`) |
| `totenrunde-mp-client` | Verbindung zum Server: WebSocket auf dem Desktop (tungstenite + rustls) und im Browser, Anmeldung, Pings |
| `totenrunde-mp-server` | Axum-Router mit Konten (SQLite, Argon2), Konto-API, Lobbys und Relay |

Das SDK kennt keine Spielinhalte. Was die Spieler einer Partie austauschen, leitet der Server als
Bytes weiter; das Format legt das Spiel fest. Die Engine ist egal – Totenrunde nutzt Bevy, der
Client hängt aber nicht davon ab.

## Ablauf

1. Das Spiel verbindet sich mit `wss://<server>/api/mp/ws` und meldet sich mit Protokollversion
   und gespeichertem Sitzungstoken an.
2. Ohne Konto fordert es einen Verknüpfungscode an und öffnet die Konto-Seite
   (`/konto/?link=CODE`). Dort meldet sich der Spieler an und bestätigt das Gerät; der Server
   schickt dem wartenden Spiel ein eigenes Token.
3. Lobby erstellen (öffentlich oder privat) oder per fünfstelligem Code beitreten, bereit
   melden, der Host startet.
4. Während der Partie leitet der Server Nachrichten an den Host, an alle anderen oder an einen
   bestimmten Platz weiter. Verlässt der Host die Lobby, endet sie für alle.

## Einbinden

```toml
# Spiel
totenrunde-mp-client = { git = "https://github.com/zerox80/totenrunde-mp-sdk", tag = "v0.1.0" }

# Server
totenrunde-mp-server = { git = "https://github.com/zerox80/totenrunde-mp-sdk", tag = "v0.1.0" }
```

Beide Crates exportieren das Protokoll als `protocol`.

### Client

```rust
use totenrunde_mp_client::protocol::{ClientMsg, ServerMsg, Target};
use totenrunde_mp_client::{Connection, Event, ws_url};

let mut conn = Connection::connect(&ws_url("https://spiel.example.de"), saved_token);
let mut events = Vec::new();

// einmal pro Frame (Zeit in Sekunden, im Browser gibt es keine std::time::Instant)
conn.poll(now, &mut events);
for event in events.drain(..) {
    match event {
        Event::Message(ServerMsg::Welcome { account: None }) => conn.send(&ClientMsg::LinkStart),
        Event::Message(ServerMsg::Relay { from, data }) => { /* Spieldaten von Platz `from` */ }
        Event::Closed(reason) => { /* neu verbinden */ }
        _ => {}
    }
}
conn.relay(Target::Host, game_bytes);
```

### Server

```rust
use totenrunde_mp_server::{Db, Multiplayer};

let db = Db::open("/data/totenrunde.sqlite3".as_ref())?;
let app = axum::Router::new()
    .merge(Multiplayer::new(db).routes())
    // … eigene Routen, statische Dateien (z. B. die Konto-Seite unter /konto/)
    ;
axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8080").await?, app).await?;
```

Routen: `/api/mp/ws`, `/api/mp/status`, `/api/account` (GET, DELETE),
`/api/account/register`, `/api/account/login`, `/api/account/logout`, `/api/account/link`,
`/api/leaderboard`. Wer die Adresse des Gegenübers kennt, legt sie als `PeerAddr` in die
Anfrage-Erweiterungen; die Grenzen für Registrierungen und Anmeldeversuche gelten dann je Adresse.

Sicherheit: Passwörter liegen nur als Argon2id-Hash vor, Sitzungstokens nur als SHA-256-Hash.
Angemeldet wird mit `Authorization: Bearer …` statt Cookie. Langsame oder überflutende
Verbindungen trennt der Server.

## Entwicklung

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p totenrunde-mp-client --target wasm32-unknown-unknown -- -D warnings
```

Ändert sich ein Nachrichtenformat inkompatibel, wird `protocol::VERSION` erhöht; Spiele mit
anderer Version bekommen vom Server `Outdated`.

## Lizenz

MIT, siehe `LICENSE`.
