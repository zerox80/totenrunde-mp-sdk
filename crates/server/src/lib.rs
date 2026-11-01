//! Server des Totenrunde-Multiplayers als Axum-Router: Konten (SQLite, Argon2), JSON-API für
//! die Konto-Webseite, Lobbys und das Weiterleiten der Spieldaten über WebSocket.
//!
//! Gespielt wird im Koop mit bis zu vier Spielern. Das Spiel des Lobby-Hosts berechnet die
//! Welt, jedes Spiel seinen eigenen Spieler. Der Server prüft Konten und Lobbys und reicht die
//! Spieldaten zwischen den Mitgliedern weiter – vom Spielinhalt versteht er nichts.
//!
//! ```no_run
//! use totenrunde_mp_server::{Db, Multiplayer};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Db::open(std::path::Path::new("/data/totenrunde.sqlite3"))?;
//! let app = axum::Router::new().merge(Multiplayer::new(db).routes());
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
//! axum::serve(listener, app).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Routen: `/api/mp/ws` (WebSocket), `/api/mp/status`, `/api/account` (GET/DELETE),
//! `/api/account/{register,login,logout,link}`, `/api/leaderboard`. Wer die Adresse des
//! Gegenübers kennt, legt sie als [`PeerAddr`] in die Anfrage; sonst gelten die Grenzen für
//! Anmeldeversuche für alle gemeinsam.

pub mod api;
pub mod db;
pub mod hub;
pub mod ws;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::routing::{any, get, post};
use tokio::sync::Semaphore;

pub use db::Db;
pub use hub::Hub;
pub use totenrunde_mp_protocol as protocol;

/// Adresse des Gegenübers (von den Servern je Verbindung in die Anfrage gelegt).
#[derive(Clone, Copy, Debug)]
pub struct PeerAddr(pub IpAddr);

#[derive(Clone)]
pub struct Multiplayer {
    pub db: Db,
    pub hub: Arc<Hub>,
    pub limits: Arc<Limiter>,
    /// Gemeinsame Obergrenze für laufende Argon2-Arbeit, auch über mehrere Konten hinweg.
    password_jobs: Arc<Semaphore>,
}

impl Multiplayer {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            hub: Arc::default(),
            limits: Arc::default(),
            password_jobs: Arc::new(Semaphore::new(2)),
        }
    }

    pub fn routes(self) -> Router {
        Router::new()
            .route(totenrunde_mp_protocol::WS_PATH, any(ws::upgrade))
            .route("/api/mp/status", get(api::status))
            .route("/api/account", get(api::me).delete(api::delete_account))
            .route("/api/account/register", post(api::register))
            .route("/api/account/login", post(api::login))
            .route("/api/account/logout", post(api::logout))
            .route("/api/account/link", post(api::link).get(api::link_status))
            .route("/api/leaderboard", get(api::leaderboard))
            .with_state(self)
    }
}

/// Einfache Ratenbegrenzung mit festen Zeitfenstern je Schlüssel.
#[derive(Default)]
pub struct Limiter {
    map: Mutex<HashMap<String, (u32, Instant)>>,
}

impl Limiter {
    fn with<T>(&self, f: impl FnOnce(&mut HashMap<String, (u32, Instant)>) -> T) -> T {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() > 10_000 {
            map.retain(|_, (_, start)| start.elapsed() < Duration::from_secs(3600));
        }
        f(&mut map)
    }

    /// Zählt einen Versuch; `false`, wenn im Zeitfenster schon `max` Versuche waren.
    pub fn allow(&self, key: String, max: u32, window: Duration) -> bool {
        self.with(|map| {
            let entry = map.entry(key).or_insert((0, Instant::now()));
            if entry.1.elapsed() > window {
                *entry = (0, Instant::now());
            }
            entry.0 = entry.0.saturating_add(1);
            entry.0 <= max
        })
    }

    pub fn reset(&self, key: &str) {
        self.with(|map| map.remove(key));
    }
}
