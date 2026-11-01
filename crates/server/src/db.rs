//! Konten, Sitzungen und Spielstatistik in SQLite.
//!
//! Passwörter liegen nur als Argon2id-Hash vor, Sitzungstokens nur als SHA-256-Hash: Wer die
//! Datenbank liest, kann sich damit weder anmelden noch eine Sitzung übernehmen. Alle
//! Funktionen blockieren; Aufrufer im Server nutzen `spawn_blocking`.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use argon2::Argon2;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use totenrunde_mp_protocol::{MatchPlayer, valid_name, valid_password};

/// Sitzungen, die so lange nicht benutzt wurden, verfallen.
const SESSION_DAYS: i64 = 90;

const SCHEMA: &str = "
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password TEXT NOT NULL,
    created INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    account INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    created INTEGER NOT NULL,
    used INTEGER NOT NULL,
    device TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_account ON sessions(account);
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY,
    ended INTEGER NOT NULL,
    round INTEGER NOT NULL,
    players INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS match_players (
    match INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    account INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    kills INTEGER NOT NULL,
    headshots INTEGER NOT NULL,
    downs INTEGER NOT NULL,
    revives INTEGER NOT NULL,
    points INTEGER NOT NULL,
    PRIMARY KEY (match, account)
);
CREATE INDEX IF NOT EXISTS match_players_account ON match_players(account);
";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountRow {
    pub id: i64,
    pub name: String,
}

/// Wo eine Sitzung entstanden ist (nur zur Anzeige).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Device {
    Website,
    Game,
}

impl Device {
    fn as_str(self) -> &'static str {
        match self {
            Device::Website => "website",
            Device::Game => "game",
        }
    }
}

#[derive(Serialize, Default, Debug, PartialEq, Eq)]
pub struct Stats {
    pub games: i64,
    pub best_round: i64,
    pub kills: i64,
    pub headshots: i64,
    pub downs: i64,
    pub revives: i64,
    pub points: i64,
}

#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct LeaderEntry {
    pub name: String,
    pub best_round: i64,
    pub kills: i64,
    pub games: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RegisterError {
    Invalid,
    NameTaken,
    Internal,
}

#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn token_hash(token: &str) -> String {
    hex(&Sha256::digest(token.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Zufälliges Sitzungstoken (256 Bit, hexadezimal).
pub fn new_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("Betriebssystem liefert keine Zufallszahlen");
    hex(&bytes)
}

impl Db {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::init(conn)
    }

    pub fn in_memory() -> Self {
        Self::init(Connection::open_in_memory().expect("SQLite im Speicher"))
            .expect("Schema anlegen")
    }

    fn init(conn: Connection) -> rusqlite::Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "DELETE FROM sessions WHERE used < ?1",
            [now() - SESSION_DAYS * 86_400],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn register(&self, name: &str, password: &str) -> Result<AccountRow, RegisterError> {
        if !valid_name(name) || !valid_password(password) {
            return Err(RegisterError::Invalid);
        }
        let hash = hash_password(password).ok_or(RegisterError::Internal)?;
        let conn = self.conn();
        let result = conn.execute(
            "INSERT INTO accounts (name, password, created) VALUES (?1, ?2, ?3)",
            params![name, hash, now()],
        );
        match result {
            Ok(_) => Ok(AccountRow {
                id: conn.last_insert_rowid(),
                name: name.to_owned(),
            }),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(RegisterError::NameTaken)
            }
            Err(error) => {
                eprintln!("Konto konnte nicht angelegt werden: {error}");
                Err(RegisterError::Internal)
            }
        }
    }

    /// Prüft Name und Passwort. Auch bei unbekanntem Namen wird ein Hash geprüft, damit die
    /// Antwortzeit nicht verrät, ob es das Konto gibt.
    pub fn login(&self, name: &str, password: &str) -> Option<AccountRow> {
        let row: Option<(i64, String, String)> = self
            .conn()
            .query_row(
                "SELECT id, name, password FROM accounts WHERE name = ?1",
                [name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .ok()
            .flatten();
        match row {
            Some((id, name, hash)) if verify_password(password, &hash) => {
                Some(AccountRow { id, name })
            }
            Some(_) => None,
            None => {
                let _ = verify_password(password, dummy_hash());
                None
            }
        }
    }

    pub fn verify(&self, account: i64, password: &str) -> bool {
        let hash: Option<String> = self
            .conn()
            .query_row(
                "SELECT password FROM accounts WHERE id = ?1",
                [account],
                |r| r.get(0),
            )
            .optional()
            .ok()
            .flatten();
        hash.is_some_and(|h| verify_password(password, &h))
    }

    /// Neue Sitzung; liefert das Token im Klartext (nur der Hash wird gespeichert).
    pub fn create_session(&self, account: i64, device: Device) -> Option<String> {
        let token = new_token();
        let t = now();
        self.conn()
            .execute(
                "INSERT INTO sessions (token, account, created, used, device) VALUES (?1, ?2, ?3, ?3, ?4)",
                params![token_hash(&token), account, t, device.as_str()],
            )
            .ok()?;
        Some(token)
    }

    pub fn account_for_token(&self, token: &str) -> Option<AccountRow> {
        if token.len() != 64 {
            return None;
        }
        let conn = self.conn();
        let hash = token_hash(token);
        let row = conn
            .query_row(
                "SELECT a.id, a.name FROM sessions s JOIN accounts a ON a.id = s.account
                 WHERE s.token = ?1 AND s.used >= ?2",
                params![hash, now() - SESSION_DAYS * 86_400],
                |r| {
                    Ok(AccountRow {
                        id: r.get(0)?,
                        name: r.get(1)?,
                    })
                },
            )
            .optional()
            .ok()
            .flatten()?;
        let _ = conn.execute(
            "UPDATE sessions SET used = ?1 WHERE token = ?2",
            params![now(), hash],
        );
        Some(row)
    }

    pub fn delete_session(&self, token: &str) {
        let _ = self
            .conn()
            .execute("DELETE FROM sessions WHERE token = ?1", [token_hash(token)]);
    }

    /// Konto samt Sitzungen und Statistik löschen.
    pub fn delete_account(&self, account: i64) -> bool {
        self.conn()
            .execute("DELETE FROM accounts WHERE id = ?1", [account])
            .is_ok_and(|n| n > 0)
    }

    pub fn created(&self, account: i64) -> i64 {
        self.conn()
            .query_row(
                "SELECT created FROM accounts WHERE id = ?1",
                [account],
                |r| r.get(0),
            )
            .unwrap_or(0)
    }

    /// Partie speichern; `players` enthält die Konto-ID je Teilnehmer.
    pub fn record_match(&self, round: u32, players: &[(i64, MatchPlayer)]) {
        if players.is_empty() {
            return;
        }
        let mut conn = self.conn();
        let Ok(tx) = conn.transaction() else { return };
        let saved = (|| -> rusqlite::Result<()> {
            tx.execute(
                "INSERT INTO matches (ended, round, players) VALUES (?1, ?2, ?3)",
                params![now(), round, players.len() as i64],
            )?;
            let id = tx.last_insert_rowid();
            for (account, p) in players {
                tx.execute(
                    "INSERT OR IGNORE INTO match_players
                     (match, account, kills, headshots, downs, revives, points)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id,
                        account,
                        p.kills,
                        p.headshots,
                        p.downs,
                        p.revives,
                        p.points.min(i64::MAX as u64) as i64
                    ],
                )?;
            }
            Ok(())
        })();
        match saved.and_then(|()| tx.commit()) {
            Ok(()) => {}
            Err(error) => eprintln!("Partie konnte nicht gespeichert werden: {error}"),
        }
    }

    pub fn stats(&self, account: i64) -> Stats {
        self.conn()
            .query_row(
                "SELECT COUNT(*), COALESCE(MAX(m.round), 0), COALESCE(SUM(p.kills), 0),
                        COALESCE(SUM(p.headshots), 0), COALESCE(SUM(p.downs), 0),
                        COALESCE(SUM(p.revives), 0), COALESCE(SUM(p.points), 0)
                 FROM match_players p JOIN matches m ON m.id = p.match WHERE p.account = ?1",
                [account],
                |r| {
                    Ok(Stats {
                        games: r.get(0)?,
                        best_round: r.get(1)?,
                        kills: r.get(2)?,
                        headshots: r.get(3)?,
                        downs: r.get(4)?,
                        revives: r.get(5)?,
                        points: r.get(6)?,
                    })
                },
            )
            .unwrap_or_default()
    }

    /// Bestenliste: höchste erreichte Runde, bei Gleichstand mehr Kills.
    pub fn leaderboard(&self, limit: usize) -> Vec<LeaderEntry> {
        let conn = self.conn();
        let Ok(mut stmt) = conn.prepare(
            "SELECT a.name, MAX(m.round) AS best, SUM(p.kills) AS kills, COUNT(*)
             FROM match_players p
             JOIN matches m ON m.id = p.match
             JOIN accounts a ON a.id = p.account
             GROUP BY p.account
             ORDER BY best DESC, kills DESC, a.name
             LIMIT ?1",
        ) else {
            return Vec::new();
        };
        stmt.query_map([limit as i64], |r| {
            Ok(LeaderEntry {
                name: r.get(0)?,
                best_round: r.get(1)?,
                kills: r.get(2)?,
                games: r.get(3)?,
            })
        })
        .map(|rows| rows.filter_map(Result::ok).collect())
        .unwrap_or_default()
    }
}

fn hash_password(password: &str) -> Option<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .ok()
        .map(|h| h.to_string())
}

fn verify_password(password: &str, hash: &str) -> bool {
    PasswordHash::new(hash).is_ok_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

fn dummy_hash() -> &'static str {
    static HASH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    HASH.get_or_init(|| hash_password("kein-echtes-passwort").unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player(slot: u8, kills: u32) -> MatchPlayer {
        MatchPlayer {
            slot,
            kills,
            headshots: 1,
            downs: 0,
            revives: 2,
            points: 1500,
        }
    }

    #[test]
    fn register_login_and_sessions() {
        let db = Db::in_memory();
        let anna = db.register("Anna", "geheim123").unwrap();
        assert_eq!(
            db.register("anna", "anderes12"),
            Err(RegisterError::NameTaken)
        );
        assert_eq!(db.register("a", "geheim123"), Err(RegisterError::Invalid));
        assert_eq!(db.register("Bernd", "kurz"), Err(RegisterError::Invalid));

        assert_eq!(db.login("ANNA", "geheim123"), Some(anna.clone()));
        assert_eq!(db.login("Anna", "falsch!!!"), None);
        assert_eq!(db.login("Niemand", "geheim123"), None);

        let token = db.create_session(anna.id, Device::Game).unwrap();
        assert_eq!(db.account_for_token(&token), Some(anna.clone()));
        assert_eq!(db.account_for_token(&new_token()), None);
        db.delete_session(&token);
        assert_eq!(db.account_for_token(&token), None);
    }

    #[test]
    fn stats_and_leaderboard() {
        let db = Db::in_memory();
        let anna = db.register("Anna", "geheim123").unwrap();
        let bernd = db.register("Bernd", "geheim123").unwrap();
        db.record_match(7, &[(anna.id, player(0, 40)), (bernd.id, player(1, 55))]);
        db.record_match(12, &[(anna.id, player(0, 90))]);

        let stats = db.stats(anna.id);
        assert_eq!(stats.games, 2);
        assert_eq!(stats.best_round, 12);
        assert_eq!(stats.kills, 130);
        assert_eq!(stats.revives, 4);

        let board = db.leaderboard(10);
        assert_eq!(board[0].name, "Anna");
        assert_eq!(board[0].best_round, 12);
        assert_eq!(board[1].name, "Bernd");

        let token = db.create_session(bernd.id, Device::Website).unwrap();
        assert!(db.delete_account(bernd.id));
        assert_eq!(db.account_for_token(&token), None);
        assert_eq!(db.leaderboard(10).len(), 1);
    }
}
