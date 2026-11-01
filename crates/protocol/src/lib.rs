//! Netzwerkprotokoll zwischen Spiel und Multiplayer-Server.
//!
//! Spiel und Server sprechen über eine WebSocket-Verbindung (`WS_PATH`) mit binären
//! Nachrichten, kodiert mit postcard. Der Server verwaltet Konten und Lobbys; was die
//! Spieler einer laufenden Partie austauschen, leitet er nur als Bytes weiter
//! ([`ClientMsg::Relay`] → [`ServerMsg::Relay`]). Das Format dieser Bytes legt das Spiel fest.
//!
//! Ablauf einer Verbindung:
//! 1. Spiel sendet [`ClientMsg::Hello`] mit Protokollversion und gespeichertem Sitzungstoken.
//! 2. Server antwortet mit [`ServerMsg::Welcome`] (angemeldet oder nicht) oder
//!    [`ServerMsg::Outdated`].
//! 3. Ohne Konto: [`ClientMsg::LinkStart`] liefert einen Code, den der Spieler auf der
//!    Webseite bestätigt; danach schickt der Server [`ServerMsg::Linked`] mit neuem Token.
//! 4. Lobby erstellen oder beitreten, bereit melden; der Host startet die Partie.

#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use serde::{Deserialize, Serialize};

/// Wird erhöht, sobald sich Nachrichten inkompatibel ändern (auch die des Spiels im Relay).
pub const VERSION: u16 = 1;
/// Pfad der WebSocket-Verbindung auf dem Server.
pub const WS_PATH: &str = "/api/mp/ws";
/// Seite der Webseite, auf der man ein Konto anlegt und Geräte verknüpft.
pub const ACCOUNT_PATH: &str = "/konto/";
pub const MAX_PLAYERS: usize = 4;
/// Größte erlaubte Nachricht (Bytes) in beide Richtungen.
pub const MAX_MESSAGE: usize = 64 * 1024;
pub const NAME_MIN: usize = 3;
pub const NAME_MAX: usize = 16;
pub const PASSWORD_MIN: usize = 8;
pub const PASSWORD_MAX: usize = 128;
pub const LOBBY_CODE_LEN: usize = 5;
pub const LINK_CODE_LEN: usize = 8;
/// Zeichen für Lobby- und Verknüpfungscodes (ohne leicht verwechselbare wie 0/O, 1/I).
pub const CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/// Nachricht vom Spiel an den Server.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ClientMsg {
    /// Erste Nachricht jeder Verbindung.
    Hello {
        version: u16,
        token: Option<String>,
    },
    /// Code anfordern, um dieses Gerät auf der Webseite mit einem Konto zu verknüpfen.
    LinkStart,
    /// Sitzung dieses Geräts beenden.
    Logout,
    ListLobbies,
    CreateLobby {
        public: bool,
    },
    JoinLobby {
        code: String,
    },
    LeaveLobby,
    SetReady {
        ready: bool,
    },
    /// Nur der Host: Partie starten (alle anderen müssen bereit sein).
    StartGame,
    /// Nur der Host: Partie ist vorbei; der Server speichert die Statistik und die Lobby
    /// kehrt in den Warteraum zurück.
    GameOver {
        result: MatchResult,
    },
    /// Spieldaten an andere Mitglieder der laufenden Partie.
    Relay {
        to: Target,
        data: Vec<u8>,
    },
    Ping {
        id: u32,
    },
}

/// Empfänger einer weitergeleiteten Nachricht.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// Der Host der Partie
    Host,
    /// Alle anderen Mitglieder
    Others,
    /// Ein bestimmtes Mitglied (Platznummer)
    Player(u8),
}

/// Nachricht vom Server an das Spiel.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ServerMsg {
    Welcome {
        account: Option<Account>,
    },
    /// Protokollversion passt nicht; das Spiel (oder der Server) muss aktualisiert werden.
    Outdated {
        server: u16,
    },
    LinkCode {
        code: String,
    },
    /// Gerät wurde auf der Webseite verknüpft; das Token gehört gespeichert.
    Linked {
        token: String,
        account: Account,
    },
    SignedOut,
    Lobbies {
        list: Vec<LobbyInfo>,
    },
    /// Aktueller Stand der eigenen Lobby (nach jeder Änderung).
    Lobby(LobbyView),
    /// Man ist nicht mehr in einer Lobby.
    LobbyClosed {
        reason: CloseReason,
    },
    Error {
        code: ErrorCode,
    },
    /// Die Partie beginnt; alle Mitglieder laden die Karte.
    Start {
        seed: u64,
    },
    Relay {
        from: u8,
        data: Vec<u8>,
    },
    /// Ein Mitglied hat die laufende Partie verlassen.
    PlayerLeft {
        slot: u8,
    },
    Pong {
        id: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub name: String,
}

/// Eintrag in der Liste offener Lobbys.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LobbyInfo {
    pub code: String,
    pub host: String,
    pub players: u8,
    pub playing: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LobbyView {
    pub code: String,
    pub public: bool,
    /// Platz des Hosts
    pub host: u8,
    /// Eigener Platz
    pub you: u8,
    pub playing: bool,
    pub members: Vec<LobbyMember>,
}

impl LobbyView {
    pub fn is_host(&self) -> bool {
        self.host == self.you
    }

    pub fn member(&self, slot: u8) -> Option<&LobbyMember> {
        self.members.iter().find(|m| m.slot == slot)
    }

    /// Alle außer dem Host sind bereit (der Host startet selbst).
    pub fn all_ready(&self) -> bool {
        self.members.iter().all(|m| m.ready || m.slot == self.host)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LobbyMember {
    pub slot: u8,
    pub name: String,
    pub ready: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// Selbst verlassen
    Left,
    HostLeft,
    ServerShutdown,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Erst `Hello` senden
    NoHello,
    NotSignedIn,
    LobbyNotFound,
    LobbyFull,
    AlreadyPlaying,
    NotInLobby,
    NotHost,
    NotReady,
    TooManyRequests,
    Internal,
}

/// Ergebnis einer Partie (vom Host gemeldet).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MatchResult {
    pub round: u32,
    pub players: Vec<MatchPlayer>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MatchPlayer {
    pub slot: u8,
    pub kills: u32,
    pub headshots: u32,
    pub downs: u32,
    pub revives: u32,
    pub points: u64,
}

/// Nachricht binär kodieren.
pub fn encode<T: Serialize>(msg: &T) -> Vec<u8> {
    postcard::to_allocvec(msg).unwrap_or_default()
}

/// Nachricht dekodieren; `None` bei ungültigen Daten.
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Option<T> {
    postcard::from_bytes(bytes).ok()
}

/// Erlaubter Kontoname: 3–16 Zeichen aus Buchstaben, Ziffern, `_`, `-` und `.`.
pub fn valid_name(name: &str) -> bool {
    (NAME_MIN..=NAME_MAX).contains(&name.chars().count())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

pub fn valid_password(password: &str) -> bool {
    (PASSWORD_MIN..=PASSWORD_MAX).contains(&password.chars().count())
}

/// Eingabe eines Codes vereinheitlichen: Großbuchstaben, nur Zeichen des Alphabets.
pub fn normalize_code(input: &str) -> String {
    input
        .chars()
        .map(|c| c.to_ascii_uppercase())
        .filter(|c| c.is_ascii() && CODE_ALPHABET.contains(&(*c as u8)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;
    use alloc::vec;

    #[test]
    fn messages_round_trip() {
        let msg = ClientMsg::Relay {
            to: Target::Player(2),
            data: vec![1, 2, 3],
        };
        assert_eq!(decode::<ClientMsg>(&encode(&msg)), Some(msg));
        let msg = ServerMsg::Lobby(LobbyView {
            code: "ABCDE".to_string(),
            public: true,
            host: 0,
            you: 1,
            playing: false,
            members: vec![LobbyMember {
                slot: 0,
                name: "Anna".to_string(),
                ready: false,
            }],
        });
        assert_eq!(decode::<ServerMsg>(&encode(&msg)), Some(msg));
        assert_eq!(decode::<ServerMsg>(&[0xff, 0xff, 0xff]), None);
    }

    #[test]
    fn names_and_codes() {
        assert!(valid_name("Zombie_Jäger".replace('ä', "ae").as_str()));
        assert!(!valid_name("ab"));
        assert!(!valid_name("mit leerzeichen"));
        assert!(!valid_name("<script>"));
        assert_eq!(normalize_code(" ab-c0d e"), "ABCDE");
    }
}
