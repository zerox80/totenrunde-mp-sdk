//! JSON-API der Konto-Webseite: registrieren, anmelden, Profil, Gerät verknüpfen,
//! Bestenliste. Angemeldet wird mit `Authorization: Bearer <token>`; Geräte werden erst
//! nach ausdrücklicher Bestätigung auf der Konto-Seite verknüpft.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::db::{AccountRow, Device, RegisterError};
use super::{Multiplayer, PeerAddr};

#[derive(Deserialize)]
pub struct Credentials {
    name: String,
    password: String,
}

#[derive(Deserialize)]
pub struct PasswordBody {
    password: String,
}

#[derive(Deserialize)]
pub struct CodeBody {
    code: String,
}

#[derive(Serialize)]
struct SessionOut {
    token: String,
    name: String,
}

fn error(status: StatusCode, code: &str) -> Response {
    (status, Json(json!({ "error": code }))).into_response()
}

fn ip(peer: Option<Extension<PeerAddr>>) -> IpAddr {
    peer.map(|p| p.0.0)
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?.trim();
    (token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit())).then(|| token.to_owned())
}

/// Blockierende Datenbankarbeit außerhalb des Async-Laufzeitsystems.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(f).await.ok()
}

/// Passwortarbeit vor dem Einreihen begrenzen und Versuche einschließlich laufender zählen.
async fn password_work<T: Send + 'static>(
    mp: &Multiplayer,
    fail_key: Option<&str>,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Response> {
    let permit = mp
        .password_jobs
        .clone()
        .try_acquire_owned()
        .map_err(|_| error(StatusCode::TOO_MANY_REQUESTS, "rate_limited"))?;
    if let Some(key) = fail_key
        && !mp.limits.allow(key.to_owned(), 8, Duration::from_secs(900))
    {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, "rate_limited"));
    }
    blocking(move || {
        // Ein abgebrochener HTTP-Request darf die noch laufende Argon2-Arbeit nicht freigeben.
        let _permit = permit;
        f()
    })
    .await
    .ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "internal"))
}

async fn signed_in(mp: &Multiplayer, headers: &HeaderMap) -> Option<(AccountRow, String)> {
    let token = bearer(headers)?;
    let db = mp.db.clone();
    let t = token.clone();
    let account = blocking(move || db.account_for_token(&t)).await.flatten()?;
    Some((account, token))
}

pub async fn register(
    State(mp): State<Multiplayer>,
    peer: Option<Extension<PeerAddr>>,
    Json(c): Json<Credentials>,
) -> Response {
    if !mp.limits.allow(
        format!("register:{}", ip(peer)),
        6,
        Duration::from_secs(3600),
    ) {
        return error(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    }
    let db = mp.db.clone();
    let name = c.name.trim().to_owned();
    let result = password_work(&mp, None, move || {
        let account = db.register(&name, &c.password)?;
        let token = db
            .create_session(account.id, Device::Website)
            .ok_or(RegisterError::Internal)?;
        Ok::<_, RegisterError>(SessionOut {
            token,
            name: account.name,
        })
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(response) => return response,
    };
    match result {
        Ok(out) => (StatusCode::CREATED, Json(out)).into_response(),
        Err(RegisterError::Invalid) => error(StatusCode::BAD_REQUEST, "invalid"),
        Err(RegisterError::NameTaken) => error(StatusCode::CONFLICT, "name_taken"),
        _ => error(StatusCode::INTERNAL_SERVER_ERROR, "internal"),
    }
}

pub async fn login(
    State(mp): State<Multiplayer>,
    peer: Option<Extension<PeerAddr>>,
    Json(c): Json<Credentials>,
) -> Response {
    let name = c.name.trim().to_owned();
    let fail_key = format!("fail:{}", name.to_lowercase());
    if !mp
        .limits
        .allow(format!("login:{}", ip(peer)), 30, Duration::from_secs(600))
    {
        return error(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    }
    let db = mp.db.clone();
    let result = password_work(&mp, Some(&fail_key), move || {
        let account = db.login(&name, &c.password)?;
        let token = db.create_session(account.id, Device::Website)?;
        Some(SessionOut {
            token,
            name: account.name,
        })
    })
    .await;
    let result = match result {
        Ok(result) => result,
        Err(response) => return response,
    };
    match result {
        Some(out) => {
            mp.limits.reset(&fail_key);
            Json(out).into_response()
        }
        None => error(StatusCode::UNAUTHORIZED, "wrong_credentials"),
    }
}

pub async fn logout(State(mp): State<Multiplayer>, headers: HeaderMap) -> StatusCode {
    if let Some(token) = bearer(&headers) {
        let db = mp.db.clone();
        blocking(move || db.delete_session(&token)).await;
    }
    StatusCode::NO_CONTENT
}

pub async fn me(State(mp): State<Multiplayer>, headers: HeaderMap) -> Response {
    let Some((account, _)) = signed_in(&mp, &headers).await else {
        return error(StatusCode::UNAUTHORIZED, "signed_out");
    };
    let db = mp.db.clone();
    let id = account.id;
    let Some((created, stats)) = blocking(move || (db.created(id), db.stats(id))).await else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    };
    Json(json!({ "name": account.name, "created": created, "stats": stats })).into_response()
}

pub async fn delete_account(
    State(mp): State<Multiplayer>,
    headers: HeaderMap,
    Json(body): Json<PasswordBody>,
) -> Response {
    let Some((account, _)) = signed_in(&mp, &headers).await else {
        return error(StatusCode::UNAUTHORIZED, "signed_out");
    };
    let fail_key = format!("fail:{}", account.name.to_lowercase());
    let db = mp.db.clone();
    let deleted = match password_work(&mp, Some(&fail_key), move || {
        db.verify(account.id, &body.password) && db.delete_account(account.id)
    })
    .await
    {
        Ok(deleted) => deleted,
        Err(response) => return response,
    };
    if deleted {
        mp.limits.reset(&fail_key);
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(StatusCode::FORBIDDEN, "wrong_credentials")
    }
}

/// Gerät mit dem angemeldeten Konto verknüpfen: Das wartende Spiel erhält eine eigene Sitzung.
pub async fn link(
    State(mp): State<Multiplayer>,
    peer: Option<Extension<PeerAddr>>,
    headers: HeaderMap,
    Json(body): Json<CodeBody>,
) -> Response {
    if !mp
        .limits
        .allow(format!("link:{}", ip(peer)), 30, Duration::from_secs(600))
    {
        return error(StatusCode::TOO_MANY_REQUESTS, "rate_limited");
    }
    let Some((account, _)) = signed_in(&mp, &headers).await else {
        return error(StatusCode::UNAUTHORIZED, "signed_out");
    };
    if !mp.hub.link_pending(&body.code) {
        return error(StatusCode::NOT_FOUND, "unknown_code");
    }
    let db = mp.db.clone();
    let id = account.id;
    let Some(Some(token)) = blocking(move || db.create_session(id, Device::Game)).await else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "internal");
    };
    if mp.hub.complete_link(&body.code, account, token.clone()) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        let db = mp.db.clone();
        blocking(move || db.delete_session(&token)).await;
        error(StatusCode::NOT_FOUND, "unknown_code")
    }
}

#[derive(Deserialize)]
pub struct CodeQuery {
    code: String,
}

pub async fn link_status(State(mp): State<Multiplayer>, Query(q): Query<CodeQuery>) -> Response {
    Json(json!({ "valid": mp.hub.link_pending(&q.code) })).into_response()
}

pub async fn leaderboard(State(mp): State<Multiplayer>) -> Response {
    let db = mp.db.clone();
    let board = blocking(move || db.leaderboard(25))
        .await
        .unwrap_or_default();
    Json(board).into_response()
}

pub async fn status(State(mp): State<Multiplayer>) -> Response {
    let (online, lobbies) = mp.hub.online();
    Json(json!({ "online": online, "lobbies": lobbies })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn account_lifecycle_releases_password_capacity() {
        let mp = Multiplayer::new(super::super::Db::in_memory());
        let credentials = || Credentials {
            name: "Anna".to_owned(),
            password: "geheim123".to_owned(),
        };
        let registered = register(State(mp.clone()), None, Json(credentials())).await;
        assert_eq!(registered.status(), StatusCode::CREATED);
        assert_eq!(mp.password_jobs.available_permits(), 2);

        let response = login(State(mp.clone()), None, Json(credentials())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let session: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {}", session["token"].as_str().unwrap())
                .parse()
                .unwrap(),
        );
        assert_eq!(mp.password_jobs.available_permits(), 2);
        assert_eq!(
            me(State(mp.clone()), headers.clone()).await.status(),
            StatusCode::OK
        );
        let deleted = delete_account(
            State(mp.clone()),
            headers.clone(),
            Json(PasswordBody {
                password: credentials().password,
            }),
        )
        .await;
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        assert_eq!(mp.password_jobs.available_permits(), 2);
        assert_eq!(
            me(State(mp), headers).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
