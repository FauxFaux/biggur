use std::thread;
use std::time::Duration;

use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use r2d2::ManageConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reqwest::header::AUTHORIZATION;
use rusqlite::types::ToSql;
use serde_json::Value;

use super::Instant;

pub struct Cache {
    pub raw: SqliteConnectionManager,
    pub client: reqwest::Client,
    pub client_id: String,
}

impl Cache {
    pub fn fetch(&self, url: &str, cache_secs: i64) -> Result<Value, Error> {
        let now = now();
        let db = self.raw.connect()?;

        if let Some(cached_at) = db
            .prepare_cached("select max(occurred) from raw where url=?")?
            .query_row(&[url], |row| row.get::<_, Option<Instant>>(0))?
        {
            info!("{:?}: exists in cache, from {:?}", url, cached_at);
            if now.signed_duration_since(cached_at).num_seconds() < cache_secs {
                return Ok(db
                    .prepare_cached("select returned from raw where occurred=? and url=?")?
                    .query_row(&[&cached_at as &ToSql, &url], |row| row.get(0))?);
            } else {
                trace!("{:?}: ...but was too old", url)
            }
        }

        let body = self.try_fetch_body(url)?;

        let mut write_raw =
            db.prepare_cached("insert into raw (occurred, url, returned) values (?,?,?)")?;
        write_raw.insert(&[&now as &ToSql, &url, &body])?;

        Ok(body)
    }

    fn try_fetch_body(&self, url: &str) -> Result<Value, Error> {
        for fetch in 0..4 {
            match self.actually_fetch_body(url) {
                Ok(val) => return Ok(val),
                Err(e) => {
                    warn!("fetch {} failed, will re-try. {:?}", fetch, e);
                }
            }

            thread::sleep(Duration::from_secs(15));
        }

        self.actually_fetch_body(url)
    }

    fn actually_fetch_body(&self, url: &str) -> Result<Value, Error> {
        info!("{:?}: fetching", url);

        let body: Value = self
            .client
            .get(url)
            .header(AUTHORIZATION, format!("Client-ID {}", self.client_id))
            .send()?
            .json()?;

        trace!("returned: {:?}", body);

        Ok(unpack_response(&body)
            .with_context(|_| format_err!("unpacking {:?}, which returned {:?}", url, body))?
            .to_owned())
    }
}

fn unpack_response(body: &Value) -> Result<&Value, Error> {
    let body = body.as_object().ok_or(err_msg("root wasn't object"))?;
    ensure!(
        body.get("success")
            .and_then(|success| success.as_bool())
            .unwrap_or(false),
        "request wasn't success"
    );
    Ok(body.get("data").ok_or(err_msg("data absent"))?)
}

fn now() -> Instant {
    chrono::Utc::now()
}
