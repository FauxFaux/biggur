#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use std::time::Duration;

use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use reqwest::Client;
use rusqlite::types::ToSql;
use rusqlite::Statement;
use serde_json::Value;

type Instant = chrono::DateTime<chrono::Utc>;

struct Cache {
    db: rusqlite::Connection,
    client: reqwest::Client,
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let cache = Cache {
        db: rusqlite::Connection::open("biggur.db")?,
        client: reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(5))
            .build()?,
    };

    for gallery in &["viral", "rising"] {
        for page in 0..=5 {
            let url = format!(
                "https://api.imgur.com/3/gallery/hot/{}/{}.json",
                gallery, page
            );

            let data = cache
                .fetch(&url)
                .with_context(|_| format_err!("fetching {:?}", url))?;
        }
    }

    Ok(())
}

impl Cache {
    fn fetch(&self, url: &str) -> Result<Value, Error> {
        let now = now();

        if let Some(cached_at) = self
            .db
            .prepare_cached("select max(occurred) from raw where url=?")?
            .query_row(&[url], |row| row.get::<_, Option<Instant>>(0))?
        {
            trace!("{:?}: exists in cache, from {:?}", url, cached_at);
            if cached_at.signed_duration_since(now).num_seconds() < 60 * 60 {
                return Ok(self
                    .db
                    .prepare_cached("select returned from raw where occurred=? and url=?")?
                    .query_row(&[&cached_at as &ToSql, &url], |row| row.get(0))?);
            } else {
                trace!("{:?}: ...but was too old", url)
            }
        }

        info!("{:?}: fetching", url);
        let body: Value = self.client.get(url).send()?.json()?;
        trace!("returned: {:?}", body);

        let body = unpack_response(&body)
            .with_context(|_| format_err!("unpacking {:?}, which returned {:?}", url, body))?;

        let mut write_raw = self
            .db
            .prepare_cached("insert into raw (occurred, url, returned) values (?,?,?)")?;
        write_raw.insert(&[&now as &ToSql, &url, body])?;

        Ok(body.to_owned())
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
