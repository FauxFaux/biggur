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

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let db = rusqlite::Connection::open("biggur.db")?;
    let client = reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(5))
        .build()?;

    let mut write_raw = db.prepare("insert into raw (occurred, url, returned) values (?,?,?)")?;

    for gallery in &["viral", "rising"] {
        for page in 0..=5 {
            let url = format!(
                "https://api.imgur.com/3/gallery/hot/{}/{}.json",
                gallery, page
            );

            let data = fetch(&client, &mut write_raw, &url)
                .with_context(|_| format_err!("fetching {:?}", url))?;
        }
    }

    Ok(())
}

fn fetch(client: &Client, write_raw: &mut Statement, url: &str) -> Result<Value, Error> {
    info!("fetch: {:?}", url);
    let body: Value = client.get(url).send()?.json()?;
    trace!("returned: {:?}", body);

    let body = unpack_response(&body)
        .with_context(|_| format_err!("unpacking {:?}, which returned {:?}", url, body))?;

    write_raw.insert(&[&now() as &ToSql, &url, body])?;

    Ok(body.to_owned())
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
