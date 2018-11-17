#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

mod cache;
mod whole;

use std::env;
use std::thread;
use std::time::Duration;

use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use r2d2_sqlite::SqliteConnectionManager;
use serde_json::Value;

type Instant = chrono::DateTime<chrono::Utc>;
type JsonObj = serde_json::Map<String, Value>;

fn main() -> Result<(), Error> {
    pretty_env_logger::init();
    let client_id = env::var("IMGUR_CLIENT_ID")
        .with_context(|_| err_msg("loading IMGUR_CLIENT_ID from environment"))?;

    let r2 = r2d2_sqlite::SqliteConnectionManager::file("biggur.db");

    setup_watch_hot(r2, client_id)?;

    rouille::start_server("0.0.0.0:5812", |req| unimplemented!());
}

fn setup_watch_hot(db: SqliteConnectionManager, client_id: String) -> Result<(), Error> {
    let cache = cache::Cache {
        client: http_client_with_timeout(15)?,
        client_id,
        db,
    };

    whole::load_and_write_whole(&cache)?;

    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(30 * 60));

        if let Err(e) = whole::load_and_write_whole(&cache) {
            error!("writing whole failed: {:?}", e);
        }
    });

    Ok(())
}

fn http_client_with_timeout(secs: u64) -> Result<reqwest::Client, Error> {
    Ok(reqwest::ClientBuilder::new()
        .timeout(Duration::from_secs(secs))
        .build()?)
}
