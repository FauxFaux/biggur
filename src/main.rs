#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

use std::collections::HashSet;
use std::env;
use std::fs;
use std::thread;
use std::time::Duration;

use cast::u64;
use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use reqwest::header::AUTHORIZATION;
use rusqlite::types::ToSql;
use serde_json::Value;

type Instant = chrono::DateTime<chrono::Utc>;

struct Cache {
    db: rusqlite::Connection,
    client: reqwest::Client,
    client_id: String,
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let cache = Cache {
        db: rusqlite::Connection::open("biggur.db")?,
        client: reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(15))
            .build()?,
        client_id: env::var("IMGUR_CLIENT_ID")
            .with_context(|_| err_msg("loading IMGUR_CLIENT_ID from environment"))?,
    };

    for gallery in &["viral", "rising"] {
        let mut items = Vec::with_capacity(300);

        for page in 0..=5 {
            let url = format!(
                "https://api.imgur.com/3/gallery/hot/{}/{}.json",
                gallery, page
            );

            items.extend(
                cache
                    .fetch(&url, 60 * 60)
                    .with_context(|_| format_err!("fetching {:?}", url))?
                    .as_array()
                    .ok_or(err_msg("data should be array"))?
                    .into_iter()
                    .map(|v| v.to_owned()),
            );
        }

        let mut already_seen = HashSet::with_capacity(items.len());
        let mut whole = Vec::with_capacity(items.len());

        for item in items {
            let item = item.as_object().ok_or(err_msg("non-object item"))?;

            let id = item
                .get("id")
                .and_then(|id| id.as_str())
                .ok_or(err_msg("id is mandatory"))?;

            if !already_seen.insert(id.to_string()) {
                continue;
            }

            let title = item
                .get("title")
                .and_then(|title| title.as_str())
                .ok_or(err_msg("title is mandatory"))?;

            let images =
                if let Some(images) = item.get("images").and_then(|images| images.as_array()) {
                    let images_count = item
                        .get("images_count")
                        .and_then(|count| count.as_u64())
                        .ok_or(err_msg("images but no images_count"))?;

                    if images_count <= u64(images.len()) {
                        // we already have them all!
                        images.to_owned()
                    } else {
                        cache
                            .fetch(
                                &format!("https://api.imgur.com/3/album/{}", id),
                                2 * 24 * 60 * 60,
                            )?
                            .as_object()
                            .ok_or(err_msg("album must be an object"))?
                            .get("images")
                            .ok_or(err_msg("album must contain images"))?
                            .as_array()
                            .ok_or(err_msg("album images must be an array"))?
                            .to_owned()
                    }
                } else {
                    vec![Value::Object(item.to_owned())]
                };

            let images: Result<Vec<_>, Error> = images.into_iter().map(map_img).collect();

            whole.push(json!({
                "id": id,
                "title": title,
                "images": images?,
            }));
        }

        serde_json::to_writer(fs::File::create(format!("{}.json", gallery))?, &whole)?;
    }

    Ok(())
}

fn map_img(img: Value) -> Result<Value, Error> {
    let (format, size) = if let Some(mp4_size) = img.get("mp4_size").and_then(|mp4| mp4.as_u64()) {
        ("mp4", mp4_size)
    } else {
        (
            extension(
                img.get("link")
                    .ok_or(err_msg("image must have link"))?
                    .as_str()
                    .ok_or(err_msg("link must be a string"))?,
            )?,
            img.get("size")
                .and_then(|size| size.as_u64())
                .ok_or(err_msg("size is always present"))?,
        )
    };

    Ok(json!({
        "id": img.get("id"),
        "w": img.get("width"),
        "h": img.get("width"),
        "size": size,
        "format": format,
        "desc": img.get("description"),
        "nsfw": img.get("nsfw"),
    }))
}

fn extension(url: &str) -> Result<&str, Error> {
    Ok(&url[url.rfind('.').ok_or(err_msg("no dot"))? + 1..])
}

impl Cache {
    fn fetch(&self, url: &str, cache_secs: i64) -> Result<Value, Error> {
        let now = now();

        if let Some(cached_at) = self
            .db
            .prepare_cached("select max(occurred) from raw where url=?")?
            .query_row(&[url], |row| row.get::<_, Option<Instant>>(0))?
        {
            info!("{:?}: exists in cache, from {:?}", url, cached_at);
            if now.signed_duration_since(cached_at).num_seconds() < cache_secs {
                return Ok(self
                    .db
                    .prepare_cached("select returned from raw where occurred=? and url=?")?
                    .query_row(&[&cached_at as &ToSql, &url], |row| row.get(0))?);
            } else {
                trace!("{:?}: ...but was too old", url)
            }
        }

        let body = self.try_fetch_body(url)?;

        let mut write_raw = self
            .db
            .prepare_cached("insert into raw (occurred, url, returned) values (?,?,?)")?;
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
