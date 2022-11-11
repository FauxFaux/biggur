#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

use std::collections::HashSet;
use std::convert::TryFrom;
use std::env;
use std::fs;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::header::AUTHORIZATION;
use rusqlite::types::ToSql;
use serde_json::Value;

type Instant = chrono::DateTime<chrono::Utc>;

struct Cache {
    db: rusqlite::Connection,
    client: reqwest::blocking::Client,
    client_id: String,
}

fn main() -> Result<()> {
    pretty_env_logger::init();

    let cache = Cache {
        db: rusqlite::Connection::open("biggur.db")?,
        client: reqwest::blocking::ClientBuilder::new()
            .timeout(Duration::from_secs(15))
            .build()?,
        client_id: env::var("IMGUR_CLIENT_ID")
            .context("loading IMGUR_CLIENT_ID from environment")?,
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
                    .with_context(|| anyhow!("fetching {:?}", url))?
                    .as_array()
                    .ok_or(anyhow!("data should be array"))?
                    .into_iter()
                    .map(|v| v.to_owned()),
            );
        }

        let mut already_seen = HashSet::with_capacity(items.len());
        let mut whole = Vec::with_capacity(items.len());

        for item in items {
            let item = item.as_object().ok_or(anyhow!("non-object item"))?;

            let id = item
                .get("id")
                .and_then(|id| id.as_str())
                .ok_or(anyhow!("id is mandatory"))?;

            if !already_seen.insert(id.to_string()) {
                continue;
            }

            let title = item
                .get("title")
                .and_then(|title| title.as_str())
                .ok_or(anyhow!("title is mandatory"))?;

            let images =
                if let Some(images) = item.get("images").and_then(|images| images.as_array()) {
                    let images_count = item
                        .get("images_count")
                        .and_then(|count| count.as_u64())
                        .ok_or(anyhow!("images but no images_count"))?;

                    if images_count <= u64::try_from(images.len()).expect("usize u64") {
                        // we already have them all!
                        images.to_owned()
                    } else {
                        cache
                            .fetch(
                                &format!("https://api.imgur.com/3/album/{}", id),
                                2 * 24 * 60 * 60,
                            )?
                            .as_object()
                            .ok_or(anyhow!("album must be an object"))?
                            .get("images")
                            .ok_or(anyhow!("album must contain images"))?
                            .as_array()
                            .ok_or(anyhow!("album images must be an array"))?
                            .to_owned()
                    }
                } else {
                    vec![Value::Object(item.to_owned())]
                };

            let images: Result<Vec<_>> = images.into_iter().map(map_img).collect();

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

fn map_img(img: Value) -> Result<Value> {
    let (format, size) = if let Some(mp4_size) = img.get("mp4_size").and_then(|mp4| mp4.as_u64()) {
        ("mp4", mp4_size)
    } else {
        (
            extension(
                img.get("link")
                    .ok_or(anyhow!("image must have link"))?
                    .as_str()
                    .ok_or(anyhow!("link must be a string"))?,
            )?,
            img.get("size")
                .and_then(|size| size.as_u64())
                .ok_or(anyhow!("size is always present"))?,
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

fn extension(url: &str) -> Result<&str> {
    Ok(&url[url.rfind('.').ok_or(anyhow!("no dot"))? + 1..])
}

impl Cache {
    fn fetch(&self, url: &str, cache_secs: i64) -> Result<Value> {
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
                    .query_row(&[&cached_at as &dyn ToSql, &url], |row| row.get(0))?);
            } else {
                trace!("{:?}: ...but was too old", url)
            }
        }

        let body = self.try_fetch_body(url)?;

        let mut write_raw = self
            .db
            .prepare_cached("insert into raw (occurred, url, returned) values (?,?,?)")?;
        write_raw.insert(&[&now as &dyn ToSql, &url, &body])?;

        Ok(body)
    }

    fn try_fetch_body(&self, url: &str) -> Result<Value> {
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

    fn actually_fetch_body(&self, url: &str) -> Result<Value> {
        info!("{:?}: fetching", url);

        let body: Value = self
            .client
            .get(url)
            .header(AUTHORIZATION, format!("Client-ID {}", self.client_id))
            .send()?
            .json()?;

        trace!("returned: {:?}", body);

        Ok(unpack_response(&body)
            .with_context(|| anyhow!("unpacking {:?}, which returned {:?}", url, body))?
            .to_owned())
    }
}

fn unpack_response(body: &Value) -> Result<&Value> {
    let body = body.as_object().ok_or(anyhow!("root wasn't object"))?;
    ensure!(
        body.get("success")
            .and_then(|success| success.as_bool())
            .unwrap_or(false),
        "request wasn't success"
    );
    Ok(body.get("data").ok_or(anyhow!("data absent"))?)
}

fn now() -> Instant {
    chrono::Utc::now()
}
