#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

mod cache;
mod whole;

use std::collections::HashSet;
use std::env;
use std::time::Duration;

use cast::u64;
use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use serde_json::Value;

type Instant = chrono::DateTime<chrono::Utc>;
type JsonObj = serde_json::Map<String, Value>;

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    let cache = cache::Cache {
        db: rusqlite::Connection::open("biggur.db")?,
        client: reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(15))
            .build()?,
        client_id: env::var("IMGUR_CLIENT_ID")
            .with_context(|_| err_msg("loading IMGUR_CLIENT_ID from environment"))?,
    };

    for gallery in &["viral", "rising"] {
        let expanded = load_expanded(&cache, &gallery)?;

        whole::write_whole(gallery, &expanded)?;
    }

    Ok(())
}

fn load_expanded(cache: &cache::Cache, gallery: &str) -> Result<Vec<(JsonObj, Vec<Value>)>, Error> {
    let mut albums = Vec::with_capacity(300);
    for page in 0..=5 {
        let url = format!(
            "https://api.imgur.com/3/gallery/hot/{}/{}.json",
            gallery, page
        );

        albums.extend(
            cache
                .fetch(&url, 60 * 60)
                .with_context(|_| format_err!("fetching {:?}", url))?
                .as_array()
                .ok_or(err_msg("data should be array"))?
                .into_iter()
                .map(|v| v.to_owned()),
        );
    }

    let mut already_seen = HashSet::with_capacity(albums.len());
    let mut expanded = Vec::with_capacity(albums.len());

    for album in albums {
        let album = album.as_object().ok_or(err_msg("non-object item"))?;

        let id = album
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or(err_msg("id is mandatory"))?;

        if !already_seen.insert(id.to_string()) {
            continue;
        }

        expanded.push((album.to_owned(), expand_images(cache, &album, &id)?));
    }

    Ok(expanded)
}

fn expand_images(cache: &cache::Cache, album: &JsonObj, id: &str) -> Result<Vec<Value>, Error> {
    if let Some(images) = album.get("images").and_then(|images| images.as_array()) {
        let images_count = album
            .get("images_count")
            .and_then(|count| count.as_u64())
            .ok_or(err_msg("images but no images_count"))?;

        if images_count <= u64(images.len()) {
            // we already have them all!
            Ok(images.to_owned())
        } else {
            Ok(cache
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
                .to_owned())
        }
    } else {
        Ok(vec![Value::Object(album.to_owned())])
    }
}
