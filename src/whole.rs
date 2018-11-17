use std::collections::HashSet;
use std::fs;

use cast::u64;
use failure::err_msg;
use failure::Error;
use failure::ResultExt;
use serde_json::Value;

use crate::cache::Cache;
use crate::JsonObj;

pub fn load_and_write_whole(cache: &Cache) -> Result<(), Error> {
    for gallery in &["viral", "rising"] {
        let expanded = load_expanded(&cache, &gallery)?;

        write_whole(gallery, &expanded)?;
    }

    Ok(())
}

fn load_expanded(cache: &Cache, gallery: &str) -> Result<Vec<(JsonObj, Vec<Value>)>, Error> {
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

fn expand_images(cache: &Cache, album: &JsonObj, id: &str) -> Result<Vec<Value>, Error> {
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

fn write_whole(gallery: &str, expanded: &[(JsonObj, Vec<Value>)]) -> Result<(), Error> {
    let mut whole = Vec::with_capacity(expanded.len());

    for (album, images) in expanded {
        let id = album
            .get("id")
            .and_then(|id| id.as_str())
            .ok_or(err_msg("id is mandatory"))?;

        let title = album
            .get("title")
            .and_then(|title| title.as_str())
            .ok_or(err_msg("title is mandatory"))?;

        let images: Result<Vec<_>, Error> = images.into_iter().map(map_img).collect();

        whole.push(json!({
                "id": id,
                "title": title,
                "images": images?,
            }));
    }

    serde_json::to_writer(fs::File::create(format!("{}.json", gallery))?, &whole)?;

    Ok(())
}

fn map_img(img: &Value) -> Result<Value, Error> {
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
