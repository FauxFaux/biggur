use std::fs;

use failure::err_msg;
use failure::Error;
use serde_json::Value;

use super::JsonObj;

pub fn write_whole(gallery: &str, expanded: &[(JsonObj, Vec<Value>)]) -> Result<(), Error> {
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
