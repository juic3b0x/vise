use crate::{
    error::{Error, Result},
    model::{Chunk, ChunkStatus, Record, Status},
};
use std::path::Path;
use url::Url;

const MIN_CHUNK_SIZE: u64 = 1024 * 1024;

pub(super) fn make_chunks(total: Option<u64>, ranged: bool, connections: usize) -> Vec<Chunk> {
    if !ranged || total == Some(0) {
        return vec![Chunk {
            start: 0,
            end: total,
            written: 0,
            status: ChunkStatus::Queued,
        }];
    }
    let total = total.unwrap_or(0);
    let count = (connections as u64)
        .min(total.div_ceil(MIN_CHUNK_SIZE))
        .max(1);
    let size = total.div_ceil(count);
    (0..count)
        .filter_map(|i| {
            let start = i * size;
            (start < total).then_some(Chunk {
                start,
                end: Some(start.saturating_add(size).min(total)),
                written: 0,
                status: ChunkStatus::Queued,
            })
        })
        .collect()
}

pub(super) use crate::input::validate_url;

pub(super) fn validate_filename(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 200
        || name == "."
        || name == ".."
        || name.starts_with('.')
        || name
            .chars()
            .any(|c| c.is_control() || "/\\:<>\"|?*".contains(c))
        || name.ends_with(['.', ' '])
    {
        return Err(Error::Invalid(
            "filename must be a plain, non-hidden filename of at most 200 bytes".into(),
        ));
    }
    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
    if [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ]
    .contains(&stem.as_str())
    {
        return Err(Error::Invalid("reserved filename".into()));
    }
    Ok(())
}

pub(super) fn default_filename(url: &Url) -> String {
    let raw = url
        .path_segments()
        .and_then(|mut parts| parts.next_back())
        .unwrap_or("");
    let mut name: String = raw
        .chars()
        .map(|c| {
            if c.is_control() || "/\\:<>\"|?*%".contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(120)
        .collect();
    // Leave room for a numeric collision suffix even with multi-byte filenames.
    while name.len() > 160 {
        name.pop();
    }
    if validate_filename(&name).is_ok() {
        name
    } else {
        "download.bin".into()
    }
}
pub(super) fn numbered_filename(base: &str, number: usize) -> String {
    let path = Path::new(base);
    match (
        path.file_stem().and_then(|s| s.to_str()),
        path.extension().and_then(|s| s.to_str()),
    ) {
        (Some(stem), Some(ext)) => format!("{stem} ({number}).{ext}"),
        _ => format!("{base} ({number})"),
    }
}

pub(super) fn normalize_chunks(record: &mut Record) {
    for chunk in &mut record.chunks {
        chunk.status = if record.status == Status::Completed
            || chunk
                .end
                .is_some_and(|end| chunk.start + chunk.written == end)
        {
            ChunkStatus::Completed
        } else {
            match record.status {
                Status::Paused => ChunkStatus::Paused,
                Status::Failed | Status::Corrupt => ChunkStatus::Failed,
                Status::Queued | Status::Probing => ChunkStatus::Queued,
                Status::Downloading | Status::Verifying => chunk.status,
                Status::Completed => ChunkStatus::Completed,
            }
        };
    }
}

pub(super) fn validate_record(record: &Record) -> Result<()> {
    validate_url(&record.url)?;
    validate_filename(&record.filename)?;
    crate::paths::validate(record)?;
    record.checksums.validate()?;
    if record
        .cookies_file
        .as_ref()
        .is_some_and(|path| !path.is_absolute() || path.to_str().is_none())
    {
        return Err(Error::Invalid(
            "journal cookie file path must be absolute UTF-8".into(),
        ));
    }
    if record.created_at < 0 {
        return Err(Error::Invalid(
            "negative creation timestamp in journal".into(),
        ));
    }
    let mut offset = 0;
    for chunk in &record.chunks {
        if chunk.start != offset
            || chunk
                .end
                .is_some_and(|end| end < chunk.start || chunk.written > end - chunk.start)
            || chunk.start.checked_add(chunk.written).is_none()
        {
            return Err(Error::Invalid("invalid chunk offsets in state file".into()));
        }
        offset = chunk.end.unwrap_or(0);
    }
    if record.ranged
        && !record.chunks.is_empty()
        && (record.total_bytes != Some(offset) || record.chunks.iter().any(|c| c.end.is_none()))
    {
        return Err(Error::Invalid("invalid range layout in state file".into()));
    }
    if !record.ranged && record.chunks.len() > 1 {
        return Err(Error::Invalid("invalid stream layout in state file".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunks_cover_exactly_once() {
        for total in [
            1,
            MIN_CHUNK_SIZE,
            MIN_CHUNK_SIZE + 1,
            17 * MIN_CHUNK_SIZE + 7,
        ] {
            let chunks = make_chunks(Some(total), true, 8);
            let mut cursor = 0;
            for chunk in chunks {
                assert_eq!(chunk.start, cursor);
                cursor = chunk.end.unwrap();
            }
            assert_eq!(cursor, total);
        }
    }
    #[test]
    fn rejects_unsafe_paths_and_urls() {
        for name in ["../file", ".vise", "a/b", "a\\b", "CON.txt", "", "x\n"] {
            assert!(validate_filename(name).is_err());
        }
        assert!(validate_url("file:///etc/passwd").is_err());
        assert!(validate_url("https://user:pass@example.com/a").is_err());
        assert!(validate_url("https://example.com/a").is_ok());
    }
}
