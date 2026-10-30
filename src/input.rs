//! Bounded UTF-8 URL-list ingestion shared by CLI files and API text uploads.
use std::{
    io::{BufRead, BufReader, Cursor, Read},
    path::{Path, PathBuf},
};

use crate::error::{Error, Result};
use url::Url;

const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_URLS: usize = 1000;
pub const MAX_BATCH_BODY_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn validate_url(input: &str) -> Result<Url> {
    if input.len() > 8192 {
        return Err(Error::Invalid(
            "URL is too long (maximum 8192 bytes)".into(),
        ));
    }
    let url = Url::parse(input).map_err(|e| Error::Invalid(format!("invalid URL: {e}")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Error::Invalid(
            "use an HTTP(S) URL without embedded credentials".into(),
        ));
    }
    Ok(url)
}

/// Preserve ordering and duplicate URLs; the engine assigns collision-safe names.
pub(crate) fn validate_urls(urls: Vec<String>) -> Result<Vec<String>> {
    if urls.is_empty() || urls.len() > MAX_URLS {
        return Err(Error::Invalid(format!(
            "provide between 1 and {MAX_URLS} URLs"
        )));
    }
    let mut bytes = urls.len().saturating_sub(1);
    for (index, url) in urls.iter().enumerate() {
        bytes = bytes.saturating_add(url.len());
        if bytes > MAX_INPUT_BYTES {
            return Err(Error::Invalid("URL list exceeds 1 MiB".into()));
        }
        validate_url(url)
            .map_err(|error| Error::Invalid(format!("entry {}: {error}", index + 1)))?;
    }
    Ok(urls)
}

pub(crate) fn parse(content: &str) -> Result<Vec<String>> {
    parse_reader(Cursor::new(content.as_bytes()))
}

fn parse_reader(reader: impl Read) -> Result<Vec<String>> {
    // A file that grows while it is being read still cannot exhaust memory.
    let mut reader = BufReader::new(reader.take(MAX_INPUT_BYTES as u64 + 1));
    let (mut urls, mut line, mut bytes, mut number) = (Vec::new(), String::new(), 0, 0);
    loop {
        line.clear();
        let count = reader.read_line(&mut line).map_err(|error| {
            Error::Invalid(format!(
                "cannot read UTF-8 input at line {}: {error}",
                number + 1
            ))
        })?;
        if count == 0 {
            break;
        }
        bytes += count;
        number += 1;
        if bytes > MAX_INPUT_BYTES {
            return Err(Error::Invalid("input exceeds 1 MiB".into()));
        }
        let value = if number == 1 {
            line.trim_start_matches('\u{feff}')
        } else {
            &line
        }
        .trim();
        if value.is_empty() || value.starts_with('#') {
            continue;
        }
        validate_url(value).map_err(|error| Error::Invalid(format!("line {number}: {error}")))?;
        urls.push(value.to_owned());
        if urls.len() > MAX_URLS {
            return Err(Error::Invalid(format!("input exceeds {MAX_URLS} URLs")));
        }
    }
    if urls.is_empty() {
        return Err(Error::Invalid("input contains no URLs".into()));
    }
    Ok(urls)
}

async fn read_file(path: PathBuf) -> Result<Vec<String>> {
    tokio::task::spawn_blocking(move || {
        read_regular_file(&path)
            .map_err(|error| Error::Invalid(format!("input file {}: {error}", path.display())))
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))?
}

fn read_regular_file(path: &Path) -> Result<Vec<String>> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(Error::Invalid("expected a regular UTF-8 text file".into()));
    }
    if metadata.len() > MAX_INPUT_BYTES as u64 {
        return Err(Error::Invalid("input exceeds 1 MiB".into()));
    }
    parse_reader(std::fs::File::open(path)?)
}

/// Positional URLs precede file entries. Validate everything before engine startup.
pub async fn load(mut urls: Vec<String>, path: Option<PathBuf>) -> Result<Vec<String>> {
    if let Some(path) = path {
        urls.extend(read_file(path).await?);
    }
    validate_urls(urls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_whitespace_comments_bom_crlf_and_keeps_duplicates_and_fragments() {
        let urls = parse("\u{feff} # list\r\n\n  https://example.com/a#part  \r\n\t# skip\nhttps://example.com/a#part\nhttp://example.com/b?x=1&y=2").unwrap();
        assert_eq!(
            urls,
            [
                "https://example.com/a#part",
                "https://example.com/a#part",
                "http://example.com/b?x=1&y=2"
            ]
        );
    }

    #[test]
    fn rejects_empty_malformed_credentials_and_limits_without_echoing_secrets() {
        for input in [
            "",
            "\n# comment",
            "ftp://example.com/a",
            "file:///etc/passwd",
            "not a URL",
            "https://user:secret@example.com/a",
        ] {
            let error = parse(input).unwrap_err().to_string();
            assert!(!error.contains("secret"));
        }
        assert!(
            parse("https://example.com/a\ninvalid")
                .unwrap_err()
                .to_string()
                .contains("line 2")
        );
        assert!(parse(&"#".repeat(MAX_INPUT_BYTES + 1)).is_err());
        assert!(parse(&"https://example.com/a\n".repeat(MAX_URLS + 1)).is_err());
        assert!(parse(&format!("https://example.com/{}", "a".repeat(8192))).is_err());
    }

    #[test]
    fn accepts_an_exactly_one_mib_list_without_a_trailing_newline() {
        let mut urls = vec![
            format!(
                "https://example.com/{}",
                "a".repeat(8191 - "https://example.com/".len())
            );
            128
        ];
        urls.first_mut().unwrap().push('a');
        let content = urls.join("\n");
        assert_eq!(content.len(), MAX_INPUT_BYTES);
        assert_eq!(validate_urls(parse(&content).unwrap()).unwrap(), urls);
    }

    #[tokio::test]
    async fn file_errors_are_actionable_and_loading_precedes_queue_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("urls.txt");
        assert!(
            read_file(path.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("urls.txt")
        );
        assert!(read_file(dir.path().into()).await.is_err());
        std::fs::write(&path, [0xff]).unwrap();
        assert!(
            read_file(path.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("UTF-8")
        );
        std::fs::write(&path, "https://example.com/b\n").unwrap();
        assert_eq!(
            load(vec!["https://example.com/a".into()], Some(path))
                .await
                .unwrap(),
            ["https://example.com/a", "https://example.com/b"]
        );
    }
}
