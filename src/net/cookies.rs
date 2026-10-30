//! Netscape cookie-file import. Values are never included in diagnostics.
use crate::error::{Error, Result};
use reqwest::{
    Url,
    cookie::{CookieStore, Jar},
};
use std::{
    fs::File,
    io::Read,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_LINE_BYTES: usize = 64 * 1024;

pub(crate) struct LoadedCookies {
    /// Canonical, UTF-8 path suitable for the SQLite journal.
    pub(crate) path: PathBuf,
    pub(super) jar: Arc<Jar>,
}

/// File I/O and parsing run on the blocking pool, never on a transfer worker.
pub(crate) async fn load(path: PathBuf) -> Result<LoadedCookies> {
    tokio::task::spawn_blocking(move || load_file(path))
        .await
        .map_err(|_| Error::Worker("cookie loader task failed".into()))?
}

fn load_file(path: PathBuf) -> Result<LoadedCookies> {
    let read_error = |error| {
        Error::Invalid(format!(
            "cannot read cookie file {}: {error}",
            path.display()
        ))
    };
    let canonical = std::fs::canonicalize(&path).map_err(read_error)?;
    if canonical.to_str().is_none() {
        return Err(Error::Invalid(
            "cookie file path must be valid UTF-8 for journal persistence".into(),
        ));
    }
    let metadata = std::fs::metadata(&canonical).map_err(read_error)?;
    if !metadata.is_file() || metadata.len() > MAX_FILE_BYTES {
        return Err(Error::Invalid(format!(
            "cookie file {} must be a regular file of at most 16 MiB",
            path.display()
        )));
    }
    let mut bytes = Vec::new();
    File::open(&canonical)
        .map_err(read_error)?
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(read_error)?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return Err(Error::Invalid("cookie file exceeds 16 MiB".into()));
    }
    let contents = std::str::from_utf8(&bytes)
        .map_err(|_| Error::Invalid(format!("cookie file {} is not UTF-8 text", path.display())))?;
    let jar = parse(contents)
        .map_err(|error| Error::Invalid(format!("cookie file {}: {error}", path.display())))?;
    Ok(LoadedCookies {
        path: canonical,
        jar: Arc::new(jar),
    })
}

/// Import tab-separated records. Expired cookies are ignored; expiration 0
/// denotes a session cookie. #HttpOnly_ lines are records, not comments.
fn parse(contents: &str) -> Result<Jar> {
    let jar = Jar::default();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::Invalid("system clock is before the Unix epoch".into()))?
        .as_secs();
    for (index, line) in contents.trim_start_matches('\u{feff}').lines().enumerate() {
        let error = |reason| Error::Invalid(format!("line {}: {reason}", index + 1));
        let (line, http_only) = match line.strip_prefix("#HttpOnly_") {
            Some(record) => (record, true),
            None if line.trim().is_empty() || line.starts_with('#') => continue,
            None => (line, false),
        };
        if line.len() > MAX_LINE_BYTES {
            return Err(error("cookie record exceeds 64 KiB"));
        }
        let cookie = NetscapeCookie::from_line(line, http_only).map_err(error)?;
        if cookie.expires != 0 && cookie.expires <= now {
            continue;
        }
        cookie.insert(&jar).map_err(error)?;
    }
    Ok(jar)
}

pub(super) struct NetscapeCookie<'a> {
    pub(super) domain: &'a str,
    pub(super) subdomains: bool,
    pub(super) path: &'a str,
    pub(super) secure: bool,
    pub(super) expires: u64,
    pub(super) name: &'a str,
    pub(super) value: &'a str,
    pub(super) http_only: bool,
}

impl<'a> NetscapeCookie<'a> {
    fn from_line(line: &'a str, http_only: bool) -> std::result::Result<Self, &'static str> {
        let fields: Vec<_> = line.split('\t').collect();
        let [domain, subdomains, path, secure, expires, name, value] = fields.as_slice() else {
            return Err("expected seven tab-separated Netscape fields");
        };
        let cookie = Self {
            domain: domain.strip_prefix('.').unwrap_or(domain),
            subdomains: parse_flag(subdomains)?,
            path,
            secure: parse_flag(secure)?,
            expires: expires
                .parse()
                .map_err(|_| "expiration must be Unix seconds (0 for a session cookie)")?,
            name,
            value,
            http_only,
        };
        cookie.validate()?;
        Ok(cookie)
    }

    pub(super) fn validate(&self) -> std::result::Result<(), &'static str> {
        validate_domain(self.domain)?;
        if !self.path.starts_with('/')
            || !self.path.bytes().all(|b| b.is_ascii_graphic() && b != b';')
        {
            return Err(
                "cookie path must start with '/' and contain no whitespace, control characters, or semicolons",
            );
        }
        if self.name.is_empty()
            || !self
                .name
                .bytes()
                .all(|b| b.is_ascii_graphic() && !b"()<>@,;:\\\"/[]?={}".contains(&b))
        {
            return Err("invalid cookie name");
        }
        let value = self
            .value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(self.value);
        if !value
            .bytes()
            .all(|b| matches!(b, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e))
        {
            return Err("invalid cookie value (HTTP cookie octets required)");
        }
        if self.expires > 253_402_300_799 {
            return Err("cookie expiration is out of range");
        }
        if self.name.starts_with("__Secure-") && !self.secure {
            return Err("__Secure- cookies require the secure flag");
        }
        if self.name.starts_with("__Host-") && (!self.secure || self.subdomains || self.path != "/")
        {
            return Err("__Host- cookies require secure, host-only scope and path '/'");
        }
        Ok(())
    }

    pub(super) fn insert(&self, jar: &Jar) -> std::result::Result<(), &'static str> {
        let scheme = if self.secure { "https" } else { "http" };
        let mut url = Url::parse(&format!("{scheme}://{}/", self.domain))
            .map_err(|_| "invalid cookie domain")?;
        url.set_path(self.path);
        let mut header = format!("{}={}; Path={}", self.name, self.value, self.path);
        if self.subdomains {
            header.push_str("; Domain=");
            header.push_str(self.domain);
        }
        if self.secure {
            header.push_str("; Secure");
        }
        if self.http_only {
            header.push_str("; HttpOnly");
        }
        let validation_header = header.clone();
        if self.expires != 0 {
            let expires = i64::try_from(self.expires)
                .ok()
                .and_then(|seconds| chrono::DateTime::from_timestamp(seconds, 0))
                .ok_or("cookie expiration is out of range")?;
            use std::fmt::Write;
            write!(
                header,
                "; Expires={}",
                expires.format("%a, %d %b %Y %H:%M:%S GMT")
            )
            .map_err(|_| "cannot format cookie expiration")?;
        }
        let validation = Jar::default();
        validation.add_cookie_str(&validation_header, &url);
        // Jar's import method silently rejects invalid cookies. Surface rejection
        // instead of quietly falling back to an unauthenticated download.
        let accepted = validation
            .cookies(&url)
            .and_then(|h| h.to_str().ok().map(str::to_owned))
            .is_some_and(|header| {
                header
                    .split("; ")
                    .any(|pair| pair.split_once('=') == Some((self.name, self.value)))
            });
        if !accepted {
            return Err("cookie was rejected by the HTTP cookie store");
        }
        jar.add_cookie_str(&header, &url);
        Ok(())
    }
}

fn parse_flag(value: &str) -> std::result::Result<bool, &'static str> {
    match value {
        "TRUE" => Ok(true),
        "FALSE" => Ok(false),
        _ => Err("cookie flags must be TRUE or FALSE"),
    }
}

fn validate_domain(domain: &str) -> std::result::Result<(), &'static str> {
    if domain.is_empty()
        || !domain.is_ascii()
        || domain
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b"/\\@?#%".contains(&b))
    {
        return Err("invalid cookie domain");
    }
    let host = url::Host::parse(domain).map_err(|_| "invalid cookie domain")?;
    if let url::Host::Domain(name) = host
        && (name.len() > 253
            || name.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }))
    {
        return Err("invalid cookie domain");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(jar: &Jar, url: &str) -> String {
        jar.cookies(&Url::parse(url).unwrap())
            .map(|h| h.to_str().unwrap().to_owned())
            .unwrap_or_default()
    }

    #[test]
    fn imports_comments_crlf_session_expiry_and_http_only_records() {
        let jar = parse("\u{feff}# Netscape HTTP Cookie File\r\n\r\n#HttpOnly_.example.com\tTRUE\t/private\tTRUE\t0\tsession\tsecret\r\nexample.com\tFALSE\t/\tFALSE\t4102444800\tfuture\tvalue\nexample.com\tFALSE\t/\tFALSE\t1\texpired\told\nexample.com\tFALSE\t/\tFALSE\t0\tempty\t\n").unwrap();
        let root = header(&jar, "https://example.com/");
        assert!(root.contains("future=value") && root.contains("empty="));
        assert!(!root.contains("expired") && !root.contains("session"));
        assert_eq!(
            header(&jar, "https://sub.example.com/private/file"),
            "session=secret"
        );
        assert!(header(&jar, "http://sub.example.com/private/file").is_empty());
        assert!(header(&jar, "https://example.com/private-other").contains("future=value"));
        assert!(!header(&jar, "https://example.com/private-other").contains("session"));
        assert!(header(&jar, "https://example.com.evil.test/private").is_empty());
    }

    #[test]
    fn host_only_scope_prefixes_and_replacements_are_preserved() {
        let jar = parse(".example.com\tFALSE\t/\tTRUE\t0\t__Host-session\tfirst\nexample.com\tFALSE\t/\tTRUE\t0\t__Host-session\tsecond\n").unwrap();
        assert_eq!(
            header(&jar, "https://example.com/"),
            "__Host-session=second"
        );
        assert!(header(&jar, "https://sub.example.com/").is_empty());
        assert!(header(&jar, "http://example.com/").is_empty());
    }

    #[test]
    fn malformed_records_report_line_and_reason_without_credentials() {
        for record in [
            "not a Netscape record secret",
            "example.com\tMAYBE\t/\tFALSE\t0\tsession\tsecret",
            "example.com\tFALSE\t/\tTRUE\tsecret\tsession\tvalue",
            "example.com:8080\tFALSE\t/\tFALSE\t0\tsession\tsecret",
            "example.com/path\tFALSE\t/\tFALSE\t0\tsession\tsecret",
            "example.com\tFALSE\trelative\tFALSE\t0\tsession\tsecret",
            "example.com\tFALSE\t/; Secure\tFALSE\t0\tsession\tsecret",
            "example.com\tFALSE\t/\tFALSE\t0\tbad name\tsecret",
            "example.com\tFALSE\t/\tFALSE\t0\tsession\tsecret; injected=value",
            "example.com\tFALSE\t/\tFALSE\t0\tsession\tsecret\textra",
            "example.com\tFALSE\t/\tFALSE\t0\t__Secure-session\tsecret",
            "example.com\tTRUE\t/\tTRUE\t0\t__Host-session\tsecret",
            "example.com\tFALSE\t/\tTRUE\t253402300800\tsession\tsecret",
        ] {
            let error = parse(&format!("# header\n{record}\n"))
                .unwrap_err()
                .to_string();
            assert!(error.contains("line 2"), "{error}");
            assert!(!error.contains("secret"), "{error}");
        }
    }

    #[tokio::test]
    async fn file_errors_are_actionable_and_success_returns_an_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.txt");
        let error = load(missing).await.err().unwrap().to_string();
        assert!(error.contains("cannot read cookie file") && error.contains("missing.txt"));
        assert!(load(dir.path().into()).await.is_err());
        let path = dir.path().join("cookies.txt");
        std::fs::write(&path, "# header\nsecret malformed\n").unwrap();
        let error = load(path.clone()).await.err().unwrap().to_string();
        assert!(error.contains("line 2") && !error.contains("secret"));
        std::fs::write(&path, "example.com\tFALSE\t/\tFALSE\t0\tsession\tvalue\n").unwrap();
        let loaded = load(path.clone()).await.unwrap();
        assert_eq!(loaded.path, path.canonicalize().unwrap());
        assert_eq!(header(&loaded.jar, "http://example.com/"), "session=value");
    }
}
