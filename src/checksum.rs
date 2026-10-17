//! Expected digests, representation metadata, and bounded-memory file hashing.
use std::{io::Read, path::PathBuf};

use base64::{Engine, engine::general_purpose::STANDARD};
use md5::Md5;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExpectedChecksums {
    pub sha256: Option<String>,
    pub md5: Option<String>,
}
impl ExpectedChecksums {
    pub fn new(sha256: Option<&str>, md5: Option<&str>) -> Result<Self> {
        Ok(Self {
            sha256: sha256.map(|v| normalize(v, 64, "SHA-256")).transpose()?,
            md5: md5.map(|v| normalize(v, 32, "MD5")).transpose()?,
        })
    }
    pub fn is_empty(&self) -> bool {
        self.sha256.is_none() && self.md5.is_none()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Checksums {
    pub expected_sha256: Option<String>,
    pub expected_md5: Option<String>,
    pub actual_sha256: Option<String>,
    pub actual_md5: Option<String>,
    /// Successful verification time, UTC Unix epoch milliseconds.
    pub verified_at: Option<i64>,
}
impl From<ExpectedChecksums> for Checksums {
    fn from(value: ExpectedChecksums) -> Self {
        Self {
            expected_sha256: value.sha256,
            expected_md5: value.md5,
            ..Self::default()
        }
    }
}
impl Checksums {
    pub fn expected(&self) -> ExpectedChecksums {
        ExpectedChecksums {
            sha256: self.expected_sha256.clone(),
            md5: self.expected_md5.clone(),
        }
    }
    pub fn clear_result(&mut self) {
        self.actual_sha256 = None;
        self.actual_md5 = None;
        self.verified_at = None;
    }
    pub fn validate(&self) -> Result<()> {
        ExpectedChecksums::new(
            self.expected_sha256.as_deref(),
            self.expected_md5.as_deref(),
        )?;
        ExpectedChecksums::new(self.actual_sha256.as_deref(), self.actual_md5.as_deref())?;
        if self.verified_at.is_some_and(|time| time < 0)
            || (self.verified_at.is_some()
                && (self.expected().is_empty() || self.mismatch().is_some()))
        {
            return Err(Error::Invalid(
                "invalid checksum verification record".into(),
            ));
        }
        Ok(())
    }
    pub fn mismatch(&self) -> Option<String> {
        let mut errors = Vec::new();
        for (name, expected, actual) in [
            ("SHA-256", &self.expected_sha256, &self.actual_sha256),
            ("MD5", &self.expected_md5, &self.actual_md5),
        ] {
            if let Some(expected) = expected {
                if actual
                    .as_ref()
                    .is_none_or(|actual| !actual.eq_ignore_ascii_case(expected))
                {
                    errors.push(format!(
                        "{name} expected {expected}, actual {}",
                        actual.as_deref().unwrap_or("not computed")
                    ));
                }
            }
        }
        (!errors.is_empty()).then(|| errors.join("; "))
    }
}

fn normalize(value: &str, digits: usize, name: &str) -> Result<String> {
    let value = value.trim();
    if value.len() != digits || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!(
            "expected {name} must contain exactly {digits} hexadecimal digits"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

/// Hash only requested algorithms, in one sequential pass on Tokio's blocking
/// pool. Cancellation is cooperative between 1 MiB reads; the caller joins the
/// hash worker before any other operation can mutate or delete its staging file.
pub async fn hash_file(
    path: PathBuf,
    expected: ExpectedChecksums,
    cancel: CancellationToken,
) -> Result<Checksums> {
    tokio::task::spawn_blocking(move || {
        let mut report = Checksums::from(expected.clone());
        let mut sha256 = expected.sha256.as_ref().map(|_| Sha256::new());
        let mut md5 = expected.md5.as_ref().map(|_| Md5::new());
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let bytes = buffer
                .get(..count)
                .ok_or_else(|| Error::Worker("hash read exceeds buffer capacity".into()))?;
            if let Some(hash) = &mut sha256 {
                hash.update(bytes);
            }
            if let Some(hash) = &mut md5 {
                hash.update(bytes);
            }
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        report.actual_sha256 = sha256.map(|hash| format!("{:x}", hash.finalize()));
        report.actual_md5 = md5.map(|hash| format!("{:x}", hash.finalize()));
        Ok(report)
    })
    .await
    .map_err(|error| Error::Worker(error.to_string()))?
}

/// Repr-Digest always describes the selected representation. Message-content
/// digests are usable only for complete HTTP 200 bodies, never range fragments.
/// Trailer-only digests and the ambiguous obsolete Digest field are not used.
pub fn from_headers(headers: &HeaderMap, full_body: bool) -> Result<ExpectedChecksums> {
    let mut expected = ExpectedChecksums::default();
    for field in ["repr-digest", "content-digest"] {
        if field == "content-digest" && !full_body {
            continue;
        }
        for value in headers.get_all(field) {
            let value = value
                .to_str()
                .map_err(|_| Error::Protocol(format!("non-ASCII {field}")))?;
            for member in value.split(',') {
                let Some((algorithm, value)) = member.trim().split_once('=') else {
                    if matches!(member.trim(), "sha-256" | "md5") {
                        return Err(Error::Protocol(format!("missing digest value in {field}")));
                    }
                    continue;
                };
                let (slot, length) = match algorithm.trim() {
                    "sha-256" => (&mut expected.sha256, 32),
                    "md5" => (&mut expected.md5, 16),
                    _ => continue,
                };
                let value = value.split(';').next().unwrap_or("").trim();
                let encoded = value
                    .strip_prefix(':')
                    .and_then(|v| v.strip_suffix(':'))
                    .ok_or_else(|| {
                        Error::Protocol(format!("invalid {algorithm} byte sequence in {field}"))
                    })?;
                merge_digest(slot, encoded, length, field)?;
            }
        }
    }
    if full_body {
        for value in headers.get_all("content-md5") {
            merge_digest(
                &mut expected.md5,
                value
                    .to_str()
                    .map_err(|_| Error::Protocol("non-ASCII Content-MD5".into()))?
                    .trim(),
                16,
                "Content-MD5",
            )?;
        }
    }
    Ok(expected)
}
pub fn from_response(response: &reqwest::Response) -> Result<ExpectedChecksums> {
    let mut headers = response.headers().clone();
    // Content-Location can bind Repr-Digest to a different resource. A full-body
    // Content-Digest still describes the output in that case.
    if headers.get("content-location").is_some_and(|value| {
        value
            .to_str()
            .ok()
            .and_then(|location| response.url().join(location).ok())
            .is_none_or(|location| location != *response.url())
    }) {
        headers.remove("repr-digest");
    }
    from_headers(&headers, response.status() == reqwest::StatusCode::OK)
}

fn merge_digest(
    slot: &mut Option<String>,
    encoded: &str,
    length: usize,
    field: &str,
) -> Result<()> {
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| Error::Protocol(format!("invalid base64 in {field}")))?;
    if bytes.len() != length {
        return Err(Error::Protocol(format!("invalid digest length in {field}")));
    }
    use std::fmt::Write;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut hex, "{byte:02x}").map_err(|error| Error::Worker(error.to_string()))?;
    }
    if slot.as_ref().is_some_and(|old| old != &hex) {
        return Err(Error::Protocol(format!("conflicting checksums in {field}")));
    }
    *slot = Some(hex);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_and_normalizes_expected_values() {
        assert_eq!(
            ExpectedChecksums::new(Some(&"AB".repeat(32)), None)
                .unwrap()
                .sha256,
            Some("ab".repeat(32))
        );
        for value in ["", "abc", &"x".repeat(64), &"0".repeat(65)] {
            assert!(ExpectedChecksums::new(Some(value), None).is_err());
        }
        assert!(ExpectedChecksums::new(None, Some(&"0".repeat(31))).is_err());
    }
    #[tokio::test]
    async fn known_vectors_single_pass_and_cancellation() {
        for (data, sha, md5) in [
            (
                b"abc".as_slice(),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "900150983cd24fb0d6963f7d28e17f72",
            ),
            (
                b"".as_slice(),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "d41d8cd98f00b204e9800998ecf8427e",
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("payload");
            tokio::fs::write(&path, data).await.unwrap();
            let expected = ExpectedChecksums::new(Some(sha), Some(md5)).unwrap();
            let report = hash_file(path.clone(), expected.clone(), CancellationToken::new())
                .await
                .unwrap();
            assert_eq!(report.mismatch(), None);
            assert_eq!(report.actual_sha256.as_deref(), Some(sha));
            assert_eq!(report.actual_md5.as_deref(), Some(md5));
            let cancel = CancellationToken::new();
            cancel.cancel();
            assert!(matches!(
                hash_file(path, expected, cancel).await,
                Err(Error::Cancelled)
            ));
        }
    }
    #[test]
    fn distinguishes_representation_and_partial_content_digests() {
        let mut headers = HeaderMap::new();
        let value = format!(
            "sha-256=:{}:, md5=:{}:",
            STANDARD.encode([1; 32]),
            STANDARD.encode([2; 16])
        );
        headers.insert("content-digest", value.parse().unwrap());
        headers.insert("content-md5", STANDARD.encode([2; 16]).parse().unwrap());
        assert!(from_headers(&headers, false).unwrap().is_empty());
        let expected = from_headers(&headers, true).unwrap();
        assert_eq!(expected.sha256, Some("01".repeat(32)));
        assert_eq!(expected.md5, Some("02".repeat(16)));
        headers.insert("repr-digest", value.parse().unwrap());
        assert_eq!(from_headers(&headers, false).unwrap(), expected);
        headers.insert("repr-digest", "sha-256=:bad:".parse().unwrap());
        assert!(from_headers(&headers, false).is_err());
        headers.insert("repr-digest", "sha-256".parse().unwrap());
        assert!(from_headers(&headers, false).is_err());
        headers.insert(
            "repr-digest",
            format!(
                "sha-256=:{}:, sha-256=:{}:",
                STANDARD.encode([1; 32]),
                STANDARD.encode([3; 32])
            )
            .parse()
            .unwrap(),
        );
        assert!(from_headers(&headers, false).is_err());
    }
}
