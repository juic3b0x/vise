pub struct Request {
    pub file: usize,
    pub start: u64,
    pub end: u64,
    pub status: u16,
    pub head: bool,
    pub close: bool,
}

pub fn etag(seed: u64, file: usize, size: u64) -> String {
    format!("\"bench-{seed}-{file}-{size}\"")
}

pub fn parse(bytes: &[u8], seed: u64, sizes: &[u64]) -> Result<Request, u16> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Request::new(&mut headers);
    let length = match parsed.parse(bytes).map_err(|_| 400u16)? {
        httparse::Status::Complete(length) => length,
        _ => return Err(400),
    };
    // No pipelining, request bodies or credential-bearing headers in this fixture.
    if length != bytes.len() {
        return Err(400);
    }
    let head = match parsed.method {
        Some("HEAD") => true,
        Some("GET") => false,
        _ => return Err(405),
    };
    let file: usize = parsed
        .path
        .and_then(|p| p.strip_prefix("/file/"))
        .and_then(|p| p.strip_suffix(".bin"))
        .filter(|p| !p.is_empty() && p.len() <= 5 && p.bytes().all(|c| c.is_ascii_digit()))
        .and_then(|p| p.parse().ok())
        .ok_or(404u16)?;
    let size = *sizes.get(file).ok_or(404u16)?;
    let mut range = None;
    let mut if_range = None;
    let mut close = parsed.version != Some(1);
    for header in parsed.headers {
        let name = header.name;
        if [
            "authorization",
            "proxy-authorization",
            "cookie",
            "transfer-encoding",
        ]
        .iter()
        .any(|forbidden| name.eq_ignore_ascii_case(forbidden))
        {
            return Err(400);
        }
        if name.eq_ignore_ascii_case("content-length") && header.value != b"0" {
            return Err(400);
        }
        if name.eq_ignore_ascii_case("range") {
            if range.is_some() {
                return Err(400);
            }
            range = Some(std::str::from_utf8(header.value).map_err(|_| 400u16)?);
        }
        if name.eq_ignore_ascii_case("if-range") {
            if_range = Some(header.value);
        }
        if name.eq_ignore_ascii_case("connection") && header.value.eq_ignore_ascii_case(b"close") {
            close = true;
        }
    }
    let expected = etag(seed, file, size);
    let use_range = if_range.is_none_or(|value| value == expected.as_bytes());
    let (start, end, status) = if let Some(range) = range.filter(|_| use_range) {
        let value = range.strip_prefix("bytes=").ok_or(416u16)?;
        let (start, end) = value.split_once('-').ok_or(416u16)?;
        let (start, end) = if start.is_empty() {
            let suffix = number(end)?;
            if suffix == 0 {
                return Err(416);
            }
            (size.saturating_sub(suffix), size - 1)
        } else {
            let start = number(start)?;
            let end = if end.is_empty() {
                size - 1
            } else {
                number(end)?.min(size - 1)
            };
            (start, end)
        };
        if start >= size || start > end {
            return Err(416);
        }
        (start, end, 206)
    } else {
        (0, size - 1, 200)
    };
    Ok(Request {
        file,
        start,
        end,
        status,
        head,
        close,
    })
}

fn number(value: &str) -> Result<u64, u16> {
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|c| c.is_ascii_digit()) {
        return Err(416);
    }
    value.parse().map_err(|_| 416)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_if_range_and_request_bounds() {
        let request = parse(
            b"GET /file/0.bin HTTP/1.1\r\nRange: bytes=5-9\r\nIf-Range: \"bench-42-0-100\"\r\n\r\n",
            42,
            &[100],
        )
        .unwrap();
        assert_eq!((request.start, request.end, request.status), (5, 9, 206));
        let request = parse(
            b"GET /file/0.bin HTTP/1.1\r\nRange: bytes=5-\r\nIf-Range: \"old\"\r\n\r\n",
            42,
            &[100],
        )
        .unwrap();
        assert_eq!((request.start, request.end, request.status), (0, 99, 200));
        assert!(
            parse(
                b"GET /file/0.bin HTTP/1.1\r\nRange: bytes=100-\r\n\r\n",
                42,
                &[100]
            )
            .is_err()
        );
        assert!(parse(b"GET /file/../secret HTTP/1.1\r\n\r\n", 42, &[100]).is_err());
        assert!(
            parse(
                b"GET /file/0.bin HTTP/1.1\r\nAuthorization: secret\r\n\r\n",
                42,
                &[100]
            )
            .is_err()
        );
    }
}
