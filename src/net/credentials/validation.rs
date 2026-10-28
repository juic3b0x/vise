use super::*;

fn text(value: &str, maximum: usize) -> Result<()> {
    if value.len() > maximum || value.bytes().any(|b| b.is_ascii_control()) {
        return Err(invalid(
            "request credential exceeds its limit or contains control characters",
        ));
    }
    HeaderValue::from_str(value).map_err(|_| invalid("invalid request credential encoding"))?;
    Ok(())
}

fn header_name(name: &str) -> Result<String> {
    if name.len() > 128 {
        return Err(invalid("header name exceeds 128 bytes"));
    }
    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| invalid("invalid HTTP token header name"))?;
    let name = name.as_str();
    if matches!(name, "cookie" | "set-cookie") {
        return Err(invalid(
            "raw cookie headers are forbidden; use --cookies FILE or the API's structured cookies",
        ));
    }
    if matches!(
        name,
        "host"
            | "range"
            | "if-range"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "accept-encoding"
            | "content-encoding"
            | "proxy-authorization"
            | "proxy-authenticate"
            | "proxy-connection"
            | "keep-alive"
            | "upgrade"
            | "te"
            | "trailer"
            | "want-repr-digest"
    ) || name.starts_with("if-")
    {
        return Err(invalid(
            "header is controlled by Vise and cannot be overridden",
        ));
    }
    Ok(name.into())
}

impl RequestOverrides {
    pub(super) fn normalized(&self) -> Result<Self> {
        let mut output = self.clone();
        output.normalize_headers()?;
        if let Some(value) = &mut output.referer {
            normalize_referer(value)?;
        }
        if let Some(value) = &output.user_agent {
            text(&value.0, 512)?;
        }
        if let Some(cookies) = &mut output.cookies {
            if cookies.len() > 128 {
                return Err(invalid("at most 128 supplied cookies are allowed"));
            }
            cookies
                .iter_mut()
                .try_for_each(|cookie| cookie.normalize())?;
        }
        Ok(output)
    }

    fn normalize_headers(&mut self) -> Result<()> {
        let Some(headers) = &mut self.headers else {
            return Ok(());
        };
        if headers.len() > 32 {
            return Err(invalid("at most 32 custom headers are allowed"));
        }
        for header in headers.iter_mut() {
            header.name = header_name(&header.name)?;
            text(&header.value.0, 8192)?;
            let field = match header.name.as_str() {
                "user-agent" => Some(&mut self.user_agent),
                "referer" => Some(&mut self.referer),
                _ => None,
            };
            if let Some(field) = field {
                if field.is_some() {
                    return Err(invalid(
                        "duplicate User-Agent or Referer sources; use one form only",
                    ));
                }
                *field = Some(header.value.clone());
            }
        }
        headers.retain(|h| !matches!(h.name.as_str(), "user-agent" | "referer"));
        Ok(())
    }
}

fn normalize_referer(value: &mut Secret) -> Result<()> {
    text(&value.0, 8192)?;
    if value.0.is_empty() {
        return Ok(());
    }
    let mut url =
        Url::parse(&value.0).map_err(|_| invalid("referer must be an absolute HTTP(S) URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid(
            "referer must be HTTP(S) without embedded credentials",
        ));
    }
    url.set_fragment(None);
    value.0 = url.into();
    text(&value.0, 8192)
}

impl TaskRequestContext {
    pub(super) fn validate(&self) -> Result<()> {
        if self.headers.len() > 32 {
            return Err(invalid("at most 32 custom headers are allowed"));
        }
        let headers: usize = self
            .headers
            .iter()
            .map(|h| h.name.len() + h.value.0.len())
            .sum();
        let cookies: usize = self.cookies.iter().map(|c| c.size()).sum();
        let total = headers
            + cookies
            + self.referer.as_ref().map_or(0, |v| v.0.len())
            + self.user_agent.0.len();
        if headers > 32768 || cookies > 32768 || total > 65536 {
            return Err(invalid(
                "combined request credentials exceed their size limit",
            ));
        }
        Ok(())
    }
}
