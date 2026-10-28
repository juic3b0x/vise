//! Explicit, narrowly used secret serialization boundaries. Never log the output.
use super::*;
use serde_json::{Value, json};

impl RequestOverrides {
    /// Insert unredacted fields into an outbound admission body only.
    pub fn encode_admission(&self, body: &mut Value) -> Result<()> {
        self.resolve(&Self::default())?;
        let object = body
            .as_object_mut()
            .ok_or_else(|| invalid("admission body must be an object"))?;
        if let Some(headers) = &self.headers {
            object.insert("headers".into(), headers_wire(headers));
        }
        if let Some(value) = &self.referer {
            object.insert("referer".into(), json!(value.0));
        }
        if let Some(value) = &self.user_agent {
            object.insert("user_agent".into(), json!(value.0));
        }
        if let Some(cookies) = &self.cookies {
            object.insert("cookies".into(), cookies.iter().map(|c| c.wire()).collect());
        }
        Ok(())
    }
}
fn headers_wire(headers: &[Header]) -> Value {
    headers
        .iter()
        .map(|h| json!({"name":h.name,"value":h.value.0}))
        .collect()
}
impl TaskRequestContext {
    pub(crate) fn journal_json(&self) -> Result<String> {
        serde_json::to_string(&json!({"version":1,"headers":headers_wire(&self.headers),
            "referer":self.referer.as_ref().map(|v|&v.0),"user_agent":self.user_agent.0,
            "custom_user_agent":self.custom_user_agent,"cookies":self.cookies.iter().map(|c|c.wire()).collect::<Vec<_>>()}))
            .map_err(|_|invalid("cannot encode private request credentials"))
    }
    pub(crate) fn from_journal(value: &str) -> Result<Self> {
        // Reject unknown envelope versions and suppress all serde error text:
        // malformed types can otherwise echo credential-bearing input.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Stored {
            version: u8,
            headers: Vec<Header>,
            referer: Option<Secret>,
            user_agent: Secret,
            custom_user_agent: bool,
            cookies: Vec<cookie::SuppliedCookie>,
        }
        if value.len() > ADMISSION_BODY_BYTES * 4 {
            return Err(invalid("private request credential envelope is too large"));
        }
        let stored: Stored = serde_json::from_str(value)
            .map_err(|_| invalid("invalid private request credential envelope"))?;
        if stored.version != 1 {
            return Err(invalid("unsupported private request credential version"));
        }
        let options = RequestOverrides {
            headers: Some(stored.headers),
            referer: stored.referer,
            user_agent: Some(stored.user_agent),
            cookies: Some(stored.cookies),
        };
        let mut context = options
            .resolve(&RequestOverrides::default())?
            .ok_or_else(|| invalid("missing private request credentials"))?;
        context.custom_user_agent = stored.custom_user_agent;
        Ok(context)
    }
}
