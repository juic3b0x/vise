use serde::Serialize;

#[derive(Default, Serialize)]
pub struct Stats {
    pub connections: u64,
    pub peak_open_connections: u64,
    #[serde(skip)]
    pub open_connections: u64,
    pub requests: Vec<RequestLog>,
}

// Only bounded numeric request metadata. Never store raw request/header text.
#[derive(Serialize)]
pub struct RequestLog {
    pub connection: u64,
    pub file: Option<usize>,
    pub start: u64,
    pub end: u64,
    pub status: u16,
    pub head: bool,
    pub slow: bool,
    pub body_bytes: u64,
    pub started_s: f64,
    pub first_byte_s: Option<f64>,
    pub last_byte_s: Option<f64>,
    pub reset: bool,
}
