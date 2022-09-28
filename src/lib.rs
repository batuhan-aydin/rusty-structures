pub mod bloom_filter;
pub mod priority_queue;

pub type RustyResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub struct RustyError {
    reason: String
}

impl RustyError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

impl std::error::Error for RustyError {}

impl std::fmt::Display for RustyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Reason: {}", self.reason)
    }
}