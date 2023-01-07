use serde::Serialize;

/// Search.
#[derive(Debug, Serialize)]
pub struct Search {}

impl Search {
    /// Creates a new search object.
    pub fn new() -> Search {
        Search {}
    }
}
