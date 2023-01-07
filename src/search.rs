use serde::Serialize;

/// Search.
#[derive(Debug, Serialize, Default)]
pub struct Search {
    /// Array of Item ids to return.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}
