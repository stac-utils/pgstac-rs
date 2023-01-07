use serde::Serialize;

/// Search.
#[derive(Debug, Serialize, Default)]
pub struct Search {
    /// The maximum number of results to return (page size).
    pub limit: Option<usize>,

    /// Array of Item ids to return.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}
