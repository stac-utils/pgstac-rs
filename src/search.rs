use serde::Serialize;

/// Search.
#[derive(Debug, Serialize, Default)]
pub struct Search {
    /// The maximum number of results to return (page size).
    pub limit: Option<usize>,

    /// Requested bounding box.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub bbox: Vec<f64>,

    /// Single date+time, or a range ('/' separator), formatted to [RFC 3339,
    /// section 5.6](https://tools.ietf.org/html/rfc3339#section-5.6).
    ///
    /// Use double dots `..` for open date ranges.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub datetime: String,

    /// Array of Item ids to return.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ids: Vec<String>,
}
