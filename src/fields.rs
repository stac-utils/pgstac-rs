use serde::Serialize;

/// Fields to include or exclude.
///
/// Should probably be in a stac-api crate.
#[derive(Clone, Default, Debug, Serialize)]
pub struct Fields {
    /// Fields to include.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    /// Fields to exclude.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}
