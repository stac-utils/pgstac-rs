use serde::Serialize;

/// Sort by.
#[derive(Clone, Debug, Serialize, Default)]
pub struct SortBy {
    /// The field to sort by.
    pub field: String,

    /// The direction to sort by.
    pub direction: String,
}
