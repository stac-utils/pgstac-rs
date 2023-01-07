use serde::Deserialize;
use stac::{Item, ItemCollection};

/// A page of search results.
#[derive(Debug, Deserialize)]
pub struct Page {
    /// The STAC item collection.
    #[serde(flatten)]
    pub item_collection: ItemCollection,

    /// The next id.
    pub next: Option<String>,

    /// The previous id.
    pub prev: Option<String>,

    /// The search context.
    pub context: Context,
}

/// A page of search results.
#[derive(Debug, Deserialize)]
pub struct Context {
    /// The limit.
    pub limit: usize,

    /// The number returned.
    pub returned: usize,
}

impl Page {
    /// Returns this page's items.
    pub fn items(&self) -> &[Item] {
        &self.item_collection.items
    }

    /// Returns this page's next token, if it has one.
    pub fn next_token(&self) -> Option<String> {
        self.next.as_ref().map(|next| format!("next:{}", next))
    }

    /// Returns this page's prev token, if it has one.
    pub fn prev_token(&self) -> Option<String> {
        self.prev.as_ref().map(|prev| format!("prev:{}", prev))
    }
}
