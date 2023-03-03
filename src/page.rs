use serde::Deserialize;
use serde_json::{Map, Value};
use stac_api::Context;

/// A page of search results.
#[derive(Debug, Deserialize)]
pub struct Page {
    /// This should always be "FeatureCollection".
    pub r#type: String,

    /// These are the out features, usually STAC items, but maybe not legal STAC
    /// items if fields are excluded.
    pub features: Vec<Map<String, Value>>,

    /// The next id.
    pub next: Option<String>,

    /// The previous id.
    pub prev: Option<String>,

    /// The search context.
    pub context: Context,
}

impl Page {
    /// Returns this page's next token, if it has one.
    pub fn next_token(&self) -> Option<String> {
        self.next.as_ref().map(|next| format!("next:{}", next))
    }

    /// Returns this page's prev token, if it has one.
    pub fn prev_token(&self) -> Option<String> {
        self.prev.as_ref().map(|prev| format!("prev:{}", prev))
    }
}
