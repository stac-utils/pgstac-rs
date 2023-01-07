//! Rust interface for [pgstac](https://github.com/stac-utils/pgstac)
//!
//! # Examples
//!
//! [Client] provides an interface to query a **pgstac** database. It can be created from anything that implements [tokio_postgres::GenericClient].
//!
//! ```
//! use pgstac::Client;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let (client, connection) = tokio_postgres::connect("postgresql://username:password@localhost:5432/postgis", NoTls).await.unwrap();
//! let client = Client::new(client);
//! # })
//! ```
//!
//! If you want to work in a transaction, you can do that too:
//!
//! ```no_run
//! # use pgstac::Client;
//! # use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let (mut client, connection) = tokio_postgres::connect("postgresql://username:password@localhost:5432/postgis", NoTls).await.unwrap();
//! let client = Client::new(client.transaction().await.unwrap());
//! /// Do stuff.
//! client.into_inner().commit();
//! # })
//! ```

#![deny(missing_docs)]

use serde::de::DeserializeOwned;
use stac::{Collection, Item};
use thiserror::Error;
use tokio_postgres::{
    types::{ToSql, WasNull},
    GenericClient, Row,
};

/// Crate-specific error enum.
#[derive(Debug, Error)]
pub enum Error {
    /// A boxed error.
    ///
    /// Used to capture generic errors from [tokio_postgres::types::FromSql].
    #[error(transparent)]
    Boxed(#[from] Box<dyn std::error::Error + Sync + Send>),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [tokio_postgres::Error]
    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),

    /// An unknown error.
    ///
    /// Used when [tokio_postgres::types::FromSql] doesn't have a source.
    #[error("unknown error")]
    Unknown,
}

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A **pgstac** client.
///
/// Not every **pgstac** function is provided, and some names are changed to
/// match Rust conventions.
#[derive(Debug)]
pub struct Client<C>(C)
where
    C: GenericClient;

impl<C: GenericClient> Client<C> {
    /// Creates a new client.
    pub fn new(client: C) -> Client<C> {
        Client(client)
    }

    /// Returns this client's inner client.
    pub fn into_inner(self) -> C {
        self.0
    }

    /// Returns the **pgstac** version.
    pub async fn version(&self) -> Result<String> {
        self.string("get_version", &[]).await
    }

    /// Returns the value of a **pgstac** setting.
    pub async fn setting(&self, setting: &str) -> Result<String> {
        self.string("get_setting", &[&setting]).await
    }

    /// Fetches all collections.
    pub async fn collections(&self) -> Result<Vec<Collection>> {
        self.vec("all_collections", &[]).await
    }

    /// Fetches a collection by id.
    pub async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        self.opt("get_collection", &[&id]).await
    }

    /// Adds a collection.
    pub async fn add_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("create_collection", &[&collection]).await
    }

    /// Adds or updates a collection.
    pub async fn upsert_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("upsert_collection", &[&collection]).await
    }

    /// Updates a collection.
    pub async fn update_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("update_collection", &[&collection]).await
    }

    /// Deletes a collection.
    pub async fn delete_collection(&self, id: &str) -> Result<()> {
        self.void("delete_collection", &[&id]).await
    }

    /// Fetches an item.
    pub async fn item(&self, id: &str, collection: &str) -> Result<Option<Item>> {
        self.opt("get_item", &[&id, &collection]).await
    }

    /// Adds an item.
    pub async fn add_item(&self, item: Item) -> Result<()> {
        let item = serde_json::to_value(item)?;
        self.void("create_item", &[&item]).await
    }

    /// Updates an item.
    pub async fn update_item(&self, item: Item) -> Result<()> {
        let item = serde_json::to_value(item)?;
        self.void("update_item", &[&item]).await
    }

    async fn query_one<'a>(
        &'a self,
        function: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Row, tokio_postgres::Error> {
        let param_string = (0..params.len())
            .map(|i| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("SELECT * from pgstac.{}({})", function, param_string);
        self.0.query_one(&query, params).await
    }

    async fn string(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<String> {
        let row = self.query_one(function, params).await?;
        row.try_get(function).map_err(Error::from)
    }

    async fn vec<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        if let Some(value) = self.opt(function, params).await? {
            Ok(value)
        } else {
            Ok(Vec::new())
        }
    }

    async fn opt<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let row = self.query_one(function, params).await?;
        match row.try_get(function) {
            Ok(value) => serde_json::from_value(value).map_err(Error::from),
            Err(err) => {
                if let Some(err) = err.into_source() {
                    if err.downcast_ref::<WasNull>().is_some() {
                        Ok(None)
                    } else {
                        Err(Error::from(err))
                    }
                } else {
                    Err(Error::Unknown)
                }
            }
        }
    }

    async fn void(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<()> {
        let _ = self.query_one(function, params).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use async_once::AsyncOnce;
    use bb8::Pool;
    use bb8_postgres::PostgresConnectionManager;
    use geojson::{Geometry, Value};
    use lazy_static::lazy_static;
    use pgstac_test::pgstac_test;
    use stac::{Collection, Item};
    use tokio_postgres::{NoTls, Transaction};

    lazy_static! {
        static ref POOL: AsyncOnce<Pool<PostgresConnectionManager<NoTls>>> =
            AsyncOnce::new(async {
                let config = std::env::var("PGSTAC_RS_TEST_DB")
                    .unwrap_or("postgresql://username:password@localhost:5432/postgis".to_string());
                let _ = tokio_postgres::connect(&config, NoTls).await.unwrap();
                let manager =
                    PostgresConnectionManager::new_from_stringlike(config, NoTls).unwrap();
                Pool::builder().build(manager).await.unwrap()
            });
    }

    fn longmont() -> Geometry {
        Geometry::new(Value::Point(vec![40.1672, -105.1019]))
    }

    #[pgstac_test]
    async fn version(client: Client<Transaction<'_>>) {
        let _ = client.version().await.unwrap();
    }

    #[pgstac_test]
    async fn setting(client: Client<Transaction<'_>>) {
        assert_eq!(client.setting("context").await.unwrap(), "off");
    }

    #[pgstac_test]
    async fn collections(client: Client<Transaction<'_>>) {
        assert!(client.collections().await.unwrap().is_empty());
        client
            .add_collection(Collection::new("an-id", "a description"))
            .await
            .unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
    }

    #[pgstac_test]
    async fn add_collection_duplicate(client: Client<Transaction<'_>>) {
        assert!(client.collections().await.unwrap().is_empty());
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.add_collection(collection).await.is_err());
    }

    #[pgstac_test]
    async fn upsert_collection(client: Client<Transaction<'_>>) {
        assert!(client.collections().await.unwrap().is_empty());
        let mut collection = Collection::new("an-id", "a description");
        client.upsert_collection(collection.clone()).await.unwrap();
        collection.title = Some("a title".to_string());
        client.upsert_collection(collection).await.unwrap();
        assert_eq!(
            client
                .collection("an-id")
                .await
                .unwrap()
                .unwrap()
                .title
                .unwrap(),
            "a title"
        );
    }

    #[pgstac_test]
    async fn update_collection(client: Client<Transaction<'_>>) {
        let mut collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client
            .collection("an-id")
            .await
            .unwrap()
            .unwrap()
            .title
            .is_none());
        collection.title = Some("a title".to_string());
        client.update_collection(collection).await.unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
        assert_eq!(
            client
                .collection("an-id")
                .await
                .unwrap()
                .unwrap()
                .title
                .unwrap(),
            "a title"
        );
    }

    #[pgstac_test]
    async fn update_collection_does_not_exit(client: Client<Transaction<'_>>) {
        let collection = Collection::new("an-id", "a description");
        assert!(client.update_collection(collection).await.is_err());
    }

    #[pgstac_test]
    async fn collection_not_found(client: Client<Transaction<'_>>) {
        assert!(client.collection("not-an-id").await.unwrap().is_none());
    }

    #[pgstac_test]
    async fn delete_collection(client: Client<Transaction<'_>>) {
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_some());
        client.delete_collection("an-id").await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_none());
    }

    #[pgstac_test]
    async fn delete_collection_does_not_exist(client: Client<Transaction<'_>>) {
        assert!(client.delete_collection("not-an-id").await.is_err());
    }

    #[pgstac_test]
    async fn item(client: Client<Transaction<'_>>) {
        assert!(client
            .item("an-id", "collection-id")
            .await
            .unwrap()
            .is_none());
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        assert_eq!(
            client
                .item("an-id", "collection-id")
                .await
                .unwrap()
                .unwrap(),
            item
        );
    }

    #[pgstac_test]
    async fn item_without_collection(client: Client<Transaction<'_>>) {
        let item = Item::new("an-id");
        assert!(client.add_item(item.clone()).await.is_err());
    }

    #[pgstac_test]
    async fn update_item(client: Client<Transaction<'_>>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.properties
            .additional_fields
            .insert("foo".into(), "bar".into());
        client.update_item(item).await.unwrap();
        assert_eq!(
            client
                .item("an-id", "collection-id")
                .await
                .unwrap()
                .unwrap()
                .properties
                .additional_fields["foo"],
            "bar"
        );
    }
}
