use serde::de::DeserializeOwned;
use stac::Collection;
use thiserror::Error;
use tokio_postgres::{
    types::{ToSql, WasNull},
    GenericClient, Row,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Client<C>(C)
where
    C: GenericClient;

impl<C: GenericClient> Client<C> {
    pub fn new(client: C) -> Client<C> {
        Client(client)
    }

    pub async fn version(&self) -> Result<String> {
        self.string("get_version", &[]).await
    }

    pub async fn setting(&self, setting: &str) -> Result<String> {
        self.string("get_setting", &[&setting]).await
    }

    pub async fn collections(&self) -> Result<Vec<Collection>> {
        self.vec("all_collections", &[]).await
    }

    pub async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        self.opt("get_collection", &[&id]).await
    }

    pub async fn add_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("create_collection", &[&collection]).await
    }

    pub async fn upsert_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("upsert_collection", &[&collection]).await
    }

    pub async fn update_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        self.void("update_collection", &[&collection]).await
    }

    pub async fn delete_collection(&self, id: &str) -> Result<()> {
        self.void("delete_collection", &[&id]).await
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
        let row = self.query_one(function, params).await?;
        match row.try_get(function) {
            Ok(value) => serde_json::from_value(value).map_err(Error::from),
            Err(err) => {
                let err = err.into_source().unwrap(); // TODO don't unwrap
                if err.downcast::<WasNull>().is_ok() {
                    Ok(Vec::new())
                } else {
                    unimplemented!() // TODO implement
                }
            }
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
                let err = err.into_source().unwrap(); // TODO don't unwrap
                if err.downcast::<WasNull>().is_ok() {
                    Ok(None)
                } else {
                    unimplemented!() // TODO implement
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
    use lazy_static::lazy_static;
    use pgstac_test::pgstac_test;
    use stac::Collection;
    use tokio_postgres::{NoTls, Transaction};

    lazy_static! {
        static ref POOL: AsyncOnce<Pool<PostgresConnectionManager<NoTls>>> =
            AsyncOnce::new(async {
                let config = "postgresql://username:password@localhost:5432/postgis";
                let _ = tokio_postgres::connect(config, NoTls).await.unwrap();
                let manager =
                    PostgresConnectionManager::new_from_stringlike(config, NoTls).unwrap();
                Pool::builder().build(manager).await.unwrap()
            });
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
}
