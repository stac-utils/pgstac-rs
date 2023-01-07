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
        let row = self.query_one("get_version", &[]).await?;
        row.try_get("get_version").map_err(Error::from)
    }

    pub async fn setting(&self, setting: &str) -> Result<String> {
        let row = self.query_one("get_setting", &[&setting]).await?;
        row.try_get("get_setting").map_err(Error::from)
    }

    pub async fn collections(&self) -> Result<Vec<Collection>> {
        let row = self.query_one("all_collections", &[]).await?;
        match row.try_get("all_collections") {
            Ok(collections) => serde_json::from_value(collections).map_err(Error::from),
            Err(err) => {
                let err = err.into_source().unwrap(); // TODO don't unwrap
                if err.downcast::<WasNull>().is_ok() {
                    Ok(Vec::new())
                } else {
                    unimplemented!()
                }
            }
        }
    }

    pub async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let row = self.query_one("get_collection", &[&id]).await?;
        match row.try_get("get_collection") {
            Ok(collection) => serde_json::from_value(collection).map_err(Error::from),
            Err(err) => {
                let err = err.into_source().unwrap(); // TODO don't unwrap
                if err.downcast::<WasNull>().is_ok() {
                    Ok(None)
                } else {
                    unimplemented!()
                }
            }
        }
    }

    pub async fn add_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        let _ = self.query_one("create_collection", &[&collection]).await?;
        Ok(())
    }

    pub async fn update_collection(&self, collection: Collection) -> Result<()> {
        let collection = serde_json::to_value(collection)?;
        let _ = self.query_one("update_collection", &[&collection]).await?;
        Ok(())
    }

    pub async fn delete_collection(&self, id: &str) -> Result<()> {
        let _ = self.query_one("delete_collection", &[&id]).await?;
        Ok(())
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
