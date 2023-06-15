//! Rust interface for [pgstac](https://github.com/stac-utils/pgstac).
//!
//! # Examples
//!
//! The top-level [connect] function is the simplest entrypoint:
//!
//! ```
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (client, connection) = pgstac::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move { connection.await.unwrap() });
//! # })
//! ```
//!
//! If you want to work in a transaction, you can create your own client:
//!
//! ```no_run
//! use stac::Collection;
//! use pgstac::Client;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! # let config = "postgresql://username:password@localhost:5432/postgis";
//! let (mut client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move { connection.await.unwrap() });
//! let client = Client::new(client.transaction().await.unwrap());
//! client.add_collection(Collection::new("an-id", "a description")).await.unwrap();
//! let transaction = client.into_inner();
//! transaction.commit().await.unwrap();
//! # })
//! ```

#![deny(missing_docs)]

mod client;
mod page;

pub use {client::Client, page::Page};

/// Crate-specific error enum.
#[derive(Debug, thiserror::Error)]
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

/// A convenience function which parses a connection string and connects to a **pgstac** database.
///
/// # Examples
///
/// ```
/// use tokio_postgres::NoTls;
///
/// # tokio_test::block_on(async {
/// let config = "postgresql://username:password@localhost:5432/postgis";
/// let (client, connection) = pgstac::connect(config, NoTls).await.unwrap();
/// tokio::spawn(async move { connection.await.unwrap() });
/// # })
/// ```
pub async fn connect<T>(
    config: &str,
    tls: T,
) -> Result<(
    Client<tokio_postgres::Client>,
    tokio_postgres::Connection<tokio_postgres::Socket, T::Stream>,
)>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>,
{
    let (client, connection) = tokio_postgres::connect(config, tls).await?;
    Ok((Client::new(client), connection))
}
