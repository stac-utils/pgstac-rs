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

mod client;
mod fields;
mod page;
mod search;
mod sort_by;

pub use {
    client::Client,
    fields::Fields,
    page::{Context, Page},
    search::Search,
    sort_by::SortBy,
};

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
