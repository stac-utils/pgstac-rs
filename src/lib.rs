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

pub use {client::Client, fields::Fields, page::Page, search::Search, sort_by::SortBy};
