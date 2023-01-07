# pgstac-rs

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/gadomski/pgstac-rs/ci.yml?branch=main&style=for-the-badge)](https://github.com/gadomski/pgstac-rs/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/pgstac?style=for-the-badge)](https://docs.rs/pgstac/latest/pgstac/)
[![Crates.io](https://img.shields.io/crates/v/pgstac?style=for-the-badge)](https://crates.io/crates/pgstac)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Rust interface for [pgstac](https://github.com/stac-utils/pgstac).

## Testing

**pgstac-rs** needs a blank **pgstac** database for testing.
The repo comes with a [docker-compose](./docker-compose.yml) to run one.
To start the database:

```shell
docker-compose up
```

Then you can test as normal:

```shell
cargo test
```

Each test is run in its own transaction, which is rolled back after the test.

### Customizing the test database connection

By default, the tests will connect to the database at `postgresql://username:password@localhost:5432/postgis`.
If you need to customize the connection information for whatever reason, set your `PGSTAC_RS_TEST_DB` environment variable:

```shell
PGSTAC_RS_TEST_DB=postgresql://otherusername:otherpassword@otherhost:7822/otherdbname cargo test
```

## License

**pgstac-rs** is dual-licensed under both the MIT license and the Apache license (Version 2.0).
See [LICENSE-APACHE](./LICENSE-APACHE) and [LICENSE-MIT](./LICENSE-MIT) for details.
