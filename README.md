# pgstac-rs

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/pgstac-rs/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/pgstac-rs/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/pgstac?style=for-the-badge)](https://docs.rs/pgstac/latest/pgstac/)
[![Crates.io](https://img.shields.io/crates/v/pgstac?style=for-the-badge)](https://crates.io/crates/pgstac)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

**This code in this repository has been moved to <https://github.com/stac-utils/stac-rs> (<https://github.com/stac-utils/stac-rs/pull/246>)***

Rust interface for [pgstac](https://github.com/stac-utils/pgstac).

## Usage

In your `Cargo.toml`:

```toml
[dependencies]
pgstac = "0.0.6"
```

See the [documentation](https://docs.rs/pgstac) for more.

## Testing

**pgstac-rs** needs a blank **pgstac** database for testing.
The repo comes with a [docker-compose](./docker-compose.yml) to run one.
To test:

```shell
docker-compose up -d
cargo test
docker-compose down
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
