# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.4] - 2023-07-07

### Changed

- Bump stac-rs version to v0.5
- Bump pgstac version to v0.6.13 ([#2](https://github.com/gadomski/pgstac-rs/pull/2))

## [0.0.3] - 2023-01-08

### Changed

- `Client` now takes a reference to a generic client, instead of owning it

### Removed

- `Client::into_inner`

## [0.0.2] - 2023-01-08

### Changed

- Make `Error`, `Result`, and `Context` publicly visible

## [0.0.1] - 2023-01-07

Initial release

[unreleased]: https://github.com/gadomski/pgstac-rs/compare/v0.0.4...HEAD
[0.0.4]: https://github.com/gadomski/pgstac-rs/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/gadomski/pgstac-rs/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/gadomski/pgstac-rs/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/gadomski/pgstac-rs/tree/v0.0.1
