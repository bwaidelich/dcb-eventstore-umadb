# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-07-21

### Changed

- Event metadata is now stored in UmaDB's native `metadata` field (added in UmaDB v0.6.6 and
  exposed via the updated `ext-umadb` extension) instead of being packed into a JSON blob in
  `data`. The payload is stored raw in `data`, and the recording timestamp is kept under a
  reserved `umadb:recordedAt` metadata key.
- Removed the `subscribe` parameter from the `UmaDB\Client::read()` stub; as of UmaDB v0.6.0
  live streaming is served by a dedicated `Subscribe` RPC.

### BREAKING

- The on-disk storage format changed. Event stores written with earlier versions of this
  adapter (JSON blob in `data`) are not backwards-compatible and require a fresh store or a
  data migration.
- Requires the `ext-umadb` extension built against UmaDB **v0.6.6** or newer.

## [1.0.1]

- Maintenance release.

## [1.0.0]

- Initial release.
