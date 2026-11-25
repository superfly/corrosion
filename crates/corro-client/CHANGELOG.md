# Changelog

All notable changes to this crate are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0-alpha.1] - 2026-11-25

### Added

- `CorrosionApiClient::with_auth_token` builds a client that sends `Authorization: Bearer <token>` on every request, and a new `Error::InvalidAuthToken` variant for tokens that aren't valid header values ([#489](https://github.com/superfly/corrosion/pull/489)).

### Changed

- Depend on `corro-api-types` 0.2.0-alpha.1 and `sqlite-pool` 0.1.0-alpha.1, which use `rusqlite` 0.41.1 and pick up the bundled SQLite fix for [RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459) ([#601](https://github.com/superfly/corrosion/pull/601)).
