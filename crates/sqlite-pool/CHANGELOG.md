# Changelog

All notable changes to this crate are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0-alpha.1] - 2026-11-25

### Changed

- Bump `rusqlite` from 0.38 to 0.41.1 (bundled `libsqlite3-sys` 0.39), picking up the bundled SQLite fix for [RUSTSEC-2026-0459](https://rustsec.org/advisories/RUSTSEC-2026-0459) ([#601](https://github.com/superfly/corrosion/pull/601)).
  The pool hands out `rusqlite::Connection`s, so downstream crates must also move to `rusqlite` 0.41.
