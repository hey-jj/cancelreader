# Changelog

## [0.2.0] - 2026-07-07

### Changed
- Zero-length reads on active Unix readers now return `Ok(0)` at once instead of waiting for input or cancellation. (#19)
- Select backends now fall back before reading when the cancel pipe descriptor is too large for `select`, so ready input can still read through the fallback path. (#18)

### Documentation
- `new_reader` now describes readers that expose raw input handles. (#20)

## [0.2.0] - 2026-07-07

### Changed
- Zero-length reads on active Unix readers now return `Ok(0)` at once instead of waiting for input or cancellation. (#19)
- Select backends now fall back before reading when the cancel pipe descriptor is too large for `select`, so ready input can still read through the fallback path. (#18)

### Documentation
- `new_reader` now describes readers that expose raw input handles. (#20)
