# Tablr

A desktop GUI application for viewing Parquet files, built with Rust using egui and Polars.

## Features

- [x] **Multi-file Support**: Load single or multiple partitioned Parquet files
- [x] **Infinite Scrolling**: Efficiently handle large datasets thanks to Polars `LazyFrame`
- [x] **Native Performance**: Built with Rust for fast data processing and rendering
- [x] **Cross-Platform**: Runs on Windows, macOS, and Linux
- [ ] **Sorting**: TODO
- [ ] **Filtering**: TODO

## Installation

This is still a work in progress, no pre-built binaries are available yet. You can build the application from source.

```bash
cargo build --release
```

## FAQ

### Do you plan to support other file formats?

No, Tablr is focused on Parquet files only. The rationale is that other formats like CSV or JSON can be easily read with
a text editor.
