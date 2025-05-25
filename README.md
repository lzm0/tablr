# Tablr

A desktop GUI application for viewing Parquet files, built with Rust using egui and Polars.
<img width="1312" alt="image" src="https://github.com/user-attachments/assets/0b63d6aa-81a7-49be-a98c-c19989a862ae" />


## Features

- [x] **Multi-file Support**: Load single or multiple partitioned Parquet files
- [x] **Infinite Scrolling**: Efficiently handle large datasets thanks to Polars `LazyFrame`
- [x] **Native Performance**: Built with Rust for fast data processing and rendering
- [x] **Cross-Platform**: Runs on Windows, macOS, and Linux
- [ ] **Sorting**: TODO
- [ ] **Searching**: TODO
- [ ] **Filtering**: TODO

## Installation

This is still a work in progress, so no pre-built binaries are available yet. You can build the application from source.

```bash
cargo build --release
```

## FAQ

### Do you plan to support other file formats?

No, Tablr is focused on Parquet files only. The rationale is that other formats like CSV or JSON can be easily read with
a text editor.
