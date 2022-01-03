[![License](https://img.shields.io/crates/l/csv-partial-cache.svg)](https://choosealicense.com/licenses/mit/)
[![Crates.io](https://img.shields.io/crates/v/csv-partial-cache.svg)](https://crates.io/crates/csv-partial-cache)
[![Docs.rs](https://docs.rs/csv-partial-cache/badge.svg)](https://docs.rs/csv-partial-cache)

# csv-partial-cache

Csv index with partially cached columns

It meant to allow a performant access to immutable csv data without importing it to database.
The idea is to keep line offsets with frequently used columns in memory, while accessing the
full line following the offset.


Usage
-----

Let's say we have a table of http statuses:

| code | name                | description                                           |
|------|---------------------|-------------------------------------------------------|
| 100  | Continue            | Status code 100 Continue tells you that a part ...    |
| 101  | Switching Protocols | There have been many HTTP protocols created since ... |

And we'd like to cache short `code` and `name` columns.

```rust
use csv_partial_cache::{self, CsvPartialCache, FromLineOffset};

/// Full table row representation
#[derive(serde::Deserialize)]
struct FullRecord {
    code: u16,
    name: String,
    description: String,
}

/// Columns `code` and `name` are essential and small, so we'd like to keep them and memory
struct PartialRecord {
    code: u16,
    name: String,
    // The file offset to access the full record data
    offset: u16,
}

// We should tell a little more about the cached representation
impl FromLineOffset for PartialRecord {
    /// type of file offsets used instead of default `u64` to spare some memory
    type Offset = u16;

    /// Pointer to the offset field
    fn offset(&self) -> Self::Offset {
        self.offset
    }

    /// Constructor from the line and it's offset
    fn from_line_offset(line: &str, offset: Self::Offset) -> csv_partial_cache::Result<Self> {
        let (code, name) = csv_line::from_str(line)?;
        Ok(Self { code, name, offset })
    }
}

tokio_test::block_on(async {
    // Now we can load the index
    let cache = CsvPartialCache::<PartialRecord>::new("tests/status_codes.csv").unwrap();

    // Getting a partial data cached in memory
    let partial = cache.find(&100, |x| x.code).unwrap();
    assert_eq!(partial.name, "Continue");

    /// Loading full data from the csv file
    let full: FullRecord = cache.full_record(&partial).await.unwrap();
    assert_eq!(full.code, partial.code);
})
```

## Contributing

We appreciate all kinds of contributions, thank you!

### Note on README

The `README.md` file isn't meant to be changed directly. It instead generated from the crate's docs
by the [cargo-readme] command:

* Install the command if you don't have it: `cargo install cargo-readme`
* Change the crate-level docs in `src/lib.rs`, or wrapping text in `README.tpl`
* Apply the changes: `cargo readme > README.md`

If you have [rusty-hook] installed the changes will apply automatically on commit.

## License

This project is licensed under the [MIT license](LICENSE).

[cargo-readme]: https://github.com/livioribeiro/cargo-readme
[rusty-hook]: https://github.com/swellaby/rusty-hook
