//! # csv-partial-cache
//!
//! Csv index with partially cached columns
//!
//! It meant to allow a performant access to immutable csv data without importing it to database.
//! The idea is to keep line offsets with frequently used columns in memory, while accessing the
//! full line following the offset.
//!
//!
//! Usage
//! -----
//!
//! Let's say we have a table of http statuses:
//!
//! | code | name                | description                                           |
//! |------|---------------------|-------------------------------------------------------|
//! | 100  | Continue            | Status code 100 Continue tells you that a part ...    |
//! | 101  | Switching Protocols | There have been many HTTP protocols created since ... |
//!
//! And we'd like to cache short `code` and `name` columns.
//!
//! ```
//! use csv_partial_cache::{self, CsvPartialCache, FromLineOffset};
//!
//! /// Full table row representation
//! #[derive(serde::Deserialize)]
//! struct FullRecord {
//!     code: u16,
//!     name: String,
//!     description: String,
//! }
//!
//! /// Columns `code` and `name` are essential and small, so we'd like to keep them and memory
//! struct PartialRecord {
//!     code: u16,
//!     name: String,
//!     // The file offset to access the full record data
//!     offset: u16,
//! }
//!
//! // We should tell a little more about the cached representation
//! impl FromLineOffset for PartialRecord {
//!     /// type of file offsets used instead of default `u64` to spare some memory
//!     type Offset = u16;
//!
//!     /// Pointer to the offset field
//!     fn offset(&self) -> Self::Offset {
//!         self.offset
//!     }
//!
//!     /// Constructor from the line and it's offset
//!     fn from_line_offset(line: &str, offset: Self::Offset) -> csv_partial_cache::Result<Self> {
//!         let (code, name) = csv_line::from_str(line)?;
//!         Ok(Self { code, name, offset })
//!     }
//! }
//!
//! tokio_test::block_on(async {
//!     // Now we can load the index
//!     let cache = CsvPartialCache::<PartialRecord>::new("tests/status_codes.csv").unwrap();
//!
//!     // Getting a partial data cached in memory
//!     let partial = cache.find(&100, |x| x.code).unwrap();
//!     assert_eq!(partial.name, "Continue");
//!
//!     /// Loading full data from the csv file
//!     let full: FullRecord = cache.full_record(&partial).await.unwrap();
//!     assert_eq!(full.code, partial.code);
//! })
//! ```

#![warn(clippy::all, nonstandard_style, future_incompatible)]

use serde::{de::DeserializeOwned, Serialize};
use std::fs::{self, File};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::{
    io::{self, BufRead, BufReader, Seek, SeekFrom},
    time::SystemTime,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("can't open file: {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("can't create file: {1}")]
    CreateFile(#[source] std::io::Error, PathBuf),
    #[error("can't read file metadata: {1}")]
    ReadFileMetadata(#[source] std::io::Error, PathBuf),
    #[error("can't get file modification time: {1}")]
    GetFileModified(#[source] std::io::Error, PathBuf),
    #[error("can't deserialize line: {1}")]
    DeserializeLine(#[source] csv_line::Error, String),
    #[error("can't read line {1} from {2}")]
    ReadLine(#[source] io::Error, usize, String),
    #[error("can't seek for offset of line {1} in {2}")]
    SeekOffset(#[source] io::Error, usize, String),
    #[error("can't convert offset {0} of line {1} of {2}")]
    IntoOffset(u64, usize, String),
    #[error("can't seek in {1}")]
    Seek(#[source] io::Error, PathBuf),
    #[error("can't read line as offset {1} from {2}")]
    ReadLineOffset(#[source] io::Error, u64, PathBuf),
    #[error("can't decode csv line")]
    CsvLine(#[from] csv_line::Error),
    #[error("can't decode csv line from `{file}` at {offset}: {line}")]
    DecodeDetails {
        source: csv_line::Error,
        file: PathBuf,
        offset: u64,
        line: String,
    },
    #[error("can't read cache from: {1}")]
    ReadCache(#[source] serde_json::Error, PathBuf),
    #[error("can't write cache into: {1}")]
    WriteCache(#[source] serde_json::Error, PathBuf),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait FromLineOffset: Sized {
    type Offset: TryFrom<u64> + Into<u64>;

    fn offset(&self) -> Self::Offset;
    fn from_line_offset(line: &str, offset: Self::Offset) -> Result<Self>;
}

/// An index keeping the data as a sorted by id boxed slice
#[derive(Debug)]
pub struct CsvPartialCache<T> {
    pub path: PathBuf,
    pub items: Box<[T]>,
}

/// An iterator over lines and their offsets
#[derive(Debug)]
pub struct LineOffset<B, O> {
    buf: B,
    buf_name: String,
    index: usize,
    offset: u64,
    _offset_type: PhantomData<O>,
}

impl<B: BufRead, O> LineOffset<B, O> {
    pub fn from_buf(name: impl Into<String>, buf: B) -> Self {
        Self {
            buf,
            buf_name: name.into(),
            index: 0,
            offset: 0,
            _offset_type: PhantomData,
        }
    }
}

impl<O> LineOffset<BufReader<File>, O> {
    pub fn from_path(path: &Path) -> Result<Self> {
        let buf_name = path.to_string_lossy().to_string();
        let f = File::open(path).map_err(|e| Error::OpenFile(e, path.into()))?;
        let buf = BufReader::new(f);
        Ok(Self::from_buf(buf_name, buf))
    }
}

#[cfg(test)]
impl<O> LineOffset<io::Cursor<Vec<u8>>, O> {
    pub fn from_str(name: impl Into<String>, s: &str) -> Self {
        let buf = io::Cursor::new(s.as_bytes().to_vec());
        Self::from_buf(name, buf)
    }
}

impl<B, O> Iterator for LineOffset<B, O>
where
    B: BufRead + Seek,
    O: TryFrom<u64>,
{
    type Item = Result<(String, O)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        match self.buf.read_line(&mut line) {
            Ok(0) => return None,
            Ok(_n) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
            }
            Err(e) => {
                return Some(Err(Error::ReadLine(
                    e,
                    self.index,
                    self.buf_name.to_owned(),
                )))
            }
        };
        let cur_offset = match O::try_from(self.offset) {
            Ok(o) => o,
            Err(_) => {
                return Some(Err(Error::IntoOffset(
                    self.offset,
                    self.index,
                    self.buf_name.to_owned(),
                )))
            }
        };
        self.index += 1;
        self.offset = match self.buf.stream_position() {
            Ok(o) => o,
            Err(e) => {
                return Some(Err(Error::SeekOffset(
                    e,
                    self.index,
                    self.buf_name.to_owned(),
                )))
            }
        };
        Some(Ok((line, cur_offset)))
    }
}

impl<T> CsvPartialCache<T>
where
    T: FromLineOffset,
{
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut items = Vec::new();
        let mut index = LineOffset::from_path(&path)?;
        index.next(); // skip the header
        for row in index {
            let (line, offset) = row?;
            items.push(T::from_line_offset(&line, offset)?);
        }
        let items = items.into_boxed_slice();
        Ok(Self { path, items })
    }

    /// .binary_search_by_key(&qid, |item| item.qid)
    pub fn find<B, F>(&self, b: &B, f: F) -> Option<&T>
    where
        F: FnMut(&T) -> B,
        B: Ord,
    {
        self.items
            .binary_search_by_key(b, f)
            .map(|index| self.items.get(index).unwrap())
            .ok()
    }

    /// Returns a csv line by it's id
    async fn details_line(&self, row: &T) -> Result<String> {
        use tokio::fs::File;
        use tokio::io::BufReader;
        use tokio::io::{AsyncBufReadExt, AsyncSeekExt};

        let mut f = File::open(&self.path)
            .await
            .map_err(|e| Error::OpenFile(e, self.path.to_owned()))?;
        let offset = row.offset().into();
        f.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| Error::Seek(e, self.path.to_owned()))?;
        let mut buf = BufReader::new(f);
        let mut line = String::new();
        buf.read_line(&mut line)
            .await
            .map_err(|e| Error::ReadLineOffset(e, offset, self.path.to_owned()))?;
        Ok(line)
    }

    /// Returns the first record by the `id`, deserialized into `T`
    pub async fn full_record<D: DeserializeOwned>(&self, row: &T) -> Result<D> {
        let line = self.details_line(row).await?;
        csv_line::from_str::<D>(&line).map_err(|e| Error::DecodeDetails {
            source: e,
            file: self.path.clone(),
            offset: row.offset().into(),
            line,
        })
    }
}

// #[cfg(feature = "cache")]
impl<T> CsvPartialCache<T>
where
    T: FromLineOffset + DeserializeOwned + Serialize,
{
    /// Creates the index using intermediate json file for speed-up
    pub fn from_cache(csv_path: impl Into<PathBuf>, cache_path: impl AsRef<Path>) -> Result<Self> {
        let csv_path = csv_path.into();
        let cache_path = cache_path.as_ref();
        Ok(if is_cache_expired(&csv_path, cache_path)? {
            let index = Self::new(csv_path)?;
            Self::items_to_cache(&index.items, cache_path)?;
            index
        } else {
            let items = Self::items_from_cache(cache_path)?;
            Self {
                path: csv_path,
                items,
            }
        })
    }

    fn items_to_cache(items: &[T], cache_path: &Path) -> Result<()> {
        let file = File::create(cache_path).map_err(|e| Error::CreateFile(e, cache_path.into()))?;
        serde_json::to_writer(file, items).map_err(|e| Error::ReadCache(e, cache_path.into()))?;
        Ok(())
    }

    fn items_from_cache(cache_path: &Path) -> Result<Box<[T]>> {
        let file = File::open(cache_path).map_err(|e| Error::OpenFile(e, cache_path.into()))?;
        let reader = BufReader::new(file);
        let items: Vec<T> =
            serde_json::from_reader(reader).map_err(|e| Error::ReadCache(e, cache_path.into()))?;
        Ok(items.into_boxed_slice())
    }
}

/// Checks if the csv file is modified after caching
fn is_cache_expired(csv_path: &Path, cache_path: &Path) -> Result<bool> {
    if !Path::new(cache_path).exists() {
        return Ok(true);
    }
    let csv_modified = file_modified_at(csv_path)?;
    let cache_modified = file_modified_at(cache_path)?;
    Ok(cache_modified < csv_modified)
}

fn file_modified_at(path: &Path) -> Result<SystemTime> {
    let meta = fs::metadata(path).map_err(|e| Error::ReadFileMetadata(e, path.into()))?;
    meta.modified()
        .map_err(|e| Error::GetFileModified(e, path.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iteration() {
        let mut items = LineOffset::from_str("noname", "foo\nbar\r\nbaz");
        assert_eq!(items.next().unwrap().unwrap(), ("foo".into(), 0));
        assert_eq!(items.next().unwrap().unwrap(), ("bar".into(), 4));
        assert_eq!(items.next().unwrap().unwrap(), ("baz".into(), 9));
        assert!(items.next().is_none());
    }

    #[test]
    fn iteration_offset_overflow() {
        let line1 = "x".repeat(255);
        let mut items = LineOffset::<_, u8>::from_str("noname", &format!("{}\nfoo", line1));
        assert_eq!(items.next().unwrap().unwrap(), (line1, 0));
        assert!(matches!(items.next().unwrap(), Err(Error::IntoOffset(..))));
    }
}
