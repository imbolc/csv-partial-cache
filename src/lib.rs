#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![doc = include_str!("../README.md")]

use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Seek, SeekFrom},
    marker::PhantomData,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Serialize, de::DeserializeOwned};

/// Represents an error that can occur in this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred when opening a file.
    #[error("can't open file: {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    /// An I/O error occurred when creating a file.
    #[error("can't create file: {1}")]
    CreateFile(#[source] std::io::Error, PathBuf),
    /// An I/O error occurred when reading file metadata.
    #[error("can't read file metadata: {1}")]
    ReadFileMetadata(#[source] std::io::Error, PathBuf),
    /// An I/O error occurred when getting the file modification time.
    #[error("can't get file modification time: {1}")]
    GetFileModified(#[source] std::io::Error, PathBuf),
    /// A deserialization error occurred for a line.
    #[error("can't deserialize line: {1}")]
    DeserializeLine(#[source] csv::Error, String),
    /// An I/O error occurred when reading a line.
    #[error("can't read line {1} from {2}")]
    ReadLine(#[source] io::Error, usize, String),
    /// An I/O error occurred when seeking for an offset of a line.
    #[error("can't seek for offset of line {1} in {2}")]
    SeekOffset(#[source] io::Error, usize, String),
    /// An offset conversion error occurred.
    #[error("can't convert offset {0} of line {1} of {2}")]
    IntoOffset(u64, usize, String),
    /// An I/O error occurred when seeking in a file.
    #[error("can't seek in {1}")]
    Seek(#[source] io::Error, PathBuf),
    /// An I/O error occurred when reading a line as an offset.
    #[error("can't read line as offset {1} from {2}")]
    ReadLineOffset(#[source] io::Error, u64, PathBuf),
    /// A CSV error occurred.
    #[error("can't decode csv line")]
    Csv(#[from] csv::Error),
    /// A CSV decoding error occurred with details.
    #[error("can't decode csv line from `{file}` at {offset}: {line}")]
    DecodeDetails {
        /// The underlying cause of the error.
        source: csv::Error,
        /// The path to the file where the error occurred.
        file: PathBuf,
        /// The byte offset in the file where the error occurred.
        offset: u64,
        /// The content of the line where the error occurred.
        line: String,
    },
    /// An error occurred when reading from the cache.
    #[error("can't read cache from: {1}")]
    ReadCache(#[source] serde_json::Error, PathBuf),
    /// An error occurred when writing to the cache.
    #[error("can't write cache into: {1}")]
    WriteCache(#[source] serde_json::Error, PathBuf),
}

/// A `Result` alias where the `Err` case is `csv_partial_cache::Error`.
pub type Result<T> = std::result::Result<T, Error>;

/// A trait for items that can be created from a line and an offset.
pub trait FromLineOffset: Sized {
    /// The type of the offset.
    type Offset: TryFrom<u64> + Into<u64>;

    /// Returns the item's offset.
    fn offset(&self) -> Self::Offset;
    /// Creates an item from a line and an offset.
    fn from_line_offset(line: &str, offset: Self::Offset) -> Result<Self>;
}

/// An index that stores data as a boxed slice sorted by ID.
#[derive(Debug)]
pub struct CsvPartialCache<T> {
    /// The path to the CSV file.
    pub path: PathBuf,
    /// The items in the cache.
    pub items: Box<[T]>,
}

/// An iterator over lines and their byte offsets.
#[derive(Debug)]
pub struct LineOffset<B, O> {
    buf: B,
    buf_name: String,
    index: usize,
    offset: u64,
    _offset_type: PhantomData<O>,
}

impl<B: BufRead, O> LineOffset<B, O> {
    /// Creates a new `LineOffset` from a buffer.
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
    /// Creates a new `LineOffset` from a path.
    pub fn from_path(path: &Path) -> Result<Self> {
        let buf_name = path.to_string_lossy().to_string();
        let f = File::open(path).map_err(|e| Error::OpenFile(e, path.into()))?;
        let buf = BufReader::new(f);
        Ok(Self::from_buf(buf_name, buf))
    }
}

#[cfg(test)]
impl<O> LineOffset<io::Cursor<Vec<u8>>, O> {
    /// Creates a new `LineOffset` from a string.
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
                )));
            }
        };
        let cur_offset = match O::try_from(self.offset) {
            Ok(o) => o,
            Err(_) => {
                return Some(Err(Error::IntoOffset(
                    self.offset,
                    self.index,
                    self.buf_name.to_owned(),
                )));
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
                )));
            }
        };
        Some(Ok((line, cur_offset)))
    }
}

impl<T> CsvPartialCache<T>
where
    T: FromLineOffset,
{
    /// Creates a new `CsvPartialCache` from a path.
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

    /// Finds an item in the cache by a key.
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

    /// Returns a CSV line by its ID.
    async fn details_line(&self, row: &T) -> Result<String> {
        use tokio::{
            fs::File,
            io::{AsyncBufReadExt, AsyncSeekExt, BufReader},
        };

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

    /// Returns the full record by its ID, deserialized into `D`.
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
    /// Creates an index using an intermediate JSON file for faster lookups.
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
        serde_json::to_writer(file, items).map_err(|e| Error::WriteCache(e, cache_path.into()))?;
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

/// Checks if the cache is expired by comparing the modification times of the
/// CSV and cache files.
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
        let mut items = LineOffset::<_, u8>::from_str("noname", &format!("{line1}\nfoo"));
        assert_eq!(items.next().unwrap().unwrap(), (line1, 0));
        assert!(matches!(items.next().unwrap(), Err(Error::IntoOffset(..))));
    }
}
