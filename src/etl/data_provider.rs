use anyhow;
use std::{
    cmp::Ord,
    fs::File,
    io::{prelude::*, BufReader, BufWriter, SeekFrom},
};
use tempfile::tempfile;

#[derive(Eq, Clone, PartialEq, PartialOrd, Ord)]
pub struct Entry<Key, Value> {
    pub key: Key,
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value> {
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value }
    }
}

pub struct DataProvider {
    file: BufReader<File>,
    len: usize,
}

impl DataProvider {
    pub fn new<Key, Value>(
        buffer: Vec<Entry<Key, Value>>,
    ) -> anyhow::Result<DataProvider, std::io::Error>
    where
        Self: Sized,
        Key: AsRef<[u8]>,
        Value: AsRef<[u8]>,
    {
        let file = tempfile()?;
        let mut w = BufWriter::new(file);
        for entry in &buffer {
            let k = entry.key.as_ref();
            let v = entry.value.as_ref();

            w.write_all(&k.len().to_be_bytes())?;
            w.write_all(&v.len().to_be_bytes())?;
            w.write_all(k)?;
            w.write_all(v)?;
        }

        let mut file = BufReader::new(w.into_inner()?);
        file.seek(SeekFrom::Start(0))?;
        let len = buffer.len();
        Ok(DataProvider { file, len })
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_next(&mut self) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.len == 0 {
            return Ok(None);
        }

        let mut buffer_key_length = [0; 8];
        let mut buffer_value_length = [0; 8];

        self.file.read_exact(&mut buffer_key_length)?;
        self.file.read_exact(&mut buffer_value_length)?;

        let key_length = usize::from_be_bytes(buffer_key_length);
        let value_length = usize::from_be_bytes(buffer_value_length);
        let mut key = vec![0; key_length];
        let mut value = vec![0; value_length];

        self.file.read_exact(&mut key)?;
        self.file.read_exact(&mut value)?;

        self.len -= 1;

        Ok(Some((key, value)))
    }
}
