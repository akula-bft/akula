use anyhow;
use std::{
    cmp::Ord,
    fs::File,
    io::{prelude::*, SeekFrom},
};
use tempfile::tempfile;

#[derive(Eq, Clone, PartialEq, PartialOrd, Ord)]
pub struct Entry<Key, Value> {
    pub key: Key,
    pub value: Value,
    pub id: usize,
}

impl<Key, Value> Entry<Key, Value> {
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value, id: 0 }
    }
}

pub struct DataProvider {
    pub file: File,
    pub id: usize,
}

impl DataProvider {
    pub fn new<Key, Value>(
        buffer: Vec<Entry<Key, Value>>,
        id: usize,
    ) -> anyhow::Result<DataProvider, std::io::Error>
    where
        Self: Sized,
        Key: AsRef<[u8]>,
        Value: AsRef<[u8]>,
    {
        let mut file = tempfile()?;

        for entry in buffer {
            let k = entry.key.as_ref();
            let v = entry.value.as_ref();

            file.write(&k.len().to_be_bytes())?;
            file.write(&v.len().to_be_bytes())?;
            file.write(k)?;
            file.write(v)?;
        }
        // Reset position at 0 byte
        file.seek(SeekFrom::Start(0))?;
        Ok(DataProvider { file, id })
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut buffer_key_length = [0; 8];
        let mut buffer_value_length = [0; 8];

        // EOF reached
        if self.file.read(&mut buffer_key_length[..])? == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        // EOF reached
        if self.file.read(&mut buffer_value_length[..])? == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        let key_length = usize::from_be_bytes(buffer_key_length);
        let value_length = usize::from_be_bytes(buffer_value_length);
        let mut key = vec![0; key_length];
        let mut value = vec![0; value_length];

        // EOF reached
        if self.file.read(&mut key[..])? == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        // EOF reached
        if self.file.read(&mut value[..])? == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        Ok((key, value))
    }
}
