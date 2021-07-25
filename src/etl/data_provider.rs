use std::fs::File;
use anyhow;
use std::io::{Write};
use std::cmp::{Ord, Ordering};
use tempfile::tempfile;
use std::io::SeekFrom;
use std::io::prelude::*;

pub trait Provider {
    fn new(buffer: Vec<Entry>, id: usize) -> anyhow::Result<Self, std::io::Error> where Self: Sized;
    fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)>;
}

#[derive(Eq, Clone)]
pub struct Entry {
    pub key:   Vec<u8>,
    pub value: Vec<u8>,
    pub id:    usize
}

pub struct DataProvider {
    pub file: File,
    pub id: usize,
}

impl Provider  for DataProvider {
    fn new(buffer: Vec<Entry>, id: usize) -> anyhow::Result<DataProvider, std::io::Error> where Self: Sized {
        let mut tmp_file = tempfile()?;
        for entry in buffer {
            tmp_file.write(&entry.key.len().to_be_bytes())?;
            tmp_file.write(&entry.value.len().to_be_bytes())?;
            tmp_file.write(&entry.key)?;
            tmp_file.write(&entry.value)?;
        }
        // Reset position at 0 byte
        tmp_file.seek(SeekFrom::Start(0))?;
        Ok(DataProvider{
            file: tmp_file,
            id: id,
        })
    }

    fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut buffer_key_length = [0; 8];
        let mut buffer_value_length = [0; 8];

        let mut bytes_read = self.file.read(&mut buffer_key_length[..])?;
        // EOF reached
        if bytes_read == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        bytes_read = self.file.read(&mut buffer_value_length[..])?;
        // EOF reached
        if bytes_read == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        let key_length = usize::from_be_bytes(buffer_key_length);
        let value_length = usize::from_be_bytes(buffer_value_length);
        let mut key = vec![0; key_length];
        let mut value = vec![0; value_length];
        bytes_read = self.file.read(&mut key[..])?;

        // EOF reached
        if bytes_read == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
    
        bytes_read = self.file.read(&mut value[..])?;
        // EOF reached
        if bytes_read == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        Ok((key, value))
    }
}

impl<'kv> Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        let key_cmp = self.key.cmp(&other.key);
        if key_cmp == Ordering::Equal {
            return self.value.cmp(&other.value);
        }
        return key_cmp;
    }
}

impl<'kv> PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'kv> PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key.cmp(&other.key) == Ordering::Equal && self.value.cmp(&other.value) == Ordering::Equal
    }
}