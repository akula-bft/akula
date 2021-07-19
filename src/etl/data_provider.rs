use std::fs::File;
use anyhow;
use std::io::{Write};
use std::cmp::{Ord, Ordering};
use tempfile::tempfile;
use std::io::SeekFrom;
use std::io::prelude::*;

pub trait Provider<'kv>  {
    fn new(buffer: Vec<Entry<'kv>>) -> anyhow::Result<Self, std::io::Error> where Self: Sized;
    fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)>;
}

#[derive(Eq, Copy, Clone)]
pub struct Entry<'kv> {
    pub key: &'kv [u8],
    pub value: &'kv [u8],
}

pub type DataProvider = File;

impl<'kv> Provider<'kv>  for DataProvider {
    fn new(buffer: Vec<Entry<'kv>>) -> anyhow::Result<DataProvider, std::io::Error> where Self: Sized {
        let mut tmp_file = tempfile()?;
        for entry in buffer {
            tmp_file.write(&entry.key.len().to_be_bytes())?;
            tmp_file.write(&entry.value.len().to_be_bytes())?;
            tmp_file.write(&entry.key)?;
            tmp_file.write(&entry.value)?;
        }
        // Reset position at 0 byte
        tmp_file.seek(SeekFrom::Start(0))?;
        Ok(tmp_file)
    }

    fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let mut buffer_key_length = [0; 8];
        let mut buffer_value_length = [0; 8];

        let mut bytes_read = self.read(&mut buffer_key_length[..])?;
        // Check for Errors
        if bytes_read != 8 {
            return Ok((Vec::new(), Vec::new()));
        }
        bytes_read = self.read(&mut buffer_value_length[..])?;
        // Check for Errors
        if bytes_read != 8 {
            return Ok((Vec::new(), Vec::new()));
        }

        let key_length = usize::from_be_bytes(buffer_key_length);
        let value_length = usize::from_be_bytes(buffer_value_length);
        let mut key = vec![0; key_length];
        let mut value = vec![0; value_length];
        bytes_read = self.read(&mut key[..])?;

        // Check for Errors
        if bytes_read != key_length {
            return Ok((Vec::new(), Vec::new()));
        }
    
        bytes_read = self.read(&mut value[..])?;
        // Check for Errors
        if bytes_read != value_length {
            return Ok((Vec::new(), Vec::new()));
        }
        Ok((key, value))
    }
}

impl<'kv> Ord for Entry<'kv> {
    fn cmp(&self, other: &Self) -> Ordering {
        let key_cmp = self.key.cmp(&other.key);
        if key_cmp == Ordering::Equal {
            return self.value.cmp(&other.value);
        }
        return key_cmp;
    }
}

impl<'kv> PartialOrd for Entry<'kv> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'kv> PartialEq for Entry<'kv> {
    fn eq(&self, other: &Self) -> bool {
        self.key.cmp(&other.key) == Ordering::Equal && self.value.cmp(&other.value) == Ordering::Equal
    }
}