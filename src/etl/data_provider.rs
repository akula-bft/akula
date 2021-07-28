use std::fs::File;
use anyhow;
use std::io::{Write};
use std::cmp::Ord;
use tempfile::tempfile;
use std::io::SeekFrom;
use std::io::prelude::*;

pub trait Provider {
    fn new(buffer: Vec<Entry>, id: usize) -> anyhow::Result<Self, std::io::Error> where Self: Sized;
    fn to_next(&mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>)>;
}

#[derive(Eq, Clone, PartialEq, PartialOrd, Ord)]
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