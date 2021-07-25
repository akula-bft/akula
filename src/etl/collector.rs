use super::data_provider::*;
// use crate::{common, kv::*, CursorDupSort, *};
use crate::{kv::Table, MutableTransaction, MutableCursor};
use std::collections::BinaryHeap;
use std::cmp::Reverse;

struct Collector {
    buffer_size:     usize,
    data_providers:  Vec<DataProvider>,
    buffer_capacity: usize,
    buffer:          Vec<Entry>,
}

impl Collector {
    fn new(buffer_capacity: usize) -> Collector {
        Collector {
            buffer_size: 0,
            buffer_capacity: buffer_capacity,
            data_providers: Vec::new(),
            buffer: Vec::new(),
        }
    }

    fn collect(&mut self, entry: Entry) {
        self.buffer_size += entry.key.len() + entry.value.len();
        self.buffer.push(entry);
        if self.buffer_size > self.buffer_capacity {
            self.buffer_size = 0;
            self.buffer.sort();
            let current_id = self.data_providers.len();
            self.data_providers.push(DataProvider::new(self.buffer.clone(), current_id).unwrap());
            self.buffer.clear();
        }
    }

    async fn load<'db: 'tx, 'tx, Tx, T, F>(
        &mut self,
        tx: &'tx mut Tx,
        table: &T,
        load_function: Option<F>) -> anyhow::Result<()>
    where 
        T: Table,
        Tx: MutableTransaction<'db>,
        F: Fn(&mut <Tx as MutableTransaction<'db>>::MutableCursor<'tx, T>, Vec<u8>, Vec<u8>),
    {
        let mut cursor = tx.mutable_cursor(table).await?;
        // If only one data provider is found, then we we can write directly from memory to db without reading any files
        if self.data_providers.len() == 0 {
            self.buffer.sort();
            for entry in self.buffer.iter() {
                if let Some(f) = &load_function {
                    (f)(&mut cursor, entry.key.to_vec(), entry.value.to_vec());
                } else {
                    cursor.put(entry.key.as_slice(), entry.value.as_slice()).await?;
                }
            }
            self.buffer.clear();
            return Ok(());
        }
        // Flush buffer one more time
        if self.buffer_size != 0 {
            self.buffer.sort();
            let current_id = self.data_providers.len();
            self.data_providers.push(DataProvider::new(self.buffer.clone(), current_id).unwrap());
            self.buffer.clear();
        }

        let mut heap = BinaryHeap::new();

        for (current_id, data_provider) in self.data_providers.iter_mut().enumerate() {
            let (current_key, current_value) = data_provider.to_next()?;

            heap.push(Reverse(Entry{
                key:   current_key,
                value: current_value,
                id: current_id
            }));
        }

        while heap.len() > 0 {
            let entry = heap.pop().unwrap().0;
            if let Some(f) = &load_function {
                (f)(&mut cursor, entry.key.to_vec(), entry.value.to_vec());
            } else {
                cursor.put(entry.key.as_slice(), entry.value.as_slice()).await?;
            }
            let (next_key, next_value) = self.data_providers[entry.id].to_next()?;
            if next_key.len() > 0 {
                heap.push(Reverse(Entry{
                    key:   next_key,
                    value: next_value,
                    id: entry.id
                }));
            }
        }
        Ok(())
    }
}