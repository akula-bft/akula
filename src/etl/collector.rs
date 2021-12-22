use super::data_provider::*;
use crate::kv::{tables::ErasedTable, traits::*};
use std::{cmp::Reverse, collections::BinaryHeap, marker::PhantomData};

pub struct Collector<T>
where
    T: Table,
    <<T as Table>::Key as TableEncode>::Encoded: Ord,
    <<T as Table>::Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<<T as Table>::Key as TableEncode>::Encoded>,
    Vec<u8>: From<<<T as Table>::Value as TableEncode>::Encoded>,
{
    buffer_size: usize,
    data_providers: Vec<DataProvider>,
    buffer_capacity: usize,
    buffer: Vec<
        Entry<
            <<T as Table>::Key as TableEncode>::Encoded,
            <<T as Table>::Value as TableEncode>::Encoded,
        >,
    >,
    _marker: PhantomData<T>,
}

pub const OPTIMAL_BUFFER_CAPACITY: usize = 512000000; // 512 Megabytes

impl<T> Collector<T>
where
    T: Table,
    <<T as Table>::Key as TableEncode>::Encoded: Ord,
    <<T as Table>::Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<<T as Table>::Key as TableEncode>::Encoded>,
    Vec<u8>: From<<<T as Table>::Value as TableEncode>::Encoded>,
{
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            buffer_size: 0,
            buffer_capacity,
            data_providers: Vec::new(),
            buffer: Vec::new(),
            _marker: PhantomData,
        }
    }

    fn flush(&mut self) {
        self.buffer_size = 0;
        self.buffer.sort_unstable();
        let mut buf = Vec::with_capacity(self.buffer.len());
        std::mem::swap(&mut buf, &mut self.buffer);
        self.data_providers.push(DataProvider::new(buf).unwrap());
    }

    pub fn collect(&mut self, entry: Entry<T::Key, T::Value>) {
        let key = entry.key.encode();
        let value = entry.value.encode();
        self.buffer_size += key.as_ref().len() + value.as_ref().len();
        self.buffer.push(Entry { key, value });
        if self.buffer_size > self.buffer_capacity {
            self.flush();
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn load<'tx, C>(&mut self, cursor: &mut C) -> anyhow::Result<()>
    where
        C: MutableCursor<'tx, ErasedTable<T>>,
    {
        // If only one data provider is found, then we we can write directly from memory to db without reading any files
        if self.data_providers.is_empty() {
            self.buffer.sort_unstable();
            for entry in self.buffer.drain(..) {
                cursor.put(entry.key.into(), entry.value.into()).await?;
            }
            return Ok(());
        }
        // Flush buffer one more time
        if self.buffer_size != 0 {
            self.flush();
        }

        let mut heap = BinaryHeap::new();

        // Anchor each data provider in the heap
        for (current_id, data_provider) in self.data_providers.iter_mut().enumerate() {
            if let Some((current_key, current_value)) = data_provider.to_next()? {
                heap.push(Reverse((
                    Entry::new(current_key, current_value),
                    current_id,
                )));
            }
        }

        // Take the lowest entry from all data providers in the heap.
        while let Some(Reverse((Entry { key, value }, id))) = heap.pop() {
            cursor.put(key, value).await?;
            if let Some((next_key, next_value)) = self.data_providers[id].to_next()? {
                // Insert another from the same data provider unless it's exhausted.
                heap.push(Reverse((Entry::new(next_key, next_value), id)));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::{
            new_mem_database, tables,
            traits::{MutableKV, MutableTransaction, Transaction},
        },
        models::BlockNumber,
    };

    #[tokio::test]
    async fn collect_all_at_once() {
        // generate random entries
        let mut entries: Vec<Entry<_, _>> = (0..10000)
            .map(|_| Entry::new(rand::random(), BlockNumber(rand::random())))
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

        for entry in entries.clone() {
            collector.collect(entry);
        }
        // Any cursor is fine
        let mut cursor = tx
            .mutable_cursor(tables::HeaderNumber.erased())
            .await
            .unwrap();
        collector.load(&mut cursor).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for entry in entries {
            if let Some(expected_value) = tx.get(tables::HeaderNumber, entry.key).await.unwrap() {
                assert_eq!(entry.value, expected_value);
            }
        }
    }

    #[tokio::test]
    async fn collect_chunks() {
        // generate random entries
        let mut entries: Vec<Entry<_, _>> = (0..5000)
            .map(|_| Entry::new(rand::random(), BlockNumber(rand::random())))
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = Collector::new(1000);

        for entry in entries.clone() {
            collector.collect(entry);
        }
        // Any cursor is fine
        let mut cursor = tx
            .mutable_cursor(tables::HeaderNumber.erased())
            .await
            .unwrap();
        collector.load(&mut cursor).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for entry in entries {
            if let Some(expected_value) = tx.get(tables::HeaderNumber, entry.key).await.unwrap() {
                assert_eq!(entry.value, expected_value);
            }
        }
    }
}
