use super::data_provider::*;
use crate::{kv::Table, MutableCursor};
use std::{cmp::Reverse, collections::BinaryHeap};

pub struct Collector {
    buffer_size: usize,
    data_providers: Vec<DataProvider>,
    buffer_capacity: usize,
    buffer: Vec<Entry>,
}

pub const OPTIMAL_BUFFER_CAPACITY: usize = 512000000; // 512 Megabytes

impl Collector {
    pub fn new(buffer_capacity: usize) -> Collector {
        Collector {
            buffer_size: 0,
            buffer_capacity,
            data_providers: Vec::new(),
            buffer: Vec::new(),
        }
    }

    pub fn collect(&mut self, entry: Entry) {
        self.buffer_size += entry.key.len() + entry.value.len();
        self.buffer.push(entry);
        if self.buffer_size > self.buffer_capacity {
            self.buffer_size = 0;
            self.buffer.sort_unstable();
            let current_id = self.data_providers.len();
            self.data_providers
                .push(DataProvider::new(self.buffer.clone(), current_id).unwrap());
            self.buffer.clear();
        }
    }

    #[allow(clippy::type_complexity)]
    pub async fn load<'tx, T, C>(
        &mut self,
        cursor: &mut C,
        load_function: Option<fn(&mut C, Vec<u8>, Vec<u8>)>,
    ) -> anyhow::Result<()>
    where
        T: Table,
        C: MutableCursor<'tx, T>,
    {
        // If only one data provider is found, then we we can write directly from memory to db without reading any files
        if self.data_providers.is_empty() {
            self.buffer.sort_unstable();
            for entry in &self.buffer {
                if let Some(f) = &load_function {
                    (f)(cursor, entry.key.to_vec(), entry.value.to_vec());
                } else {
                    cursor
                        .put(entry.key.as_slice(), entry.value.as_slice())
                        .await?;
                }
            }
            self.buffer.clear();
            return Ok(());
        }
        // Flush buffer one more time
        if self.buffer_size != 0 {
            self.buffer.sort_unstable();
            let current_id = self.data_providers.len();
            self.data_providers
                .push(DataProvider::new(self.buffer.clone(), current_id).unwrap());
            self.buffer.clear();
        }

        let mut heap = BinaryHeap::new();

        for (current_id, data_provider) in self.data_providers.iter_mut().enumerate() {
            let (current_key, current_value) = data_provider.to_next()?;

            heap.push(Reverse(Entry {
                key: current_key,
                value: current_value,
                id: current_id,
            }));
        }

        while let Some(e) = heap.pop() {
            let entry = e.0;
            if let Some(f) = &load_function {
                (f)(cursor, entry.key.to_vec(), entry.value.to_vec());
            } else {
                cursor
                    .put(entry.key.as_slice(), entry.value.as_slice())
                    .await?;
            }
            let (next_key, next_value) = self.data_providers[entry.id].to_next()?;
            if !next_key.is_empty() {
                heap.push(Reverse(Entry {
                    key: next_key,
                    value: next_value,
                    id: entry.id,
                }));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::{
        new_mem_database, tables,
        traits::{MutableKV, MutableTransaction, Transaction},
    };
    use rand::{distributions::Uniform, Rng}; // 0.6.5
    use std::u64;

    #[tokio::test]
    async fn collect_all_at_once() {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, u64::MAX);
        // generate random entries
        let mut entries: Vec<Entry> = (0..10000)
            .map(|_| Entry {
                key: rng.sample(&range).to_be_bytes().to_vec(),
                value: rng.sample(&range).to_be_bytes().to_vec(),
                id: 0,
            })
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

        for entry in entries.clone() {
            collector.collect(entry);
        }
        // Any cursor is fine
        let mut cursor = tx.mutable_cursor(&tables::HeaderNumber).await.unwrap();
        collector.load(&mut cursor, None).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for entry in entries {
            if let Some(expected_value) = tx
                .get(&tables::HeaderNumber, entry.key.as_slice())
                .await
                .unwrap()
            {
                assert_eq!(entry.value, expected_value);
            }
        }
    }

    #[tokio::test]
    async fn collect_chunks() {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(0, u64::MAX);
        // generate random entries
        let mut entries: Vec<Entry> = (0..10000)
            .map(|_| Entry {
                key: rng.sample(&range).to_be_bytes().to_vec(),
                value: rng.sample(&range).to_be_bytes().to_vec(),
                id: 0,
            })
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = Collector::new(1000);

        for entry in entries.clone() {
            collector.collect(entry);
        }
        // Any cursor is fine
        let mut cursor = tx.mutable_cursor(&tables::HeaderNumber).await.unwrap();
        collector.load(&mut cursor, None).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for entry in entries {
            if let Some(expected_value) = tx
                .get(&tables::HeaderNumber, entry.key.as_slice())
                .await
                .unwrap()
            {
                assert_eq!(entry.value, expected_value);
            }
        }
    }
}
