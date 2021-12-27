use super::data_provider::*;
use crate::kv::{tables::ErasedTable, traits::*};
use derive_more::*;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    ops::{Generator, GeneratorState},
    pin::Pin,
};

pub struct Collector<Key, Value>
where
    Key: TableEncode,
    Value: TableEncode,
    <Key as TableEncode>::Encoded: Ord,
    <Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<Key as TableEncode>::Encoded>,
    Vec<u8>: From<<Value as TableEncode>::Encoded>,
{
    buffer_size: usize,
    data_providers: Vec<DataProvider>,
    buffer_capacity: usize,
    buffer: Vec<Entry<<Key as TableEncode>::Encoded, <Value as TableEncode>::Encoded>>,
}

pub const OPTIMAL_BUFFER_CAPACITY: usize = 512000000; // 512 Megabytes

impl<Key, Value> Collector<Key, Value>
where
    Key: TableEncode,
    Value: TableEncode,
    <Key as TableEncode>::Encoded: Ord,
    <Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<Key as TableEncode>::Encoded>,
    Vec<u8>: From<<Value as TableEncode>::Encoded>,
{
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            buffer_size: 0,
            buffer_capacity,
            data_providers: Vec::new(),
            buffer: Vec::new(),
        }
    }

    fn flush(&mut self) {
        self.buffer_size = 0;
        self.buffer.sort_unstable();
        let mut buf = Vec::with_capacity(self.buffer.len());
        std::mem::swap(&mut buf, &mut self.buffer);
        self.data_providers.push(DataProvider::new(buf).unwrap());
    }

    pub fn push(&mut self, key: Key, value: Value) {
        let key = key.encode();
        let value = value.encode();
        self.buffer_size += key.as_ref().len() + value.as_ref().len();
        self.buffer.push(Entry { key, value });
        if self.buffer_size > self.buffer_capacity {
            self.flush();
        }
    }

    pub fn iter(&mut self) -> CollectorIter<'_> {
        CollectorIter {
            done: false,
            inner: Box::pin(|| {
                // If only one data provider is found, then we we can write directly from memory to db without reading any files
                if self.data_providers.is_empty() {
                    self.buffer.sort_unstable();
                    for entry in self.buffer.drain(..) {
                        yield Ok((entry.key.into(), entry.value.into()));
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
                    yield Ok((key, value));
                    if let Some((next_key, next_value)) = self.data_providers[id].to_next()? {
                        // Insert another from the same data provider unless it's exhausted.
                        heap.push(Reverse((Entry::new(next_key, next_value), id)));
                    }
                }
                Ok(())
            }),
        }
    }
}

pub struct CollectorIter<'a> {
    done: bool,
    inner: Pin<
        Box<
            dyn Generator<Yield = anyhow::Result<(Vec<u8>, Vec<u8>)>, Return = anyhow::Result<()>>
                + Send
                + 'a,
        >,
    >,
}

impl<'a> Iterator for CollectorIter<'a> {
    type Item = anyhow::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match Pin::new(&mut self.inner).resume(()) {
            GeneratorState::Yielded(res) => Some(res),
            GeneratorState::Complete(res) => {
                self.done = true;
                match res {
                    Ok(()) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }
}

#[derive(Deref, DerefMut, From)]
pub struct TableCollector<T>(Collector<T::Key, T::Value>)
where
    T: Table,
    T::Key: TableEncode,
    T::Value: TableEncode,
    <<T as Table>::Key as TableEncode>::Encoded: Ord,
    <<T as Table>::Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<<T as Table>::Key as TableEncode>::Encoded>,
    Vec<u8>: From<<<T as Table>::Value as TableEncode>::Encoded>;

impl<T> TableCollector<T>
where
    T: Table,
    T::Key: TableEncode,
    T::Value: TableEncode,
    <<T as Table>::Key as TableEncode>::Encoded: Ord,
    <<T as Table>::Value as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<<T as Table>::Key as TableEncode>::Encoded>,
    Vec<u8>: From<<<T as Table>::Value as TableEncode>::Encoded>,
{
    pub fn new(buffer_capacity: usize) -> Self {
        Self(Collector::new(buffer_capacity))
    }

    pub fn into_inner(self) -> Collector<T::Key, T::Value> {
        self.0
    }

    #[allow(clippy::type_complexity)]
    pub async fn load<'tx, C>(&mut self, cursor: &mut C) -> anyhow::Result<()>
    where
        C: MutableCursor<'tx, ErasedTable<T>>,
    {
        for res in self.iter() {
            let (k, v) = res?;

            cursor.put(k, v).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::{new_mem_database, tables},
        models::BlockNumber,
    };

    #[tokio::test]
    async fn collect_all_at_once() {
        // generate random entries
        let mut entries: Vec<(_, _)> = (0..10000)
            .map(|_| (rand::random(), BlockNumber(rand::random())))
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = TableCollector::new(OPTIMAL_BUFFER_CAPACITY);

        for (key, value) in entries.clone() {
            collector.push(key, value);
        }
        // Any cursor is fine
        let mut cursor = tx
            .mutable_cursor(tables::HeaderNumber.erased())
            .await
            .unwrap();
        collector.load(&mut cursor).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for (key, value) in entries {
            if let Some(expected_value) = tx.get(tables::HeaderNumber, key).await.unwrap() {
                assert_eq!(value, expected_value);
            }
        }
    }

    #[tokio::test]
    async fn collect_chunks() {
        // generate random entries
        let mut entries: Vec<(_, _)> = (0..5000)
            .map(|_| (rand::random(), BlockNumber(rand::random())))
            .collect();
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let mut collector = TableCollector::new(1000);

        for (key, value) in entries.clone() {
            collector.push(key, value);
        }
        // Any cursor is fine
        let mut cursor = tx
            .mutable_cursor(tables::HeaderNumber.erased())
            .await
            .unwrap();
        collector.load(&mut cursor).await.unwrap();

        // We sort the entries and compare them to what is in db
        entries.sort_unstable();

        for (key, value) in entries {
            if let Some(expected_value) = tx.get(tables::HeaderNumber, key).await.unwrap() {
                assert_eq!(value, expected_value);
            }
        }
    }
}
