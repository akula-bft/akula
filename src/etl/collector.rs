use super::data_provider::*;
// use crate::{common, kv::*, CursorDupSort, *};

struct Collector<'kv> {
    buffer_size:     usize,
    data_providers:  Vec<DataProvider>,
    buffer_capacity: usize,
    buffer:          Vec<Entry<'kv>>,
}

impl<'kv> Collector<'kv> {
    fn new(buffer_capacity: usize) -> Collector<'kv> {
        Collector {
            buffer_size: 0,
            buffer_capacity: buffer_capacity,
            data_providers: Vec::new(),
            buffer: Vec::new(),
        }
    }

    fn collect(&mut self, entry: Entry<'kv>) {
        self.buffer_size += entry.key.len() + entry.value.len();
        self.buffer.push(entry);
        if self.buffer_size > self.buffer_capacity {
            self.buffer_size = 0;
            self.buffer.sort();
            self.data_providers.push(DataProvider::new(self.buffer.clone()).unwrap());
            self.buffer.clear();
        }
        // TODO: in case buffer_size > buffer_capacity write to disk
    }
}