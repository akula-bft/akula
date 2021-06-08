use crate::MutableTransaction;
use bytes::Bytes;
use std::collections::HashMap;

pub struct OldestEntrySortableBufer<'tx> {
    entries: HashMap<String, Bytes<'tx>>,
    size: usize,
    optimal_size: usize,
    sorted: Vec<(Bytes<'tx>, Bytes<'tx>)>,
}

impl<'tx> OldestEntrySortableBufer<'tx> {
    pub fn new(optimal_size: usize) -> Self {
        Self {
            entries: Default::default(),
            size: Default::default(),
            optimal_size,
            sorted: Default::default(),
        }
    }
}

pub async fn transform<'db, Tx: MutableTransaction<'db>>(tx: &Tx) -> anyhow::Result<()> {
    Ok(())
}
