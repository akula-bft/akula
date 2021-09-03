use crate::{
    kv::{
        traits::{Cursor, MutableCursor},
        CustomTable, Table,
    },
    MutableTransaction,
};
use akula_table_defs::TABLES;
use anyhow::Result;
use bytes::Bytes;
use static_bytes::Bytes as StaticBytes;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
    sync::Mutex,
};

type TableName = string::String<StaticBytes>;
type SimpleBucket = BTreeMap<Vec<u8>, Option<StaticBytes>>;
type SimpleBuffer = HashMap<TableName, SimpleBucket>;
type DupSortValues = BTreeSet<StaticBytes>;

#[derive(Default)]
struct DupSortChanges {
    insert: DupSortValues,
    delete: DupSortValues,
}

type DupSortBucket = BTreeMap<Vec<u8>, DupSortChanges>;
type DupSortBuffer = HashMap<TableName, DupSortBucket>;

fn is_dupsort(table_name: &str) -> bool {
    if let Some(info) = TABLES.get(table_name) {
        info.dup_sort.is_some()
    } else {
        false
    }
}

struct Mutation<'tx: 'm, 'm, Tx: MutableTransaction<'tx>> {
    parent: &'m mut Tx,
    phantom: PhantomData<&'tx i32>,
    simple_buffer: Mutex<HashMap<TableName, SimpleBucket>>,
    dupsort_buffer: Mutex<HashMap<TableName, DupSortBucket>>,
    sequence_increment: Mutex<HashMap<TableName, u64>>,
}

impl<'tx: 'm, 'm, Tx: MutableTransaction<'tx>> Mutation<'tx, 'm, Tx> {
    fn new(parent: &'m mut Tx) -> Self {
        Mutation {
            parent,
            phantom: PhantomData,
            simple_buffer: Default::default(),
            dupsort_buffer: Default::default(),
            sequence_increment: Default::default(),
        }
    }

    async fn get<T>(&'m self, table: &T, key: &[u8]) -> Result<Option<Bytes<'m>>>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if let Some(bucket) = (*self.simple_buffer.lock().unwrap()).get(&table_name) {
            match bucket.get(key) {
                Some(value) => Ok(value.as_ref().map(|b| Bytes::from(b.to_vec()))),
                None => self.parent.get(table, key).await,
            }
        } else if let Some(bucket) = (*self.dupsort_buffer.lock().unwrap()).get(&table_name) {
            match bucket.get(key) {
                Some(entry) => Ok({
                    let mut first = entry.insert.first().map(|b| Bytes::from(b.to_vec()));

                    let mut parent_cursor = self.parent.cursor(table).await?;
                    let mut found = parent_cursor.seek_exact(key).await?.map(|p| (true, p.1));

                    // Return the first non-deleted value from the parent transaction
                    // if it is ordered before our found value, or if we found None
                    while let Some((key_match, parent_value)) = found {
                        if !key_match {
                            break;
                        }
                        if let Some(value) = &first {
                            if value < &parent_value {
                                break;
                            }
                        }
                        if !entry.delete.contains(parent_value.as_ref()) {
                            first = Some(parent_value.clone());
                            break;
                        }
                        found = parent_cursor.next().await?.map(|p| (p.0 == key, p.1));
                    }

                    first
                }),
                None => self.parent.get(table, key).await,
            }
        } else {
            self.parent.get(table, key).await
        }
    }

    async fn read_sequence<T>(&self, table: &T) -> Result<u64>
    where
        T: Table,
    {
        let sequence_increment = self.sequence_increment.lock().unwrap();
        let parent_value = self.parent.read_sequence(table).await.unwrap_or(0);
        let increment = (*sequence_increment).get(&table.db_name()).unwrap_or(&0);

        Ok(parent_value + increment)
    }

    fn init_simple_buffer(&self, name: &TableName) {
        let mut buffer = self.simple_buffer.lock().unwrap();
        if !buffer.contains_key(name) {
            buffer.insert(name.clone(), Default::default());
        }
    }

    fn init_dupsort_changes(&self, name: &TableName, k: &[u8]) {
        let mut buffer = self.dupsort_buffer.lock().unwrap();
        if !buffer.contains_key(name) {
            buffer.insert(name.clone(), Default::default());
        }
        let changes = buffer.get_mut(name).unwrap();
        if !changes.contains_key(k) {
            let copied_key: Vec<u8> = (*k).to_vec();
            changes.insert(copied_key, Default::default());
        }
    }

    async fn set<T>(&self, table: &T, k: &[u8], v: &[u8]) -> Result<()>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if is_dupsort(&table_name) {
            self.init_dupsort_changes(&table_name, k);
            let mut buffer = self.dupsort_buffer.lock().unwrap();
            let changes = buffer.get_mut(&table_name).unwrap().get_mut(k).unwrap();
            let copied_value: Vec<u8> = (*v).to_vec();
            changes.insert.insert(StaticBytes::from(copied_value));
            changes.delete.remove(v);
        } else {
            let copied_value: Vec<u8> = (*v).to_vec();
            self.init_simple_buffer(&table_name);
            self.simple_buffer
                .lock()
                .unwrap()
                .get_mut(&table_name)
                .unwrap()
                .insert(k.to_vec(), Some(StaticBytes::from(copied_value)));
        }

        Ok(())
    }

    async fn delete_key<T>(&self, table: &T, k: &[u8]) -> Result<()>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if is_dupsort(&table_name) {
            let mut parent_values = DupSortValues::new();

            {
                let mut parent_cursor = self.parent.cursor(table).await?;
                let mut found = parent_cursor.seek_exact(k).await?.map(|p| (true, p.1));

                while let Some((matches_key, parent_value)) = &found {
                    if !matches_key {
                        break;
                    }
                    let copied_value = (*parent_value).to_vec();
                    parent_values.insert(StaticBytes::from(copied_value));
                    found = parent_cursor.next().await?.map(|p| (p.0 == k, p.1));
                }
            }

            self.init_dupsort_changes(&table_name, k);
            let mut buffer = self.dupsort_buffer.lock().unwrap();
            let changes = buffer.get_mut(&table_name).unwrap().get_mut(k).unwrap();
            changes.insert.clear();
            changes.delete = parent_values;
        } else {
            self.init_simple_buffer(&table_name);
            self.simple_buffer
                .lock()
                .unwrap()
                .get_mut(&table_name)
                .unwrap()
                .insert(k.to_vec(), None);
        }

        Ok(())
    }

    async fn delete_pair<T>(&self, table: &T, k: &[u8], v: &[u8]) -> Result<()>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if is_dupsort(&table_name) {
            self.init_dupsort_changes(&table_name, k);
            let mut buffer = self.dupsort_buffer.lock().unwrap();
            let changes = buffer.get_mut(&table_name).unwrap().get_mut(k).unwrap();
            let copied_value = (*v).to_vec();
            changes.insert.remove(v);
            changes.delete.insert(StaticBytes::from(copied_value));
        } else {
            self.init_simple_buffer(&table_name);
            let mut buffer = self.simple_buffer.lock().unwrap();
            let bucket = (*buffer).get_mut(&table_name).unwrap();
            if let Some(Some(value)) = bucket.get(k) {
                if value == v {
                    bucket.insert(k.to_vec(), None);
                }
            }
        }

        Ok(())
    }

    async fn commit(self) -> anyhow::Result<()> {
        let simple_buffer = &*self.simple_buffer.lock().unwrap();
        for (table_name, bucket) in simple_buffer {
            let table = CustomTable {
                0: table_name.clone(),
            };
            let mut cursor = self.parent.mutable_cursor(&table).await?;
            for (key, ref maybe_value) in bucket {
                match maybe_value {
                    Some(ref value) => cursor.put(key, value).await?,
                    None => {
                        let maybe_deleted_pair = cursor.seek_exact(key).await;
                        if let Ok(Some((ref key, ref value))) = maybe_deleted_pair {
                            cursor.delete(key, value).await?;
                        }
                    }
                }
            }
        }

        let dupsort_buffer = &*self.dupsort_buffer.lock().unwrap();
        for (table_name, bucket) in dupsort_buffer {
            let table = CustomTable {
                0: table_name.clone(),
            };
            let mut cursor = self.parent.mutable_cursor(&table).await?;
            for (key, changes) in bucket {
                for value in &changes.delete {
                    cursor.delete(key, value).await?
                }
                for value in &changes.insert {
                    cursor.put(key, value).await?
                }
            }
        }

        let sequence_increment = self.sequence_increment.into_inner().unwrap();
        for (table_name, increment) in sequence_increment {
            if increment > 0 {
                let table = CustomTable { 0: table_name };
                self.parent.increment_sequence(&table, increment).await?;
            }
        }

        Ok(())
    }

    async fn increment_sequence<T>(&self, table: &T, amount: u64) -> Result<u64>
    where
        T: Table,
    {
        let mut sequence_increment = self.sequence_increment.lock().unwrap();
        let name = table.db_name();
        let increment = *(*sequence_increment).get(&name).unwrap_or(&0);
        (*sequence_increment).insert(name, increment + amount);
        Ok(increment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::traits::Transaction;
    use mdbx::{DatabaseFlags, Environment, NoWriteMap, WriteFlags};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_mutation() {
        let dir = tempdir().unwrap();
        let env = Environment::<NoWriteMap>::new()
            .set_max_dbs(2)
            .open(dir.path())
            .unwrap();
        let mut tx = env.begin_rw_txn().unwrap();
        let table = CustomTable::from("TxSender".to_string());
        {
            let db = tx
                .create_db(Some("TxSender"), DatabaseFlags::default())
                .unwrap();
            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("xxx"),
                Default::default(),
            )
            .unwrap();
            tx.put(
                &db,
                Bytes::from("c"),
                Bytes::from("zzz"),
                Default::default(),
            )
            .unwrap();
            tx.put(
                &db,
                Bytes::from("b"),
                Bytes::from("yyy"),
                Default::default(),
            )
            .unwrap();
        }
        {
            let ref_tx = &mut tx;
            let mutation = Mutation::new(ref_tx);
            mutation
                .set(&table, &Bytes::from("a1"), &Bytes::from("aaa"))
                .await
                .unwrap();
            mutation
                .set(&table, &Bytes::from("c"), &Bytes::from("bbb"))
                .await
                .unwrap();
            mutation
                .delete_key(&table, &Bytes::from("b"))
                .await
                .unwrap();
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("xxx")
            );
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a1"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("aaa")
            );
            assert!(mutation
                .get(&table, &Bytes::from("b"))
                .await
                .unwrap()
                .is_none());
            mutation.commit().await.unwrap();
        }
        {
            let db = tx.open_db(Some("TxSender")).unwrap();
            assert_eq!(
                tx.get(&db, &Bytes::from("a")).unwrap().unwrap(),
                Bytes::from("xxx")
            );
            assert_eq!(
                tx.get(&db, &Bytes::from("a1")).unwrap().unwrap(),
                Bytes::from("aaa")
            );
            assert!(tx.get(&db, &Bytes::from("b")).unwrap().is_none());
            assert_eq!(
                tx.get(&db, &Bytes::from("c")).unwrap().unwrap(),
                Bytes::from("bbb")
            );
        }
    }

    #[tokio::test]
    async fn test_mutation_dupsort() {
        let dir = tempdir().unwrap();
        let env = Environment::<NoWriteMap>::new()
            .set_max_dbs(2)
            .open(dir.path())
            .unwrap();
        let mut tx = env.begin_rw_txn().unwrap();
        let table_name = "AccountChangeSet".to_string();
        assert!(is_dupsort(&table_name));
        let table = CustomTable::from(table_name.clone());
        let flags = DatabaseFlags::default() | DatabaseFlags::DUP_SORT;
        let db = tx.create_db(Some(&table_name), flags).unwrap();

        tx.put(
            &db,
            Bytes::from("a"),
            Bytes::from("z"),
            WriteFlags::default(),
        )
        .unwrap();
        {
            let ref_tx = &mut tx;
            let mutation = Mutation::new(ref_tx);
            mutation
                .set(&table, &Bytes::from("a"), &Bytes::from("y"))
                .await
                .unwrap();
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("y")
            );
            mutation
                .delete_pair(&table, &Bytes::from("a"), &Bytes::from("z"))
                .await
                .unwrap();
            assert!(mutation
                .get(&table, &Bytes::from("b"))
                .await
                .unwrap()
                .is_none());
            mutation
                .set(&table, &Bytes::from("b"), &Bytes::from("x"))
                .await
                .unwrap();
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("b"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("x")
            );
            mutation
                .delete_pair(&table, &Bytes::from("b"), &Bytes::from("x"))
                .await
                .unwrap();
            assert!(mutation
                .get(&table, &Bytes::from("b"))
                .await
                .unwrap()
                .is_none());
            mutation.commit().await.unwrap();
        }

        {
            let db = tx.open_db(Some(&table_name)).unwrap();
            assert_eq!(
                tx.get(&db, &Bytes::from("a")).unwrap().unwrap(),
                Bytes::from("y")
            );
            assert!(tx.get(&db, &Bytes::from("b")).unwrap().is_none());

            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("z"),
                WriteFlags::default(),
            )
            .unwrap();
            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("x"),
                WriteFlags::default(),
            )
            .unwrap();
            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("y"),
                WriteFlags::default(),
            )
            .unwrap();
        }

        {
            let ref_tx = &mut tx;
            let mutation = Mutation::new(ref_tx);
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("x")
            );
            mutation
                .delete_key(&table, &Bytes::from("a"))
                .await
                .unwrap();
            assert!(mutation
                .get(&table, &Bytes::from("a"))
                .await
                .unwrap()
                .is_none());
            mutation.commit().await.unwrap();
        }

        {
            let db = tx.open_db(Some(&table_name)).unwrap();
            assert!(tx.get(&db, &Bytes::from("a")).unwrap().is_none());

            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("z"),
                WriteFlags::default(),
            )
            .unwrap();
            tx.put(
                &db,
                Bytes::from("a"),
                Bytes::from("y"),
                WriteFlags::default(),
            )
            .unwrap();
        }

        {
            let ref_tx = &mut tx;
            let mutation = Mutation::new(ref_tx);
            mutation
                .delete_pair(&table, &Bytes::from("a"), &Bytes::from("z"))
                .await
                .unwrap();
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("y")
            );
            mutation
                .delete_key(&table, &Bytes::from("a"))
                .await
                .unwrap();
            mutation
                .set(&table, &Bytes::from("a"), &Bytes::from("w"))
                .await
                .unwrap();
            mutation
                .set(&table, &Bytes::from("a"), &Bytes::from("x"))
                .await
                .unwrap();
            assert_eq!(
                mutation
                    .get(&table, &Bytes::from("a"))
                    .await
                    .unwrap()
                    .unwrap(),
                Bytes::from("w")
            );
            mutation.commit().await.unwrap();
        }

        {
            let db = tx.open_db(Some(&table_name)).unwrap();
            assert_eq!(
                tx.get(&db, &Bytes::from("a")).unwrap().unwrap(),
                Bytes::from("w")
            );
        }
    }

    #[tokio::test]
    async fn test_mutation_sequence() {
        let dir = tempdir().unwrap();
        let env = Environment::<NoWriteMap>::new()
            .set_max_dbs(2)
            .open(dir.path())
            .unwrap();
        let mut tx = env.begin_rw_txn().unwrap();
        let table1 = CustomTable::from("table1".to_string());
        let table2 = CustomTable::from("table2".to_string());
        let _ = tx.create_db(Some("table1"), DatabaseFlags::default());
        let _ = tx.create_db(Some("table2"), DatabaseFlags::default());
        tx.increment_sequence(&table1, 10).await.unwrap();
        {
            let ref_tx = &mut tx;
            let mutation = Mutation::new(ref_tx);
            assert_eq!(mutation.read_sequence(&table1).await.unwrap(), 10);
            assert_eq!(mutation.read_sequence(&table2).await.unwrap(), 0);
            mutation.increment_sequence(&table1, 5).await.unwrap();
            mutation.increment_sequence(&table2, 5).await.unwrap();
            assert_eq!(mutation.read_sequence(&table1).await.unwrap(), 15);
            assert_eq!(mutation.read_sequence(&table2).await.unwrap(), 5);
            mutation.commit().await.unwrap();
        }
        assert_eq!(tx.read_sequence(&table1).await.unwrap(), 15);
        assert_eq!(tx.read_sequence(&table2).await.unwrap(), 5);
    }
}
