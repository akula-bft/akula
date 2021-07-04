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
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    marker::PhantomData,
};

type TableName = string::String<static_bytes::Bytes>;
type SimpleBucket = BTreeMap<Vec<u8>, Option<Vec<u8>>>;
type SimpleBuffer = HashMap<TableName, SimpleBucket>;
type DupSortValues = BTreeSet<Vec<u8>>;

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
    simple_buffer: HashMap<TableName, SimpleBucket>,
    dupsort_buffer: HashMap<TableName, DupSortBucket>,
    sequence_increment: HashMap<TableName, u64>,
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

        if let Some(bucket) = self.simple_buffer.get(&table_name) {
            match bucket.get(key) {
                Some(entry) => Ok(entry.as_ref().map(|v| v.as_slice().into())),
                None => self.parent.get(table, key).await,
            }
        } else if let Some(bucket) = self.dupsort_buffer.get(&table_name) {
            match bucket.get(key) {
                Some(entry) => Ok({
                    let mut first = entry.insert.first().map(|v| Bytes::from(v.clone()));

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
        let parent_value = self.parent.read_sequence(table).await?;
        let increment = self.sequence_increment.get(&table.db_name()).unwrap_or(&0);

        Ok(parent_value + increment)
    }

    fn get_simple_buffer(&mut self, name: &TableName) -> &mut SimpleBucket {
        if !self.simple_buffer.contains_key(name) {
            self.simple_buffer.insert(name.clone(), Default::default());
        }
        self.simple_buffer.get_mut(name).unwrap()
    }

    fn get_dupsort_buffer(&mut self, name: &TableName) -> &mut DupSortBucket {
        if !self.dupsort_buffer.contains_key(name) {
            self.dupsort_buffer.insert(name.clone(), Default::default());
        }
        self.dupsort_buffer.get_mut(name).unwrap()
    }

    fn get_dupsort_changes(&mut self, name: &TableName, k: &[u8]) -> &mut DupSortChanges {
        let buffer = self.get_dupsort_buffer(name);
        if !buffer.contains_key(k) {
            buffer.insert(k.to_vec(), Default::default());
        }
        buffer.get_mut(k).unwrap()
    }

    async fn set<T>(&mut self, table: &T, k: &[u8], v: &[u8]) -> Result<()>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if is_dupsort(&table_name) {
            let changes = self.get_dupsort_changes(&table_name, k);
            changes.insert.insert(v.to_vec());
            changes.delete.remove(v);
        } else {
            self.get_simple_buffer(&table_name)
                .insert(k.to_vec(), Some(v.to_vec()));
        }

        Ok(())
    }

    async fn delete_key<T>(&mut self, table: &T, k: &[u8]) -> Result<()>
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
                    parent_values.insert(parent_value.to_vec());
                    found = parent_cursor.next().await?.map(|p| (p.0 == k, p.1));
                }
            }

            let changes = self.get_dupsort_changes(&table_name, k);
            changes.insert.clear();
            changes.delete = parent_values;
        } else {
            self.get_simple_buffer(&table_name).insert(k.to_vec(), None);
        }

        Ok(())
    }

    async fn delete_pair<T>(&mut self, table: &T, k: &[u8], v: &[u8]) -> Result<()>
    where
        T: Table,
    {
        let table_name = table.db_name();

        if is_dupsort(&table_name) {
            let changes = self.get_dupsort_changes(&table_name, k);
            changes.insert.remove(v);
            changes.delete.insert(v.to_vec());
        } else {
            let bucket = self.get_simple_buffer(&table_name);
            if let Some(Some(value)) = bucket.get(k) {
                if value == v {
                    bucket.insert(k.to_vec(), None);
                }
            }
        }

        Ok(())
    }

    async fn commit(self) -> anyhow::Result<()> {
        for (table_name, bucket) in self.simple_buffer {
            let table = CustomTable { 0: table_name };
            let mut cursor = self.parent.mutable_cursor(&table).await?;
            for (ref key, ref maybe_value) in bucket {
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

        for (table_name, bucket) in self.dupsort_buffer {
            let table = CustomTable { 0: table_name };
            let mut cursor = self.parent.mutable_cursor(&table).await?;
            for (ref key, ref changes) in bucket {
                for value in &changes.delete {
                    cursor.delete(key, value).await?
                }
                for value in &changes.insert {
                    cursor.put(key, value).await?
                }
            }
        }

        for (table_name, increment) in self.sequence_increment {
            if increment > 0 {
                let table = CustomTable { 0: table_name };
                self.parent.increment_sequence(&table, increment).await?;
            }
        }

        Ok(())
    }

    async fn increment_sequence<T>(&mut self, table: &T, amount: u64) -> Result<u64>
    where
        T: Table,
    {
        let parent_value = self.parent.read_sequence(table).await?;
        let name = table.db_name();
        let increment = self.sequence_increment.get(&name).unwrap_or(&0);
        let current = parent_value + increment;
        self.sequence_increment.insert(name, current + amount);
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let mut mutation = Mutation::new(ref_tx);
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
            let mut mutation = Mutation::new(ref_tx);
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
            let mut mutation = Mutation::new(ref_tx);
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
            let mut mutation = Mutation::new(ref_tx);
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
}
