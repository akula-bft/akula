use std::collections::BTreeMap;
use std::sync::Arc;
use mdbx::{EnvironmentKind};
use crate::kv::{MdbxWithDirHandle, tables};

const DATABASE_VERSION: u64 = 1;

type Migration<E> = fn(&MdbxWithDirHandle<E>) -> anyhow::Result<u64>;

fn init_database_version<E>(_env: &MdbxWithDirHandle<E>) -> anyhow::Result<u64>
    where E: EnvironmentKind
{
    Ok(DATABASE_VERSION)
}

fn set_database_version<E>(db: &MdbxWithDirHandle<E>, version: u64) -> anyhow::Result<()>
    where E: EnvironmentKind
{
    let txn = db.begin_mutable()?;
    txn.set(tables::Version, (), version)?;
    txn.commit()
}

fn get_database_version<E>(db: &MdbxWithDirHandle<E>) -> anyhow::Result<u64>
    where E: EnvironmentKind
{
    let txn = db.begin()?;
    let version = txn.get(tables::Version, ())?.unwrap_or(0);
    Ok(version)
}

fn apply_migrations<E>(
    db: Arc<MdbxWithDirHandle<E>>,
    from_version: u64,
    migrations: BTreeMap<u64, Migration<E>>,
) where E: EnvironmentKind
{
    let mut current_version = from_version;
    while current_version < DATABASE_VERSION {
        if let Some(migration) = migrations.get(&current_version) {
            match migration(&db) {
                Ok(new_version) => {
                    current_version = new_version;
                    set_database_version(&db, new_version).unwrap();
                }
                Err(err) => panic!("Failed database migration: {}", err)
            }
        } else {
            panic!("No migration from database version {}, please re-sync", current_version)
        }
    }
}

pub fn migrate_database<E>(db: Arc<MdbxWithDirHandle<E>>)
    where E: EnvironmentKind
{
    let current_version = get_database_version(&db).unwrap_or(0);

    if current_version > DATABASE_VERSION {
        panic!("Database version {} too high. Newest version {}", current_version, DATABASE_VERSION)
    }

    let migrations: BTreeMap<u64, Migration<E>> = BTreeMap::from(
        [(0, init_database_version as Migration<E>); 1],
    );

    apply_migrations(db, current_version, migrations);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_chaindata;

    #[test]
    fn test_migrate_database_new() {
        let db = Arc::new(new_mem_chaindata().unwrap());
        migrate_database(db.clone());
        let version = get_database_version(&db);
        assert_eq!(DATABASE_VERSION, version.unwrap());
    }

    #[test]
    #[should_panic(expected = "Database version 18446744073709551615 too high. Newest version")]
    fn test_migrate_database_too_high() {
        let db = Arc::new(new_mem_chaindata().unwrap());
        set_database_version(&db, u64::MAX).unwrap();
        migrate_database(db.clone());
    }

    #[test]
    #[should_panic(expected = "No migration from database version 0, please re-sync")]
    fn test_apply_migrations() {
        let db = Arc::new(new_mem_chaindata().unwrap());
        apply_migrations(db.clone(), 0, BTreeMap::new());
    }
}
