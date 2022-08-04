use std::collections::BTreeMap;
use std::sync::Arc;
use mdbx::{EnvironmentKind};
use crate::kv::{MdbxWithDirHandle, tables};

const DATABASE_VERSION: u64 = 1;

type Migration<E> = fn(&MdbxWithDirHandle<E>) -> anyhow::Result<u64>;

fn init_database_version<E>(env: &MdbxWithDirHandle<E>) -> anyhow::Result<u64>
    where E: EnvironmentKind
{
    set_database_version(env, DATABASE_VERSION)?;
    Ok(DATABASE_VERSION)
}

fn set_database_version<E>(env: &MdbxWithDirHandle<E>, version: u64) -> anyhow::Result<()>
    where E: EnvironmentKind
{
    let txn = env.begin_mutable()?;
    txn.set(tables::Version, (), version)?;
    txn.commit()
}

fn get_database_version<E>(env: &MdbxWithDirHandle<E>) -> anyhow::Result<u64>
    where E: EnvironmentKind
{
    let txn = env.begin()?;
    let version = txn.get(tables::Version, ())?.unwrap_or(0);
    Ok(version)
}

fn apply_migrations<E>(
    env: Arc<MdbxWithDirHandle<E>>,
    from_version: u64,
    migrations: BTreeMap<u64, Migration<E>>,
) where E: EnvironmentKind
{
    let mut current_version = from_version;
    while current_version < DATABASE_VERSION {
        if let Some(migration) = migrations.get(&current_version) {
            match migration(&env) {
                Ok(new_version) => {
                    current_version = new_version
                }
                Err(err) => panic!("Failed database migration: {}", err)
            }
        } else {
            panic!("No migration from database version {}, please re-sync", current_version)
        }
    }
}

pub fn migrate_database<E>(env: Arc<MdbxWithDirHandle<E>>)
    where E: EnvironmentKind
{
    let current_version = get_database_version(&env).unwrap_or(0);

    if current_version > DATABASE_VERSION {
        panic!("Database version {} too high. Newest version {}", current_version, DATABASE_VERSION)
    }

    let migrations: BTreeMap<u64, Migration<E>> = BTreeMap::from(
        [(0, init_database_version as Migration<E>); 1],
    );

    apply_migrations(env, current_version, migrations);
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
    #[should_panic(expected = "Database version 18446744073709551615 too high. Newest version 1")]
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
