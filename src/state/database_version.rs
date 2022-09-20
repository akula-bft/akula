use crate::kv::{mdbx::*, tables, MdbxWithDirHandle};
use anyhow::format_err;
use std::collections::BTreeMap;
use thiserror::Error;

const DATABASE_VERSION: u64 = 3;

type Migration<'db, E> = fn(&mut MdbxTransaction<'db, RW, E>) -> anyhow::Result<u64>;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("No migration from database version {current}, please re-sync")]
    DbTooOld { current: u64 },
    #[error("Database version {current} too high. Newest version {newest}")]
    DbTooNew { current: u64, newest: u64 },
    #[error("Migration error")]
    MigrationError(#[from] anyhow::Error),
}

fn init_database_version<E>(_: &mut MdbxTransaction<'_, RW, E>) -> anyhow::Result<u64>
where
    E: EnvironmentKind,
{
    Ok(DATABASE_VERSION)
}

fn set_database_version<E>(txn: &MdbxTransaction<'_, RW, E>, version: u64) -> anyhow::Result<()>
where
    E: EnvironmentKind,
{
    txn.set(tables::Version, (), version)?;
    Ok(())
}

fn get_database_version<K, E>(txn: &MdbxTransaction<'_, K, E>) -> anyhow::Result<u64>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    let version = txn.get(tables::Version, ())?.unwrap_or(0);
    Ok(version)
}

fn apply_migrations<'db, E>(
    tx: &mut MdbxTransaction<'db, RW, E>,
    from_version: u64,
    migrations: BTreeMap<u64, Migration<'db, E>>,
) -> Result<(), MigrationError>
where
    E: EnvironmentKind,
{
    let mut current_version = from_version;
    while current_version < DATABASE_VERSION {
        if let Some(migration) = migrations.get(&current_version) {
            let new_version =
                migration(tx).map_err(|e| format_err!("Failed database migration: {e}"))?;
            current_version = new_version;
            set_database_version(tx, new_version)?;
        } else {
            return Err(MigrationError::DbTooOld {
                current: current_version,
            });
        }
    }

    Ok(())
}

pub fn migrate_database<E>(db: &MdbxWithDirHandle<E>) -> Result<(), MigrationError>
where
    E: EnvironmentKind,
{
    let mut tx = db.begin_mutable()?;
    let current_version = get_database_version(&tx).unwrap_or(0);

    if current_version > DATABASE_VERSION {
        return Err(MigrationError::DbTooNew {
            current: current_version,
            newest: DATABASE_VERSION,
        });
    }

    let migrations: BTreeMap<u64, Migration<E>> =
        BTreeMap::from([(0, init_database_version as Migration<E>); 1]);

    apply_migrations(&mut tx, current_version, migrations)?;
    tx.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_chaindata;
    use std::assert_matches::assert_matches;

    #[test]
    fn test_migrate_database_new() {
        let db = new_mem_chaindata().unwrap();
        migrate_database(&db).unwrap();
        let version = get_database_version(&db.begin().unwrap());
        assert_eq!(DATABASE_VERSION, version.unwrap());
    }

    #[test]
    fn test_migrate_database_too_high() {
        let db = new_mem_chaindata().unwrap();
        {
            let txn = db.begin_mutable().unwrap();
            set_database_version(&txn, u64::MAX).unwrap();
            txn.commit().unwrap();
        }
        assert_matches!(
            migrate_database(&db),
            Err(MigrationError::DbTooNew {
                current,
                newest
            }) if current == u64::MAX && newest == DATABASE_VERSION
        );
    }

    #[test]
    fn test_apply_migrations() {
        let db = new_mem_chaindata().unwrap();
        assert_matches!(
            apply_migrations(&mut db.begin_mutable().unwrap(), 0, BTreeMap::new()),
            Err(MigrationError::DbTooOld { current }) if current == 0
        );
    }
}
