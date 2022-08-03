use std::collections::{HashMap};
use std::sync::Arc;
use mdbx::{EnvironmentKind};
use crate::kv::{MdbxWithDirHandle, tables};

const DATABASE_VERSION: u64 = 1;

fn init_database_version<E: EnvironmentKind>(env: &MdbxWithDirHandle<E>) -> anyhow::Result<()> {
    let txn = env.begin_mutable()?;
    txn.set(tables::Version, (), DATABASE_VERSION)?;
    txn.commit()
}

fn get_datbase_version<E>(
    env: &MdbxWithDirHandle<E>,
) -> anyhow::Result<u64>
    where E: EnvironmentKind,
{
    let txn = env.begin()?;
    let version = match txn.get(tables::Version, ())? {
        Some(v) => v,
        None => 0,
    };
    Ok(version)
}

pub fn migrate_database<E>(
    env: Arc<MdbxWithDirHandle<E>>,
) where E: EnvironmentKind,
{
    let mut current_version;
    if let Ok(version) = get_datbase_version(&env) {
        current_version = version;
    } else {
        current_version = 0;
    }

    let migrations = HashMap::from(
        [(0, init_database_version); 1],
    );

    while current_version < DATABASE_VERSION {
        match migrations.get(&current_version) {
            Some(migration) => match migration(&env) {
                Ok(_) => current_version += 1,
                Err(err) => panic!("Failed database migration: {}", err)

            },
            None => panic!("No migration from database version {}, please re-sync", current_version)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_chaindata;

    #[test]
    fn test_migrate_database() {
        let db = Arc::new(new_mem_chaindata().unwrap());
        migrate_database(db.clone());
        let version = get_datbase_version(&db);
        assert_eq!(DATABASE_VERSION, version.unwrap());
    }
}
