use super::stage::*;
use crate::{
    kv::{mdbx::*, traits::*},
    models::*,
};

pub fn unwind_by_block_key<'db: 'tx, 'tx, T, F, E>(
    tx: &'tx mut MdbxTransaction<'db, RW, E>,
    table: T,
    input: UnwindInput,
    block_key_extractor: F,
) -> anyhow::Result<()>
where
    T: Table,
    T::Key: TableDecode,
    F: Fn(T::Key) -> BlockNumber,
    E: EnvironmentKind,
{
    let mut cur = tx.cursor(table)?;
    let mut e = cur.last()?;
    while let Some(block_num) = e.map(|(k, _)| (block_key_extractor)(k)) {
        if block_num <= input.unwind_to {
            break;
        }

        cur.delete_current()?;

        e = cur.prev()?;
    }

    Ok(())
}

pub fn unwind_by_block_key_duplicates<'db: 'tx, 'tx, T, F, E>(
    tx: &'tx mut MdbxTransaction<'db, RW, E>,
    table: T,
    input: UnwindInput,
    block_key_extractor: F,
) -> anyhow::Result<()>
where
    T: DupSort,
    T::Key: TableDecode,
    F: Fn(T::Key) -> BlockNumber,
    E: EnvironmentKind,
{
    let mut cur = tx.cursor(table)?;
    let mut e = cur.last()?;
    while let Some(block_num) = e.map(|(k, _)| (block_key_extractor)(k)) {
        if block_num <= input.unwind_to {
            break;
        }

        cur.delete_current_duplicates()?;

        e = cur.prev_no_dup()?;
    }

    Ok(())
}

pub fn prune_by_block_key<'db: 'tx, 'tx, T, F, E>(
    tx: &'tx mut MdbxTransaction<'db, RW, E>,
    table: T,
    input: PruningInput,
    block_key_extractor: F,
) -> anyhow::Result<()>
where
    T: Table,
    T::Key: TableDecode,
    F: Fn(T::Key) -> BlockNumber,
    E: EnvironmentKind,
{
    let mut cur = tx.cursor(table)?;
    let mut e = cur.first()?;
    while let Some(block_num) = e.map(|(k, _)| (block_key_extractor)(k)) {
        if block_num >= input.prune_to {
            break;
        }

        cur.delete_current()?;

        e = cur.next()?;
    }

    Ok(())
}

pub fn prune_by_block_key_duplicates<'db: 'tx, 'tx, T, F, E>(
    tx: &'tx mut MdbxTransaction<'db, RW, E>,
    table: T,
    input: PruningInput,
    block_key_extractor: F,
) -> anyhow::Result<()>
where
    T: DupSort,
    T::Key: TableDecode,
    F: Fn(T::Key) -> BlockNumber,
    E: EnvironmentKind,
{
    let mut cur = tx.cursor(table)?;
    let mut e = cur.first()?;
    while let Some(block_num) = e.map(|(k, _)| (block_key_extractor)(k)) {
        if block_num >= input.prune_to {
            break;
        }

        cur.delete_current_duplicates()?;

        e = cur.next_no_dup()?;
    }

    Ok(())
}
