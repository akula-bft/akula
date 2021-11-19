use crate::{
    crypto::keccak256,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::tables,
    models::*,
    stagedsync::{stage::*, stages::*},
    upsert_hashed_storage_value, Cursor, CursorDupSort, MutableCursor, MutableTransaction,
};
use anyhow::*;
use async_trait::async_trait;
use ethereum_types::*;
use tokio_stream::StreamExt;
use tracing::*;

async fn promote_clean_state<'db, Tx>(txn: &Tx) -> anyhow::Result<()>
where
    Tx: MutableTransaction<'db>,
{
    let mut collector_account = Collector::<tables::HashedAccount>::new(OPTIMAL_BUFFER_CAPACITY);
    let mut collector_storage = Collector::<tables::HashedStorage>::new(OPTIMAL_BUFFER_CAPACITY);

    let mut src = txn.cursor(&tables::PlainState).await?;
    src.first().await?;
    let mut i = 0;
    let mut walker = src.walk(None);
    while let Some(fv) = walker.try_next().await? {
        match fv {
            tables::PlainStateFusedValue::Account { address, account } => {
                collector_account.collect(Entry::new(keccak256(address), account));
            }
            tables::PlainStateFusedValue::Storage {
                address,
                incarnation,
                location,
                value,
            } => collector_storage.collect(Entry::new(
                (keccak256(address), incarnation),
                (keccak256(location), value.into()),
            )),
        }

        i += 1;
        if i % 500_000 == 0 {
            info!("Converted {} entries", i);
        }
    }

    info!("Loading hashed account entries");
    let mut dst = txn.mutable_cursor(&tables::HashedAccount.erased()).await?;
    collector_account.load(&mut dst).await?;

    info!("Loading hashed storage entries");
    let mut dst = txn.mutable_cursor(&tables::HashedStorage.erased()).await?;
    collector_storage.load(&mut dst).await?;

    Ok(())
}

async fn promote_clean_code<'db, Tx>(txn: &Tx) -> anyhow::Result<()>
where
    Tx: MutableTransaction<'db>,
{
    info!("Hashing code keys");

    let mut collector = Collector::<tables::HashedCodeHash>::new(OPTIMAL_BUFFER_CAPACITY);

    let mut src = txn.cursor(&tables::PlainCodeHash).await?;
    src.first().await?;
    let mut walker = src.walk(None);
    while let Some(((address, incarnation), code_hash)) = walker.try_next().await? {
        collector.collect(Entry::new((keccak256(address), incarnation), code_hash));
    }

    let mut dst = txn.mutable_cursor(&tables::HashedCodeHash.erased()).await?;
    collector.load(&mut dst).await?;

    Ok(())
}

async fn promote_accounts<'db, Tx>(tx: &Tx, stage_progress: BlockNumber) -> anyhow::Result<()>
where
    Tx: MutableTransaction<'db>,
{
    let mut changeset_table = tx.cursor(&tables::AccountChangeSet).await?;
    let mut plainstate_table = tx.cursor_dup_sort(&tables::PlainState).await?;
    let mut target_table = tx.mutable_cursor(&tables::HashedAccount).await?;

    let starting_block = stage_progress + 1;

    let mut walker = changeset_table.walk(Some(starting_block));

    while let Some((_, tables::AccountChange { address, .. })) = walker.try_next().await? {
        let hashed_address = keccak256(address);
        if let Some(tables::PlainStateFusedValue::Account { account, .. }) = plainstate_table
            .seek_exact(tables::PlainStateKey::Account(address))
            .await?
        {
            target_table.upsert((hashed_address, account)).await?;
        } else if target_table.seek_exact(hashed_address).await?.is_some() {
            target_table.delete_current().await?;
        }
    }

    Ok(())
}

async fn promote_storage<'db, Tx>(tx: &Tx, stage_progress: BlockNumber) -> anyhow::Result<()>
where
    Tx: MutableTransaction<'db>,
{
    let mut changeset_table = tx.cursor(&tables::StorageChangeSet).await?;
    let mut plainstate_table = tx.cursor_dup_sort(&tables::PlainState).await?;
    let mut target_table = tx.mutable_cursor_dupsort(&tables::HashedStorage).await?;

    let starting_block = stage_progress + 1;

    let mut walker =
        changeset_table.walk(Some(tables::StorageChangeSeekKey::Block(starting_block)));

    while let Some((
        tables::StorageChangeKey {
            address,
            incarnation,
            ..
        },
        tables::StorageChange { location, .. },
    )) = walker.try_next().await?
    {
        let hashed_address = keccak256(address);
        let hashed_location = keccak256(location);
        let mut v = H256::zero();
        if let Some(tables::PlainStateFusedValue::Storage {
            address: found_address,
            incarnation: found_incarnation,
            location: found_location,
            value,
        }) = plainstate_table
            .seek_both_range(
                tables::PlainStateKey::Storage(address, incarnation),
                location,
            )
            .await?
        {
            if address == found_address
                && incarnation == found_incarnation
                && location == found_location
            {
                v = value;
            }
        }
        upsert_hashed_storage_value(
            &mut target_table,
            hashed_address,
            incarnation,
            hashed_location,
            v,
        )
        .await?;
    }

    Ok(())
}

async fn promote_code<'db, Tx>(tx: &Tx, stage_progress: BlockNumber) -> anyhow::Result<()>
where
    Tx: MutableTransaction<'db>,
{
    let mut changeset_table = tx.cursor(&tables::AccountChangeSet).await?;
    let mut plainstate_table = tx.cursor_dup_sort(&tables::PlainState).await?;
    let mut codehash_table = tx.cursor(&tables::PlainCodeHash).await?;
    let mut target_table = tx.mutable_cursor(&tables::HashedCodeHash).await?;

    let starting_block = stage_progress + 1;

    let mut walker = changeset_table.walk(Some(starting_block));

    while let Some((_, tables::AccountChange { address, .. })) = walker.try_next().await? {
        if let Some(plainstate_data) = plainstate_table
            .seek_exact(tables::PlainStateKey::Account(address))
            .await?
        {
            if let tables::PlainStateFusedValue::Account { address, account } = plainstate_data {
                // get incarnation
                if let Some(Account { incarnation, .. }) = Account::decode_for_storage(&account)? {
                    if incarnation.0 > 0 {
                        if let Some((_, code_hash)) =
                            codehash_table.seek_exact((address, incarnation)).await?
                        {
                            target_table
                                .upsert(((keccak256(address), incarnation), code_hash))
                                .await?;
                        }
                    }
                }
            } else {
                unreachable!()
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct HashState;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for HashState
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        HASH_STATE
    }
    /// Description of the stage.
    fn description(&self) -> &'static str {
        ""
    }
    /// Called when the stage is executed. The main logic of the stage should be here.
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let previous_stage = input
            .previous_stage
            .ok_or_else(|| anyhow!("Cannot be first stage"))?
            .1;
        let prev_progress = input.stage_progress.unwrap_or(BlockNumber(0));

        if prev_progress.0 > 0 {
            info!("Incrementally hashing accounts");
            promote_accounts(tx, prev_progress).await?;
            info!("Incrementally hashing storage");
            promote_storage(tx, prev_progress).await?;
            info!("Incrementally hashing code");
            promote_code(tx, prev_progress).await?;
        } else {
            promote_clean_state(tx).await?;
            promote_clean_code(tx).await?;
        }

        Ok(ExecOutput::Progress {
            stage_progress: previous_stage,
            done: true,
            must_commit: true,
        })
    }
    /// Called when the stage should be unwound. The unwind logic should be there.
    async fn unwind<'tx>(&self, tx: &'tx mut RwTx, input: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::{address::*, *},
        kv::traits::MutableKV,
        new_mem_database,
        res::chainspec::MAINNET,
        u256_to_h256, Buffer, State, Transaction, DEFAULT_INCARNATION,
    };
    use hex_literal::*;
    use std::time::Instant;

    #[tokio::test]
    async fn stage_hashstate() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().await.unwrap();

        let block_number = BlockNumber(1);
        let miner = hex!("5a0b54d5dc17e0aadc383d2db43b0a0d3e029c4c").into();
        let gas_limit = 100_000;

        // This contract initially sets its 0th storage to 0x2a
        // and its 1st storage to 0x01c9.
        // When called, it updates its 0th storage to the input provided.
        let contract_code = hex!("600035600055").to_vec();
        let deployment_code = hex!("602a6000556101c960015560068060166000396000f3").to_vec();

        let sender = hex!("b685342b8c54347aad148e1f22eff3eb3eb29391").into();

        let mut header = PartialHeader {
            number: block_number,
            beneficiary: miner,
            gas_limit,
            gas_used: 63_820,
            ..PartialHeader::empty()
        };

        let transaction = move |nonce, value, action, input| TransactionWithSender {
            message: TransactionMessage::Legacy {
                chain_id: None,
                nonce,
                gas_price: U256::from(20 * GIGA),
                gas_limit,
                action,
                value,
                input,
            },
            sender,
        };

        let mut body = BlockBodyWithSenders {
            transactions: vec![(transaction)(
                0,
                0.into(),
                TransactionAction::Create,
                deployment_code.into_iter().chain(contract_code).collect(),
            )],
            ommers: vec![],
        };

        let mut buffer = Buffer::new(&tx, BlockNumber(0), None);

        let sender_account = Account {
            balance: *ETHER,
            ..Account::default()
        };

        buffer
            .update_account(sender, None, Some(sender_account))
            .await
            .unwrap();

        // ---------------------------------------
        // Execute first block
        // ---------------------------------------
        execute_block(&mut buffer, &MAINNET, &header, &body)
            .await
            .unwrap();

        let contract_address = create_address(sender, 0);

        // ---------------------------------------
        // Execute second block
        // ---------------------------------------

        let new_val = hex!("000000000000000000000000000000000000000000000000000000000000003e");

        let block_number = BlockNumber(2);

        header.number = block_number;
        header.gas_used = 26_201;

        body.transactions = vec![(transaction)(
            1,
            1000.into(),
            TransactionAction::Call(contract_address),
            new_val.to_vec().into(),
        )];

        execute_block(&mut buffer, &MAINNET.clone(), &header, &body)
            .await
            .unwrap();

        // ---------------------------------------
        // Execute third block
        // ---------------------------------------

        let new_val = 0x3b;

        let block_number = BlockNumber(3);

        header.number = block_number;
        header.gas_used = 26_201;

        body.transactions = vec![(transaction)(
            2,
            1000.into(),
            TransactionAction::Call(contract_address),
            u256_to_h256(new_val.into()).0.to_vec().into(),
        )];

        execute_block(&mut buffer, &MAINNET.clone(), &header, &body)
            .await
            .unwrap();

        buffer.write_to_db().await.unwrap();

        // ---------------------------------------
        // Execute stage forward
        // ---------------------------------------
        assert_eq!(
            HashState {}
                .execute(
                    &mut tx,
                    StageInput {
                        restarted: false,
                        first_started_at: (Instant::now(), None),
                        previous_stage: Some((StageId(""), BlockNumber(3))),
                        stage_progress: None,
                    },
                )
                .await
                .unwrap(),
            ExecOutput::Progress {
                stage_progress: BlockNumber(3),
                done: true,
                must_commit: true,
            }
        );

        // ---------------------------------------
        // Check hashed account
        // ---------------------------------------

        let mut hashed_address_table = tx.cursor(&tables::HashedAccount).await.unwrap();
        let sender_keccak = keccak256(sender);
        let (_, account_encoded) = hashed_address_table
            .seek_exact(sender_keccak)
            .await
            .unwrap()
            .unwrap();
        let account = Account::decode_for_storage(&*account_encoded)
            .unwrap()
            .unwrap();
        assert_eq!(account.nonce, 3);
        assert!(account.balance < *ETHER);

        // ---------------------------------------
        // Check hashed storage
        // ---------------------------------------

        let mut hashed_storage_cursor = tx.cursor(&tables::HashedStorage).await.unwrap();
        let contract_keccak = keccak256(contract_address);

        let k = (contract_keccak, DEFAULT_INCARNATION);
        let mut walker = hashed_storage_cursor.walk(Some(k));

        for (location, expected_value) in [(0, new_val), (1, 0x01c9)] {
            let (wk, (hashed_location, value)) = walker.try_next().await.unwrap().unwrap();
            assert_eq!(k, wk);
            assert_eq!(hashed_location, keccak256(u256_to_h256(location.into())));
            assert_eq!(value.0, u256_to_h256(expected_value.into()));
        }

        assert!(walker.try_next().await.unwrap().is_none());
    }
}
