use crate::{
    etl::collector::*,
    kv::{mdbx::*, tables},
    models::*,
    stagedsync::{stage::*, stages::*},
    StageId,
};
use async_trait::async_trait;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::pin;
use tracing::*;

/// Generation of TransactionHash => BlockNumber mapping
#[derive(Debug)]
pub struct TxLookup {
    pub temp_dir: Arc<TempDir>,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for TxLookup
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        TX_LOOKUP
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let mut cursor = tx.cursor(tables::BlockTransactionLookup.erased())?;

        let mut collector = TableCollector::new(&*self.temp_dir, OPTIMAL_BUFFER_CAPACITY);
        let mut highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let start_block = highest_block + 1;

        let walker_block_body = tx.cursor(tables::BlockBody)?.walk(Some(start_block));
        pin!(walker_block_body);

        while let Some((
            (block_number, _),
            BodyForStorage {
                base_tx_id,
                tx_amount,
                ..
            },
        )) = walker_block_body.next().transpose()?
        {
            let walker_block_txs = tx
                .cursor(tables::BlockTransaction)?
                .walk(Some(base_tx_id))
                .take(tx_amount.try_into()?);
            pin!(walker_block_txs);

            while let Some((_, tx)) = walker_block_txs.next().transpose()? {
                collector.push(tx.hash(), tables::TruncateStart(block_number));
            }

            highest_block = block_number;
        }

        collector.load(&mut cursor)?;
        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let bodies_cursor = tx.cursor(tables::BlockBody)?;
        let mut tx_hash_cursor = tx.cursor(tables::BlockTransactionLookup)?;

        let start_block_number = input.unwind_to + 1;

        info!(
            "Started Tx Lookup Unwind, from: {} to: {}",
            input.stage_progress, input.unwind_to
        );

        let walker_block_body = bodies_cursor.walk(Some(start_block_number));
        pin!(walker_block_body);

        while let Some((
            _,
            BodyForStorage {
                base_tx_id,
                tx_amount,
                ..
            },
        )) = walker_block_body.next().transpose()?
        {
            let walker_block_txs = tx.cursor(tables::BlockTransaction)?.walk(Some(base_tx_id));
            pin!(walker_block_txs);

            let mut num_txs = 1;

            while let Some((_, tx_value)) = walker_block_txs.next().transpose()? {
                if num_txs > tx_amount {
                    break;
                }

                if tx_hash_cursor.seek(tx_value.hash())?.is_some() {
                    tx_hash_cursor.delete_current()?;
                }
                num_txs += 1;
            }
        }
        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let bodies_cursor = tx.cursor(tables::BlockBody)?;
        let mut tx_hash_cursor = tx.cursor(tables::BlockTransactionLookup)?;

        let walker_block_body = bodies_cursor.walk(None);
        pin!(walker_block_body);

        while let Some((
            (block_number, _),
            BodyForStorage {
                base_tx_id,
                tx_amount,
                ..
            },
        )) = walker_block_body.next().transpose()?
        {
            if block_number >= input.prune_to {
                break;
            }

            let walker_block_txs = tx.cursor(tables::BlockTransaction)?.walk(Some(base_tx_id));
            pin!(walker_block_txs);

            let mut num_txs = 1;

            while let Some((_, tx_value)) = walker_block_txs.next().transpose()? {
                if num_txs > tx_amount {
                    break;
                }

                if tx_hash_cursor.seek(tx_value.hash())?.is_some() {
                    tx_hash_cursor.delete_current()?;
                }
                num_txs += 1;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{accessors::chain, kv::new_mem_database};
    use bytes::Bytes;
    use hex_literal::hex;
    use std::time::Instant;

    const CHAIN_ID: Option<ChainId> = Some(ChainId(1));

    #[tokio::test]
    async fn tx_lookup_stage_with_data() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().unwrap();

        let recipient1 = Address::from(hex!("f4148309cc30f2dd4ba117122cad6be1e3ba0e2b"));
        let recipient2 = Address::from(hex!("d7fa8303df7073290f66ced1add5fe89dac0c462"));

        let block1 = BodyForStorage {
            base_tx_id: 1.into(),
            tx_amount: 2,
            uncles: vec![],
        };

        let tx1_1 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 1,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 1.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                false,
                H256::from(hex!(
                    "11d244ae19e3bb96d1bb864aa761d48e957984a154329f0de757cd105f9c7ac4"
                )),
                H256::from(hex!(
                    "0e3828d13eed24036941eb5f7fd65de57aad1184342f2244130d2941554342ba"
                )),
            )
            .unwrap(),
        };
        let hash1_1 = tx1_1.hash();

        let tx1_2 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 2,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x100.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "9e8c555909921d359bfb0c2734841c87691eb257cb5f0597ac47501abd8ba0de"
                )),
                H256::from(hex!(
                    "7bfd0f8a11568ba2abc3ab4d2df6cb013359316704a3bd7ebd14bca5caf12b57"
                )),
            )
            .unwrap(),
        };
        let hash1_2 = tx1_2.hash();

        let block2 = BodyForStorage {
            base_tx_id: 3.into(),
            tx_amount: 3,
            uncles: vec![],
        };

        let tx2_1 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 3,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10000.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "2450fdbf8fbc1dee15022bfa7392eb15f04277782343258e185972b5b2b8bf79"
                )),
                H256::from(hex!(
                    "0f556dc665406344c3f456d44a99d2a4ab70c68dce114e78d90bfd6d11287c07"
                )),
            )
            .unwrap(),
        };

        let hash2_1 = tx2_1.hash();

        let tx2_2 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 6,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                false,
                H256::from(hex!(
                    "ac0222c1258eada1f828729186b723eaf3dd7f535c5de7271ea02470cbb1029f"
                )),
                H256::from(hex!(
                    "3c6b5f961c19a134f75a0924264558d6e551f0476e1fdd431a88b52d9b4ac1e6"
                )),
            )
            .unwrap(),
        };

        let hash2_2 = tx2_2.hash();

        let tx2_3 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 2,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient2),
                value: 2.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "e41df92d64612590f72cae9e8895cd34ce0a545109f060879add106336bb5055"
                )),
                H256::from(hex!(
                    "4facd92af3fa436977834ba92287bee667f539b78a5cfc58ba8d5bf30c5a77b7"
                )),
            )
            .unwrap(),
        };

        let hash2_3 = tx2_3.hash();

        let block3 = BodyForStorage {
            base_tx_id: 6.into(),
            tx_amount: 0,
            uncles: vec![],
        };

        let hash1 = H256::random();
        let hash2 = H256::random();
        let hash3 = H256::random();

        chain::storage_body::write(&tx, hash1, 1, &block1).unwrap();
        chain::storage_body::write(&tx, hash2, 2, &block2).unwrap();
        chain::storage_body::write(&tx, hash3, 3, &block3).unwrap();

        chain::tx::write(&tx, block1.base_tx_id, &[tx1_1, tx1_2]).unwrap();
        chain::tx::write(&tx, block2.base_tx_id, &[tx2_1, tx2_2, tx2_3]).unwrap();

        let mut stage = TxLookup {
            temp_dir: Arc::new(TempDir::new().unwrap()),
        };

        let stage_input = StageInput {
            restarted: false,
            first_started_at: (Instant::now(), Some(BlockNumber(0))),
            previous_stage: Some((BODIES, 3.into())),
            stage_progress: Some(0.into()),
        };

        let output: ExecOutput = stage.execute(&mut tx, stage_input).await.unwrap();

        assert_eq!(
            output,
            ExecOutput::Progress {
                stage_progress: 3.into(),
                done: true,
            }
        );

        for (hashed_tx, block_number) in [
            (hash1_1, 1),
            (hash1_2, 1),
            (hash2_1, 2),
            (hash2_2, 2),
            (hash2_3, 2),
        ] {
            assert_eq!(
                dbg!(chain::tl::read(&tx, hashed_tx).unwrap().unwrap()),
                block_number.into()
            );
        }
    }

    #[tokio::test]
    async fn tx_lookup_stage_without_data() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().unwrap();
        let mut stage = TxLookup {
            temp_dir: Arc::new(TempDir::new().unwrap()),
        };

        let stage_input = StageInput {
            restarted: false,
            first_started_at: (Instant::now(), Some(BlockNumber(0))),
            previous_stage: Some((BODIES, 3.into())),
            stage_progress: Some(0.into()),
        };

        let output: ExecOutput = stage.execute(&mut tx, stage_input).await.unwrap();

        assert_eq!(
            output,
            ExecOutput::Progress {
                stage_progress: 0.into(),
                done: true,
            }
        );
    }

    #[tokio::test]
    async fn tx_lookup_unwind() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().unwrap();

        let recipient1 = Address::from(hex!("f4148309cc30f2dd4ba117122cad6be1e3ba0e2b"));
        let recipient2 = Address::from(hex!("d7fa8303df7073290f66ced1add5fe89dac0c462"));

        let block1 = BodyForStorage {
            base_tx_id: 1.into(),
            tx_amount: 2,
            uncles: vec![],
        };

        let tx1_1 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 1,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 1.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                false,
                H256::from(hex!(
                    "11d244ae19e3bb96d1bb864aa761d48e957984a154329f0de757cd105f9c7ac4"
                )),
                H256::from(hex!(
                    "0e3828d13eed24036941eb5f7fd65de57aad1184342f2244130d2941554342ba"
                )),
            )
            .unwrap(),
        };
        let hash1_1 = tx1_1.hash();

        let tx1_2 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 2,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x100.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "9e8c555909921d359bfb0c2734841c87691eb257cb5f0597ac47501abd8ba0de"
                )),
                H256::from(hex!(
                    "7bfd0f8a11568ba2abc3ab4d2df6cb013359316704a3bd7ebd14bca5caf12b57"
                )),
            )
            .unwrap(),
        };
        let hash1_2 = tx1_2.hash();

        let block2 = BodyForStorage {
            base_tx_id: 3.into(),
            tx_amount: 3,
            uncles: vec![],
        };

        let tx2_1 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 3,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10000.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "2450fdbf8fbc1dee15022bfa7392eb15f04277782343258e185972b5b2b8bf79"
                )),
                H256::from(hex!(
                    "0f556dc665406344c3f456d44a99d2a4ab70c68dce114e78d90bfd6d11287c07"
                )),
            )
            .unwrap(),
        };

        let hash2_1 = tx2_1.hash();

        let tx2_2 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 6,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                false,
                H256::from(hex!(
                    "ac0222c1258eada1f828729186b723eaf3dd7f535c5de7271ea02470cbb1029f"
                )),
                H256::from(hex!(
                    "3c6b5f961c19a134f75a0924264558d6e551f0476e1fdd431a88b52d9b4ac1e6"
                )),
            )
            .unwrap(),
        };

        let hash2_2 = tx2_2.hash();

        let tx2_3 = MessageWithSignature {
            message: Message::Legacy {
                chain_id: CHAIN_ID,
                nonce: 2,
                gas_price: 1_000_000.as_u256(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient2),
                value: 2.as_u256(),
                input: Bytes::new(),
            },
            signature: MessageSignature::new(
                true,
                H256::from(hex!(
                    "e41df92d64612590f72cae9e8895cd34ce0a545109f060879add106336bb5055"
                )),
                H256::from(hex!(
                    "4facd92af3fa436977834ba92287bee667f539b78a5cfc58ba8d5bf30c5a77b7"
                )),
            )
            .unwrap(),
        };

        let hash2_3 = tx2_3.hash();

        let block3 = BodyForStorage {
            base_tx_id: 6.into(),
            tx_amount: 0,
            uncles: vec![],
        };

        let hash1 = H256::random();
        let hash2 = H256::random();
        let hash3 = H256::random();

        chain::storage_body::write(&tx, hash1, 1, &block1).unwrap();
        chain::storage_body::write(&tx, hash2, 2, &block2).unwrap();
        chain::storage_body::write(&tx, hash3, 3, &block3).unwrap();

        chain::tx::write(&tx, block1.base_tx_id, &[tx1_1, tx1_2]).unwrap();
        chain::tx::write(&tx, block2.base_tx_id, &[tx2_1, tx2_2, tx2_3]).unwrap();

        chain::tl::write(&tx, hash1_1, 1.into()).unwrap();
        chain::tl::write(&tx, hash1_2, 1.into()).unwrap();
        chain::tl::write(&tx, hash2_1, 2.into()).unwrap();
        chain::tl::write(&tx, hash2_2, 2.into()).unwrap();
        chain::tl::write(&tx, hash2_3, 2.into()).unwrap();
        let mut stage = TxLookup {
            temp_dir: Arc::new(TempDir::new().unwrap()),
        };
        stage
            .unwind(
                &mut tx,
                UnwindInput {
                    stage_progress: 2.into(),
                    unwind_to: 1.into(),
                },
            )
            .await
            .unwrap();

        tx.commit().unwrap();
        let tx = db.begin_mutable().unwrap();

        for (hashed_tx, block_number) in [
            (hash1_1, Some(1.into())),
            (hash1_2, Some(1.into())),
            (hash2_1, None),
            (hash2_2, None),
            (hash2_3, None),
        ] {
            assert_eq!(dbg!(chain::tl::read(&tx, hashed_tx).unwrap()), block_number);
        }
    }
}
