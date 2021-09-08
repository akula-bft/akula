use crate::{
    crypto::*,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::tables,
    models::BodyForStorage,
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    Cursor, MutableTransaction, StageId,
};
use async_trait::async_trait;
use ethereum_types::U64;
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

#[derive(Debug)]
pub struct TxLookup;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for TxLookup
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("TxLookup")
    }

    fn description(&self) -> &'static str {
        "Generating TransactionHash => BlockNumber Mapping"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let mut bodies_cursor = tx.mutable_cursor(&tables::BlockBody).await?;
        let mut tx_hash_cursor = tx.mutable_cursor(&tables::TxLookup).await?;

        let mut block_txs_cursor = tx.cursor(&tables::BlockTransaction).await?;

        let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

        let mut start_block_number = [0; 8];
        let (_, last_processed_block_number) = tx
            .mutable_cursor(&tables::TxLookup)
            .await?
            .last()
            .await?
            .unwrap_or((bytes::Bytes::from(&[]), bytes::Bytes::from(&[])));

        (U64::from_big_endian(last_processed_block_number.as_ref()) + 1)
            .to_big_endian(&mut start_block_number);

        let walker_block_body = bodies_cursor.walk(&start_block_number, |_, _| true);
        pin!(walker_block_body);

        while let Some((block_body_key, ref block_body_value)) =
            walker_block_body.try_next().await?
        {
            let block_number = block_body_key[..8]
                .iter()
                .cloned()
                // remove trailing zeros
                .skip_while(|x| *x == 0)
                .collect::<Vec<_>>();
            let body_rpl = rlp::decode::<BodyForStorage>(block_body_value)?;
            let (tx_count, tx_base_id) = (body_rpl.tx_amount, body_rpl.base_tx_id);
            let tx_base_id_as_bytes = tx_base_id.to_be_bytes();

            let walker_block_txs = block_txs_cursor.walk(&tx_base_id_as_bytes, |_, _| true);
            pin!(walker_block_txs);

            let mut num_txs = 1;

            while let Some((_tx_key, ref tx_value)) = walker_block_txs.try_next().await? {
                if num_txs > tx_count {
                    break;
                }

                let hashed_tx_data = keccak256(tx_value);
                collector.collect(Entry {
                    key: hashed_tx_data.as_bytes().to_vec(),
                    value: block_number.clone(),
                    id: 0, // ?
                });
                num_txs += 1;
            }
        }

        collector.load(&mut tx_hash_cursor, None).await?;
        info!("Processed");
        Ok(ExecOutput::Progress {
            stage_progress: input.previous_stage.map(|(_, stage)| stage).unwrap_or(0), // ?
            done: false,
            must_commit: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<()>
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
    use crate::{accessors::chain, kv::traits::MutableKV, models::*, new_mem_database};
    use bytes::Bytes;
    use ethereum_types::*;
    use hex_literal::hex;

    #[tokio::test]
    async fn tx_lookup_stage_with_data() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().await.unwrap();

        let recipient1 = H160::from(hex!("f4148309cc30f2dd4ba117122cad6be1e3ba0e2b"));
        let recipient2 = H160::from(hex!("d7fa8303df7073290f66ced1add5fe89dac0c462"));

        let block1 = BodyForStorage {
            base_tx_id: 1,
            tx_amount: 2,
            uncles: vec![],
        };

        let tx1_1 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(1),
                nonce: 1,
                gas_price: 1_000_000.into(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 1.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(
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
        let hash1_1 = keccak256(rlp::encode(&tx1_1));

        let tx1_2 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(1),
                nonce: 2,
                gas_price: 1_000_000.into(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x100.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(
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
        let hash1_2 = keccak256(rlp::encode(&tx1_2));

        let block2 = BodyForStorage {
            base_tx_id: 3,
            tx_amount: 3,
            uncles: vec![],
        };

        let tx2_1 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(1),
                nonce: 3,
                gas_price: 1_000_000.into(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10000.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(
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

        let hash2_1 = keccak256(rlp::encode(&tx2_1));

        let tx2_2 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(1),
                nonce: 6,
                gas_price: 1_000_000.into(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient1),
                value: 0x10.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(
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

        let hash2_2 = keccak256(rlp::encode(&tx2_2));

        let tx2_3 = Transaction {
            message: TransactionMessage::Legacy {
                chain_id: Some(1),
                nonce: 2,
                gas_price: 1_000_000.into(),
                gas_limit: 21_000,
                action: TransactionAction::Call(recipient2),
                value: 2.into(),
                input: Bytes::new(),
            },
            signature: TransactionSignature::new(
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

        let hash2_3 = keccak256(rlp::encode(&tx2_3));

        let block3 = BodyForStorage {
            base_tx_id: 6,
            tx_amount: 0,
            uncles: vec![],
        };

        let hash1 = H256::random();
        let hash2 = H256::random();
        let hash3 = H256::random();

        chain::storage_body::write(&tx, hash1, 1, &block1)
            .await
            .unwrap();
        chain::storage_body::write(&tx, hash2, 2, &block2)
            .await
            .unwrap();
        chain::storage_body::write(&tx, hash3, 3, &block3)
            .await
            .unwrap();

        chain::tx::write(&tx, block1.base_tx_id, &[tx1_1, tx1_2])
            .await
            .unwrap();
        chain::tx::write(&tx, block2.base_tx_id, &[tx2_1, tx2_2, tx2_3])
            .await
            .unwrap();

        let stage = TxLookup {};

        let stage_input = StageInput {
            restarted: false,
            previous_stage: Some((StageId("BodyDownload"), 3)),
            stage_progress: Some(0),
        };

        let output: ExecOutput = stage.execute(&mut tx, stage_input).await.unwrap();

        assert_eq!(
            output,
            ExecOutput::Progress {
                stage_progress: 3,
                done: false,
                must_commit: true,
            }
        );

        for (hashed_tx, block_number) in [
            (hash1_1, vec![1]),
            (hash1_2, vec![1]),
            (hash2_1, vec![2]),
            (hash2_2, vec![2]),
            (hash2_3, vec![2]),
        ] {
            assert_eq!(
                dbg!(chain::tl::read(&tx, hashed_tx).await.unwrap().unwrap()),
                block_number
            );
        }
    }

    #[tokio::test]
    async fn tx_lookup_stage_without_data() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().await.unwrap();
        let stage = TxLookup {};

        let stage_input = StageInput {
            restarted: false,
            previous_stage: Some((StageId("BodyDownload"), 3)),
            stage_progress: Some(0),
        };

        let output: ExecOutput = stage.execute(&mut tx, stage_input).await.unwrap();

        assert_eq!(
            output,
            ExecOutput::Progress {
                stage_progress: 3,
                done: false,
                must_commit: true,
            }
        );
    }
}
