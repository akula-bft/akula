use crate::{
    kv::{
        tables::{self, ErasedTable},
        traits::*,
    },
    models::*,
    stagedsync::{format_duration, stage::*},
    StageId,
};
use async_trait::async_trait;
use rayon::prelude::*;
use std::time::{Duration, Instant};
use tokio::pin;
use tokio_stream::StreamExt as _;
use tracing::*;

/// Recovery of senders of transactions from signatures
#[derive(Debug)]
pub struct SenderRecovery {
    pub batch_size: usize,
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for SenderRecovery
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("SenderRecovery")
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        let mut body_cur = tx.cursor(tables::BlockBody).await?;
        let mut tx_cur = tx.cursor(tables::BlockTransaction.erased()).await?;
        let mut senders_cur = tx.mutable_cursor(tables::TxSender.erased()).await?;
        senders_cur.last().await?;

        let walker = walk(&mut body_cur, Some(BlockNumber(highest_block.0 + 1)));
        pin!(walker);
        let mut batch = Vec::with_capacity(self.batch_size);
        let started_at = Instant::now();
        let started_at_txnum = tx
            .get(
                tables::TotalTx,
                input.first_started_at.1.unwrap_or(BlockNumber(0)),
            )
            .await?;
        let done = loop {
            let mut read_again = false;
            debug!("Reading bodies");
            while let Some(((block_number, hash), body)) = walker.try_next().await? {
                let txs = walk(&mut tx_cur, Some(body.base_tx_id.encode().to_vec()))
                    .take(body.tx_amount.try_into()?)
                    .map(|res| res.map(|(_, tx)| tx))
                    .collect::<anyhow::Result<Vec<_>>>()
                    .await?;
                batch.push((block_number, hash, txs));

                highest_block = block_number;

                if batch.len() >= self.batch_size {
                    read_again = true;
                    break;
                }
            }

            debug!("Recovering senders from batch of {} bodies", batch.len());
            let mut recovered_senders = batch
                .par_drain(..)
                .filter_map(move |(block_number, hash, txs)| {
                    if !txs.is_empty() {
                        let senders = txs
                            .into_iter()
                            .map(|encoded_tx| {
                                let tx = ErasedTable::<tables::BlockTransaction>::decode_value(
                                    &encoded_tx,
                                )?;
                                let sender = tx.recover_sender()?;
                                Ok::<_, anyhow::Error>(sender)
                            })
                            .collect::<anyhow::Result<Vec<Address>>>();

                        Some(senders.map(|senders| {
                            (
                                ErasedTable::<tables::TxSender>::encode_key((block_number, hash))
                                    .to_vec(),
                                senders.encode(),
                            )
                        }))
                    } else {
                        None
                    }
                })
                .collect::<anyhow::Result<Vec<_>>>()?;

            debug!("Inserting recovered senders");
            for (db_key, db_value) in recovered_senders.drain(..) {
                senders_cur.append(db_key, db_value).await?;
            }

            if !read_again {
                break true;
            }

            let now = Instant::now();
            let elapsed = now - started_at;
            if elapsed > Duration::from_secs(30) {
                let mut format_string = format!("Extracted senders from block {}", highest_block);

                if let Some(started_at_txnum) = started_at_txnum {
                    let current_txnum = tx.get(tables::TotalTx, highest_block).await?;
                    let total_txnum = tx
                        .cursor(tables::TotalTx)
                        .await?
                        .last()
                        .await?
                        .map(|(_, v)| v);

                    if let Some(current_txnum) = current_txnum {
                        if let Some(total_txnum) = total_txnum {
                            let elapsed_since_start = now - input.first_started_at.0;

                            let ratio_complete = (current_txnum - started_at_txnum) as f64
                                / (total_txnum - started_at_txnum) as f64;

                            let estimated_total_time = Duration::from_secs(
                                (elapsed_since_start.as_secs() as f64 / ratio_complete) as u64,
                            );

                            debug!(
                                "Elapsed since start {:?}, ratio complete {:?}, estimated total time {:?}",
                                elapsed_since_start, ratio_complete, estimated_total_time
                            );

                            format_string = format!(
                                "{}, progress: {:0>2.2}%, {} remaining",
                                format_string,
                                ratio_complete * 100_f64,
                                format_duration(
                                    estimated_total_time.saturating_sub(elapsed_since_start),
                                    false
                                )
                            );
                        }
                    }
                }

                info!("{}", format_string);
                break false;
            }
        };

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut senders_cur = tx.mutable_cursor(tables::TxSender).await?;

        while let Some(((block_number, _), _)) = senders_cur.last().await? {
            if block_number > input.unwind_to {
                senders_cur.delete_current().await?;
            } else {
                break;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{accessors::*, kv::new_mem_database};
    use bytes::Bytes;
    use hex_literal::hex;

    const CHAIN_ID: Option<ChainId> = Some(ChainId(1));

    #[tokio::test]
    async fn recover_senders() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().await.unwrap();

        let sender1 = Address::from(hex!("de1ef574fd619979b16fd043ea97c4f4536af2e6"));
        let sender2 = Address::from(hex!("c93c9f9cac833846a66bce3bd9dc7c85e36463af"));

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

        let block3 = BodyForStorage {
            base_tx_id: 6.into(),
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

        chain::canonical_hash::write(&tx, 1, hash1).await.unwrap();
        chain::canonical_hash::write(&tx, 2, hash2).await.unwrap();
        chain::canonical_hash::write(&tx, 3, hash3).await.unwrap();

        chain::tx::write(&tx, block1.base_tx_id, &[tx1_1, tx1_2])
            .await
            .unwrap();
        chain::tx::write(&tx, block2.base_tx_id, &[tx2_1, tx2_2, tx2_3])
            .await
            .unwrap();

        let mut stage = SenderRecovery { batch_size: 50_000 };

        let stage_input = StageInput {
            restarted: false,
            first_started_at: (Instant::now(), Some(BlockNumber(0))),
            previous_stage: Some((StageId("BodyDownload"), 3.into())),
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

        let senders1 = chain::tx_sender::read(&tx, hash1, 1);
        assert_eq!(senders1.await.unwrap(), [sender1, sender1]);

        let senders2 = chain::tx_sender::read(&tx, hash2, 2);
        assert_eq!(senders2.await.unwrap(), [sender1, sender2, sender2]);

        let senders3 = chain::tx_sender::read(&tx, hash3, 3);
        assert!(senders3.await.unwrap().is_empty());
    }
}
