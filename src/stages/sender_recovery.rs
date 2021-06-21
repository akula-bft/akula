use crate::{
    accessors::chain,
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput},
    MutableTransaction, StageId,
};
use anyhow::Context;
use async_trait::async_trait;
use ethereum::{Transaction, TransactionMessage};
use ethereum_types::Address;
use secp256k1::{
    recovery::{RecoverableSignature, RecoveryId},
    Message, SECP256K1,
};
use sha3::{Digest, Keccak256};
use std::cmp;
use thiserror::Error;
use tracing::error;

#[derive(Error, Debug)]
pub enum SenderRecoveryError {
    #[error("Canonical hash for block {0} not found")]
    HashNotFound(u64),
    #[error("Block body for block {0} not found")]
    BlockBodyNotFound(u64),
}

const BUFFER_SIZE: u64 = 5000;

fn recover_sender(tx: &Transaction) -> anyhow::Result<Address> {
    let mut sig = [0u8; 64];
    sig[..32].copy_from_slice(tx.signature.r().as_bytes());
    sig[32..].copy_from_slice(tx.signature.s().as_bytes());

    let rec = RecoveryId::from_i32(tx.signature.standard_v() as i32)?;

    let public = &SECP256K1.recover(
        &Message::from_slice(TransactionMessage::from(tx.clone()).hash().as_bytes())?,
        &RecoverableSignature::from_compact(&sig, rec)?,
    )?;

    let address_slice = &Keccak256::digest(&public.serialize_uncompressed()[1..])[12..];
    Ok(Address::from_slice(address_slice))
}

async fn process_block<'db: 'tx, 'tx, RwTx>(tx: &'tx mut RwTx, height: u64) -> anyhow::Result<()>
where
    RwTx: MutableTransaction<'db>,
{
    let hash = chain::canonical_hash::read(tx, height)
        .await?
        .ok_or(SenderRecoveryError::HashNotFound(height))?;
    let body = chain::storage_body::read(tx, hash, height)
        .await?
        .ok_or(SenderRecoveryError::BlockBodyNotFound(height))?;
    let txs = chain::tx::read(tx, body.base_tx_id, body.tx_amount).await?;

    let mut senders = vec![];
    for tx in &txs {
        senders.push(recover_sender(tx)?);
    }

    chain::tx_sender::write(tx, body.base_tx_id, &senders).await
}

#[derive(Debug)]
pub struct SenderRecovery;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for SenderRecovery
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("SenderRecovery")
    }

    fn description(&self) -> &'static str {
        "Recovering senders of transactions from signatures"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let from_height = input.stage_progress.unwrap_or(0);
        let max_height = if let Some((_, height)) = input.previous_stage {
            height
        } else {
            0
        };
        let to_height = cmp::min(max_height, from_height + BUFFER_SIZE);

        let mut height = from_height;
        while height < to_height {
            process_block(tx, height + 1)
                .await
                .with_context(|| format!("Failed to recover senders for block {}", height + 1))?;
            height += 1;
        }

        let made_progress = height > from_height;
        Ok(ExecOutput::Progress {
            stage_progress: height,
            done: !made_progress,
            must_commit: made_progress,
        })
    }

    async fn unwind<'tx>(&self, _tx: &'tx mut RwTx, _input: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{kv::traits::MutableKV, models::BodyForStorage, new_mem_database};
    use ethereum::{TransactionAction, TransactionSignature};
    use ethereum_types::{H160, H256};
    use hex_literal::hex;

    #[tokio::test]
    async fn recover_senders() {
        let db = new_mem_database().unwrap();
        let mut tx = db.begin_mutable().await.unwrap();

        let sender1 = H160::from(hex!("de1ef574fd619979b16fd043ea97c4f4536af2e6"));
        let sender2 = H160::from(hex!("c93c9f9cac833846a66bce3bd9dc7c85e36463af"));

        let recipient1 = H160::from(hex!("f4148309cc30f2dd4ba117122cad6be1e3ba0e2b"));
        let recipient2 = H160::from(hex!("d7fa8303df7073290f66ced1add5fe89dac0c462"));

        let block1 = BodyForStorage {
            base_tx_id: 1,
            tx_amount: 2,
            uncles: vec![],
        };

        let tx1_1 = Transaction {
            nonce: 1.into(),
            gas_limit: 21_000.into(),
            gas_price: 1_000_000.into(),
            action: TransactionAction::Call(recipient1),
            value: 1.into(),
            input: vec![],
            signature: TransactionSignature::new(
                0x25,
                H256::from(hex!(
                    "11d244ae19e3bb96d1bb864aa761d48e957984a154329f0de757cd105f9c7ac4"
                )),
                H256::from(hex!(
                    "0e3828d13eed24036941eb5f7fd65de57aad1184342f2244130d2941554342ba"
                )),
            )
            .unwrap(),
        };

        let tx1_2 = Transaction {
            nonce: 2.into(),
            gas_limit: 21_000.into(),
            gas_price: 1_000_000.into(),
            action: TransactionAction::Call(recipient1),
            value: 0x100.into(),
            input: vec![],
            signature: TransactionSignature::new(
                0x26,
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
            base_tx_id: 3,
            tx_amount: 3,
            uncles: vec![],
        };

        let tx2_1 = Transaction {
            nonce: 3.into(),
            gas_limit: 21_000.into(),
            gas_price: 1_000_000.into(),
            action: TransactionAction::Call(recipient1),
            value: 0x10000.into(),
            input: vec![],
            signature: TransactionSignature::new(
                0x26,
                H256::from(hex!(
                    "2450fdbf8fbc1dee15022bfa7392eb15f04277782343258e185972b5b2b8bf79"
                )),
                H256::from(hex!(
                    "0f556dc665406344c3f456d44a99d2a4ab70c68dce114e78d90bfd6d11287c07"
                )),
            )
            .unwrap(),
        };

        let tx2_2 = Transaction {
            nonce: 6.into(),
            gas_limit: 21_000.into(),
            gas_price: 1_000_000.into(),
            action: TransactionAction::Call(recipient1),
            value: 0x10.into(),
            input: vec![],
            signature: TransactionSignature::new(
                0x25,
                H256::from(hex!(
                    "ac0222c1258eada1f828729186b723eaf3dd7f535c5de7271ea02470cbb1029f"
                )),
                H256::from(hex!(
                    "3c6b5f961c19a134f75a0924264558d6e551f0476e1fdd431a88b52d9b4ac1e6"
                )),
            )
            .unwrap(),
        };

        let tx2_3 = Transaction {
            nonce: 2.into(),
            gas_limit: 21_000.into(),
            gas_price: 1_000_000.into(),
            action: TransactionAction::Call(recipient2),
            value: 2.into(),
            input: vec![],
            signature: TransactionSignature::new(
                0x26,
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

        chain::canonical_hash::write(&tx, 1, hash1).await.unwrap();
        chain::canonical_hash::write(&tx, 2, hash2).await.unwrap();
        chain::canonical_hash::write(&tx, 3, hash3).await.unwrap();

        chain::tx::write(&tx, block1.base_tx_id, &[tx1_1, tx1_2])
            .await
            .unwrap();
        chain::tx::write(&tx, block2.base_tx_id, &[tx2_1, tx2_2, tx2_3])
            .await
            .unwrap();

        let stage = SenderRecovery {};

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

        let senders1 = chain::tx_sender::read(&tx, block1.base_tx_id, block1.tx_amount);
        assert_eq!(senders1.await.unwrap(), [sender1, sender1]);

        let senders2 = chain::tx_sender::read(&tx, block2.base_tx_id, block2.tx_amount);
        assert_eq!(senders2.await.unwrap(), [sender1, sender2, sender2]);

        let senders3 = chain::tx_sender::read(&tx, block3.base_tx_id, block3.tx_amount);
        assert!(senders3.await.unwrap().is_empty());
    }
}
