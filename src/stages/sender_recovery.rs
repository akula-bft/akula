use crate::{
    accessors::chain,
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput},
    MutableTransaction,
    StageId,
};
use async_trait::async_trait;
use ethereum::Transaction;
use ethereum_types::Address;

#[derive(Debug)]
pub struct SenderRecovery;

fn recover_sender(tx: &Transaction) -> Address {
    Address::random() // TODO
}

async fn process_block<RwTx>(tx: &mut RwTx, height: u64) -> anyhow::Result<()>
where
    RwTx: MutableTransaction,
{
    let hash = chain::canonical_hash::read(tx, height).await?.unwrap();
    let body = chain::storage_body::read(tx, hash, height).await?.unwrap();
    let txs = chain::tx::read(tx, body.base_tx_id, body.tx_amount).await?;

    let mut senders = vec![];
    for tx in &txs {
        senders.push(recover_sender(&tx));
    }

    chain::tx_sender::write(tx, body.base_tx_id, &senders)
}

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

    async fn execute<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: StageInput
    ) -> anyhow::Result<ExecOutput>
        where
        'db: 'tx,
    {
        let from_height = input.stage_progress.unwrap_or(0);
        let to_height = if let Some((_, height)) = input.previous_stage { height } else { 0 };

        for height in from_height..=to_height {
            process_block(tx, height).await?
        }

        let made_progress = height > from_height;
        Ok(ExecOutput::Progress {
            stage_progress: height,
            done: !made_progress,
            must_commit: made_progress,
        })

    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput
    ) -> anyhow::Result<()>
        where
        'db: 'tx
    {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}