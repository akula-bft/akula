use crate::{
    kv::{mdbx::MdbxTransaction, tables},
    mining::{proposal::create_proposal, state::MiningConfig},
    models::{BlockHeader, BlockNumber},
    stagedsync::stage::*,
    StageId,
};
use anyhow::bail;
use async_trait::async_trait;
use mdbx::{EnvironmentKind, RW};
use tracing::debug;

pub const CREATE_BLOCK: StageId = StageId("CreateBlock");

#[derive(Debug)]
pub struct CreateBlock {
    pub config: MiningConfig,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for CreateBlock
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        CREATE_BLOCK
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let parent_number = input.stage_progress.unwrap();

        let parent_header = get_header(tx, parent_number)?;

        let proposal = create_proposal(&parent_header, &self.config)?;

        debug!("Proposal created: {:?}", proposal); // TODO save block proposal

        Ok(ExecOutput::Progress {
            stage_progress: parent_number + 1,
            done: true,
            reached_tip: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        _tx: &'tx mut MdbxTransaction<'db, RW, E>,
        _input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        todo!()
    }
}

fn get_header<E>(
    tx: &mut MdbxTransaction<'_, RW, E>,
    number: BlockNumber,
) -> anyhow::Result<BlockHeader>
where
    E: EnvironmentKind,
{
    let mut cursor = tx.cursor(tables::Header)?;
    Ok(match cursor.seek(number)? {
        Some(((found_number, _), header)) if found_number == number => header,
        _ => bail!("Expected header at block height {} not found.", number.0),
    })
}
