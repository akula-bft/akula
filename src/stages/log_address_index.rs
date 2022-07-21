use crate::{
    kv::{mdbx::*, tables},
    stagedsync::stage::*,
    stages::stage_util::*,
    StageId,
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct LogAddressIndex(pub IndexParams);

#[async_trait]
impl<'db, E> Stage<'db, E> for LogAddressIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        StageId("LogAddressIndex")
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        Ok(execute_index(
            tx,
            input,
            &self.0,
            tables::LogAddressesByBlock,
            tables::LogAddressIndex,
            |block_number, address| (block_number, address),
        )?)
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        unwind_index(
            tx,
            input,
            tables::LogAddressesByBlock,
            tables::LogAddressIndex,
            |_, address| address,
        )
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        prune_index(
            tx,
            input,
            tables::LogAddressesByBlock,
            tables::LogAddressIndex,
            |block_number, address| (block_number, address),
        )
    }
}
