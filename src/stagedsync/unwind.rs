use crate::{MutableTransaction, SyncStage, Transaction};
use async_trait::async_trait;

#[async_trait]
pub trait Unwinder {
    async fn unwind_to<'db, Tx: MutableTransaction<'db>>(
        &mut self,
        block: u64,
        tx: Tx,
    ) -> anyhow::Result<()>;
}

pub struct UnwindState {
    pub stage: SyncStage,
    pub unwind_point: u64,
}

impl UnwindState {
    pub async fn done<'db, RwTx: MutableTransaction<'db>>(&self, tx: &RwTx) -> anyhow::Result<()> {
        self.stage.save_progress(tx, self.unwind_point).await?;
        self.stage.save_unwind(tx, 0).await
    }
}

#[derive(Default)]
pub struct PersistentUnwindStack {
    unwind_stack: Vec<UnwindState>,
}

impl PersistentUnwindStack {
    pub async fn add_from_db<'db, Tx: Transaction<'db>>(
        &mut self,
        tx: &Tx,
        stage_id: SyncStage,
    ) -> anyhow::Result<()> {
        if let Some(u) = Self::load_from_db(tx, stage_id).await? {
            self.unwind_stack.push(u);
        }

        Ok(())
    }

    pub async fn load_from_db<'db, Tx: Transaction<'db>>(
        tx: &Tx,
        stage: SyncStage,
    ) -> anyhow::Result<Option<UnwindState>> {
        Ok(stage.get_unwind(tx).await?.and_then(|unwind_point| {
            (unwind_point > 0).then_some(UnwindState {
                stage,
                unwind_point,
            })
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.unwind_stack.is_empty()
    }

    pub async fn add<'db, Tx: MutableTransaction<'db>>(
        &mut self,
        u: UnwindState,
        tx: &Tx,
    ) -> anyhow::Result<()> {
        let current_point = u.stage.get_unwind(tx).await?;
        if u.unwind_point < current_point.unwrap_or_default() {
            let UnwindState {
                unwind_point,
                stage,
            } = u;
            self.unwind_stack.push(u);
            stage.save_unwind(tx, unwind_point).await?;
        }

        Ok(())
    }

    pub fn pop(&mut self) -> Option<UnwindState> {
        self.unwind_stack.pop()
    }
}
