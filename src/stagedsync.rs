pub mod stage_factory;
pub mod stages;
pub mod state;
pub mod unwind;

use crate::MutableTransaction;

use self::stage_factory::StageFactory;

pub struct StagedSync<'db, RwTx: MutableTransaction<'db>> {
    pub stage_factories: Vec<StageFactory<'db, RwTx>>,
    pub unwind_order: Vec<usize>,
}

impl<'db, RwTx: MutableTransaction<'db>> StagedSync<'db, RwTx> {
    pub fn new(stages: Vec<StageFactory<'db, RwTx>>, unwind_order: Vec<usize>) -> Self {
        Self {
            stage_factories: stages,
            unwind_order,
        }
    }
}
