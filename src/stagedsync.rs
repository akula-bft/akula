pub mod stage_factory;
pub mod stages;

use crate::MutableTransaction;

use self::stage_factory::StageFactory;

pub struct StagedSync<'db, RwTx: MutableTransaction<'db>> {
    pub stage_builders: Vec<StageFactory<'db, RwTx>>,
}
