use super::{stage::Stage, unwind::PersistentUnwindStack};
use crate::MutableTransaction;

pub struct State<'db, RwTx: MutableTransaction<'db>> {
    unwind_stack: PersistentUnwindStack,
    stages: Vec<Box<dyn Stage<'db, RwTx>>>,
    unwind_order: Vec<Box<dyn Stage<'db, RwTx>>>,
    current_stage: u64,
}
