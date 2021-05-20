use super::stage::Stage;
use crate::{MutableTransaction, SyncStage};
use std::marker::PhantomData;
use tokio::sync::oneshot::Receiver as OneshotReceiver;

pub struct StageParameters<'db, RwTx: MutableTransaction<'db>> {
    pub tx: RwTx,

    closed: OneshotReceiver<()>,
    _marker: PhantomData<&'db RwTx>,
}

// pub struct StageFactory<'tx, 'db: 'tx, RwTx: MutableTransaction<'db>> {
//     pub id: SyncStage,
//     pub create:
//         Box<dyn Fn(StageParameters<'db, RwTx>) -> Stage<'tx, 'db, RwTx> + Send + Sync + 'static>,
// }
