use crate::{MutableTransaction, SyncStage};
use std::marker::PhantomData;
use tokio::sync::oneshot::Receiver as OneshotReceiver;

pub struct StageParameters<'db, RwTx: MutableTransaction<'db>> {
    pub tx: RwTx,

    closed: OneshotReceiver<()>,
    _marker: PhantomData<&'db RwTx>,
}

pub struct StageFactory<'db, RwTx: MutableTransaction<'db>> {
    pub id: SyncStage,
    pub factory: fn(StageParameters<'db, RwTx>),
}
