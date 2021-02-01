use crate::traits::Cursor;
use anyhow::anyhow;
use bytes::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoIter, RwIter, RwTxn};
use serde::__private::de;
use std::{
    any,
    collections::{hash_map::Entry, HashMap},
    marker::PhantomData,
    sync::Arc,
};
use tokio::{
    select,
    sync::{
        mpsc::{channel, Sender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
    },
    task::LocalSet,
};

pub struct WriteCursor<'env> {
    inner: Arc<DbServerInner>,
    _marker: PhantomData<&'env ()>,
}

pub struct WriteTxn<'env> {
    inner: Arc<DbServerInner>,
    _marker: PhantomData<&'env ()>,
}

impl<'env> WriteTxn<'env> {
    pub async fn cursor(&self, bucket_name: String) -> anyhow::Result<WriteCursor<'_>> {}
    fn make_cursor<'env: 'txn>(
        env: &'env Env,
        txn: &'txn mut RwTxn<'env, 'env>,
        bucket_name: &str,
    ) -> anyhow::Result<WriteCursor<'txn>> {
        let db = env
            .open_database(Some(bucket_name))?
            .ok_or_else(|| anyhow!("no database!"))?;

        Ok(WriteCursor {
            inner: db.iter_mut(txn)?,
        })
    }

    fn run_command<'env: 'txn>(
        env: &'env Env,
        txn: &'txn mut RwTxn<'env, 'env>,
        cmd: TxCommand<'txn>,
    ) {
        match cmd {
            TxCommand::Cursor { bucket_name, cb } => {
                cb.send(Self::make_cursor(env, txn, bucket_name.as_str()));
            }
        }
    }
}

enum DbCommand {
    WriteTxn {
        cb: OneshotSender<anyhow::Result<u64>>,
    },
    Cursor {
        bucket_name: String,
        cb: OneshotSender<anyhow::Result<u64>>,
    }
}

struct DbServerInner {
    local_set: Arc<LocalSet>,
    cmd_tx: Sender<DbCommand>,
    cancellation: OneshotSender<()>,
}

pub struct DbServer(Arc<DbServerInner>);

impl DbServer {
    pub fn open(
        local_set: Arc<LocalSet>,
        path: String,
        options: EnvOpenOptions,
    ) -> anyhow::Result<Self> {
        let env = Arc::new(options.open(path)?);
        let (cmd_tx, mut cmd_rx) = channel(1);
        let (cancellation, mut dropped) = oneshot();

        local_set.spawn_local(async move {
            let txns = Default::default();
            loop {
                select! {
                    _ = &mut dropped => {
                        return;
                    }
                    cmd = cmd_rx.recv() => {
                        if let Some(cmd) = cmd {
                            DbServer::run_command(&env, &mut txns, cmd);
                        } else {
                            return;
                        }
                    }
                }
            }
        });

        Ok(Self(Arc::new(DbServerInner {
            local_set,
            cmd_tx,
            cancellation,
        })))
    }

    fn run_command<'env>(
        env: &'env Env,
        txns: &mut HashMap<u64, RwTxn<'env, 'env>>,
        cmd: DbCommand,
    ) {
        match cmd {
            DbCommand::WriteTxn { cb } => {
                cb.send(
                    env.write_txn()
                        .map(|txn| loop {
                            let key = rand::random();
                            if let Entry::Vacant(entry) = txns.entry(key) {
                                entry.insert(txn);
                                break key;
                            }
                        })
                        .map_err(anyhow::Error::from),
                );
            }
            DbCommand::Cursor { cb } => {
                cb.send(env
                    .open_database(Some(bucket_name))?
                    .ok_or_else(|| anyhow!("no database!")).map(|db| db.iter_mut(txn)));
            }
        }
    }

    pub async fn write_transaction(&self) -> WriteTxn<'_> {
        WriteTxn {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}
