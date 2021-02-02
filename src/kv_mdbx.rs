use crate::traits::{self, Cursor};
use anyhow::anyhow;
use bytes::Bytes;
use futures::channel::mpsc::UnboundedReceiver;
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
        mpsc::{channel, unbounded_channel, Sender, UnboundedSender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
    },
    task::LocalSet,
};

pub struct WriteCursor<'txn> {
    cmd_tx: Sender<TxCommand>,
    cursor_id: u64,
    cleanup_tx: UnboundedSender<TxCleanupCommand>,
    _marker: PhantomData<&'txn ()>,
}

impl Drop for WriteCursor<'_> {
    fn drop(&mut self) {
        let _ = self
            .cleanup_tx
            .send(TxCleanupCommand::Cursor { id: self.cursor_id });
    }
}

enum TxCleanupCommand {
    Cursor { id: u64 },
}

enum TxCommand {
    Cursor {
        bucket_name: String,
        cb: OneshotSender<anyhow::Result<(u64, UnboundedSender<TxCleanupCommand>)>>,
    },
}

pub struct WriteTxn<'env> {
    cmd_tx: Sender<TxCommand>,
    cleanup_tx: UnboundedSender<TxCleanupCommand>,
    cancellation: OneshotSender<()>,
    _marker: PhantomData<&'env ()>,
}

impl<'env, 'txn> WriteTxn<'env> {
    fn make_cursor(
        env: &Env,
        txn: &'txn RwTxn<'env, 'env>,
        cursors: &mut HashMap<u64, RwIter<'txn, Bytes, Bytes>>,
        bucket_name: &str,
    ) -> anyhow::Result<u64> {
        let db = env
            .open_database(Some(bucket_name))?
            .ok_or_else(|| anyhow!("no database!"))?;

        loop {
            let id = rand::random();
            if let Entry::Vacant(entry) = cursors.entry(id) {
                entry.insert(db.iter_mut(&txn)?);
                break Ok(id);
            }
        }
    }

    fn run_command(
        env: &Env,
        txn: &'txn RwTxn<'env, 'env>,
        cursors: &mut HashMap<u64, RwIter<'txn, Bytes, Bytes>>,
        cleanup_tx: &UnboundedSender<TxCleanupCommand>,
        cmd: TxCommand,
    ) {
        match cmd {
            TxCommand::Cursor { bucket_name, cb } => {
                let _ = cb.send(
                    Self::make_cursor(env, txn, cursors, &bucket_name)
                        .map(|id| (id, cleanup_tx.clone())),
                );
            }
        }
    }

    fn run_cleanup(
        _env: &Env,
        _txn: &'txn RwTxn<'env, 'env>,
        cursors: &mut HashMap<u64, RwIter<'txn, Bytes, Bytes>>,
        cmd: TxCleanupCommand,
    ) {
        match cmd {
            TxCleanupCommand::Cursor { id } => {
                cursors.remove(&id).unwrap();
            }
        }
    }

    pub async fn cursor<B: ToString>(&self, bucket_name: B) -> anyhow::Result<WriteCursor<'_>> {
        let (cb, rx) = oneshot();
        assert!(self
            .cmd_tx
            .send(TxCommand::Cursor {
                bucket_name: bucket_name.to_string(),
                cb,
            })
            .await
            .is_ok());

        let (cursor_id, cleanup_tx) = rx.await.unwrap()?;

        Ok(WriteCursor {
            cmd_tx: self.cmd_tx.clone(),
            cursor_id,
            cleanup_tx,
            _marker: PhantomData,
        })
    }
}

pub struct DbServer {
    env: Arc<Env>,
    local_set: Arc<LocalSet>,
}

impl DbServer {
    pub fn open(
        local_set: Arc<LocalSet>,
        path: String,
        options: EnvOpenOptions,
    ) -> anyhow::Result<Self> {
        let env = Arc::new(options.open(path)?);
        Ok(Self { env, local_set })
    }

    /// Create a new write transaction
    pub async fn write_txn(&self) -> anyhow::Result<WriteTxn<'_>> {
        let (cmd_tx, mut cmd_rx) = channel(1);
        // Unbounded channel for cleanup as no AsyncDrop in Rust.
        let (cleanup_tx, mut cleanup_rx) = unbounded_channel();
        let (tx, rx) = oneshot();
        let (cancellation, mut dropped) = oneshot();

        let env = self.env.clone();

        self.local_set.spawn_local({
            let cleanup_tx = cleanup_tx.clone();
            async move {
            match env.write_txn() {
                Err(e) => {
                    tx.send(Err(e)).unwrap();
                }
                Ok(txn) => {
                    tx.send(Ok(())).unwrap();

                    let mut cursors = HashMap::default();

                    loop {
                        select! {
                            _ = &mut dropped => {
                                return;
                            }
                            cmd = cmd_rx.recv() => {
                                if let Some(cmd) = cmd {
                                    WriteTxn::run_command(&env, &txn, &mut cursors, &cleanup_tx, cmd);
                                } else {
                                    return;
                                }
                            }
                            cleanup = cleanup_rx.recv() => {
                                if let Some(cmd) = cleanup {
                                    WriteTxn::run_cleanup(&env, &txn, &mut cursors, cmd);
                                } else {
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }});

        rx.await.unwrap()?;

        Ok(WriteTxn {
            cmd_tx,
            cleanup_tx,
            cancellation,
            _marker: PhantomData,
        })
    }
}
