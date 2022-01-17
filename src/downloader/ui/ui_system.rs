use super::ui_view::UIView;
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, Mutex as AsyncMutex},
    task::JoinHandle,
};
use tracing::*;

pub struct UISystem {
    view_cell: Arc<Mutex<Option<Box<dyn UIView>>>>,
    event_loop: Option<UISystemEventLoop>,
    event_loop_handle: Option<JoinHandle<()>>,
    stop_signal_sender: mpsc::Sender<()>,
}

pub type UISystemShared = Arc<tokio::sync::Mutex<UISystem>>;

pub struct UISystemViewScope {
    ui_system: Arc<AsyncMutex<UISystem>>,
}

struct UISystemEventLoop {
    view_cell: Arc<Mutex<Option<Box<dyn UIView>>>>,
    stop_signal_receiver: mpsc::Receiver<()>,
}

impl UISystem {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let view_cell = Arc::new(Mutex::new(None));

        let (stop_signal_sender, stop_signal_receiver) = mpsc::channel::<()>(1);

        let event_loop = UISystemEventLoop {
            view_cell: Arc::clone(&view_cell),
            stop_signal_receiver,
        };

        Self {
            view_cell: Arc::clone(&view_cell),
            event_loop: Some(event_loop),
            event_loop_handle: None,
            stop_signal_sender,
        }
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let event_loop = self
            .event_loop
            .take()
            .ok_or_else(|| anyhow::format_err!("already started once"))?;
        let handle = tokio::spawn(async move {
            let result = event_loop.run().await;
            if let Err(error) = result {
                error!("UIEventLoop loop died: {:?}", error);
            }
        });
        self.event_loop_handle = Some(handle);
        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        if let Some(handle) = self.event_loop_handle.take() {
            self.send_stop_signal();
            handle.await?;
        }
        Ok(())
    }

    fn send_stop_signal(&self) {
        let result = self.stop_signal_sender.try_send(());
        if result.is_err() {
            warn!("UIEventLoop stop signal already sent or the loop died itself");
        }
    }

    pub fn set_view(&self, view: Option<Box<dyn UIView>>) {
        let mut view_cell = self.view_cell.lock();
        *view_cell = view;
    }
}

impl UISystemViewScope {
    pub fn new(
        ui_system: &Arc<AsyncMutex<UISystem>>,
        view: Box<dyn UIView>,
    ) -> anyhow::Result<Self> {
        ui_system.try_lock()?.set_view(Some(view));
        Ok(Self {
            ui_system: ui_system.clone(),
        })
    }
}

impl Drop for UISystemViewScope {
    fn drop(&mut self) {
        if let Ok(ui_system) = self.ui_system.try_lock() {
            ui_system.set_view(None);
        }
    }
}

impl UISystemEventLoop {
    async fn run(self) -> anyhow::Result<()> {
        let mut stop_signal_receiver = self.stop_signal_receiver;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    let view_cell = self.view_cell.lock();
                    if let Some(view) = &*view_cell {
                        _ = view.draw()?;
                    }
                }
                Some(_) = stop_signal_receiver.recv() => {
                    break;
                }
                else => {
                    break;
                }
            }
        }

        debug!("UISystemEventLoop stopped");
        Ok(())
    }
}
