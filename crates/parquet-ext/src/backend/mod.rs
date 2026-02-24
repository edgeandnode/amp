use std::thread::JoinHandle;

use tokio::sync::oneshot::Sender;

pub struct PipelineBackend {
    join_handle: Option<JoinHandle<()>>,
    signal: Option<Sender<()>>,
}

impl PipelineBackend {
    pub fn new(join_handle: JoinHandle<()>, signal: Sender<()>) -> Option<Self> {
        Some(Self {
            join_handle: Some(join_handle),
            signal: Some(signal),
        })
    }
    pub fn shutdown(&mut self) {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PipelineBackend {
    fn drop(&mut self) {
        self.shutdown();
    }
}
