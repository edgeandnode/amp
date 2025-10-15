//! V8 isolates are not Send and Sync, so we need to run them in their own thread.
//!
//! For this to play well with the async tokio runtime, this module provides a thread pool of V8
//! isolates. The `IsolateThreadPool::invoke` method will run the requested JS function on the first
//! available isolate in the pool. Isolates are reused, but the V8 context (aka JS Realm) is always
//! fresh for each invocation, ensuring isolation between executions.

use std::{panic::AssertUnwindSafe, sync::Arc, thread};

use deadpool::unmanaged::{Object, Pool};
use tokio::sync::{mpsc, oneshot};

use crate::{
    convert::{FromV8, ToV8},
    isolate::{Error, Isolate},
};

/// Request to invoke a JavaScript function. Potentially in batch.
struct JsInvoke<R> {
    /// `filename` is only used for display in stack traces.
    filename: Arc<str>,
    script: Arc<str>,
    function: Arc<str>,
    params_batch: Vec<Vec<Box<dyn ToV8>>>,
    res_tx: oneshot::Sender<Result<Vec<R>, Error>>,
}

impl<R> JsInvoke<R> {
    fn new(
        filename: Arc<str>,
        script: Arc<str>,
        function: Arc<str>,
        params_batch: Vec<Vec<Box<dyn ToV8>>>,
    ) -> (Self, oneshot::Receiver<Result<Vec<R>, Error>>) {
        let (res_tx, res_rx) = oneshot::channel();
        (
            Self {
                filename,
                script,
                function,
                params_batch,
                res_tx,
            },
            res_rx,
        )
    }
}

trait DynJsInvoke: Send {
    fn invoke(self: Box<Self>, isolate: &mut Isolate);
}

impl<R: FromV8> DynJsInvoke for JsInvoke<R> {
    fn invoke(self: Box<Self>, isolate: &mut Isolate) {
        let JsInvoke {
            filename,
            script,
            function,
            params_batch,
            res_tx,
        } = *self;

        // Catch panics during execution.
        let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
            isolate.invoke_batch(
                filename.as_ref(),
                script.as_ref(),
                function.as_ref(),
                params_batch
                    .iter()
                    .map(|p| p.iter().map(|p| p.as_ref()))
                    .collect(),
            )
        }))
        .map_err(|panic| {
            let err_msg = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or(panic.downcast_ref::<&str>().copied())
                .unwrap_or_default();
            Error::Panic(err_msg.to_string())
        })
        .and_then(|x| x);

        let _ = res_tx.send(result);
    }
}

/// Handle to a pool of V8 isolates, each running in its own thread.
#[derive(Debug, Clone)]
pub struct IsolatePool {
    pool: Pool<IsolateThread>,
}

impl Default for IsolatePool {
    fn default() -> Self {
        Self::new()
    }
}

impl IsolatePool {
    pub fn new() -> Self {
        let size = num_cpus::get();
        let pool = Pool::new(size);
        Self { pool }
    }

    /// Dummy pool with no threads.
    pub fn dummy() -> Self {
        Self { pool: Pool::new(0) }
    }

    /// Invoke a JavaScript function on the first available isolate.
    pub async fn invoke_batch<R: FromV8 + 'static>(
        &self,
        filename: &str,
        script: &str,
        function: &str,
        params: Vec<Vec<Box<dyn ToV8>>>,
    ) -> Result<Vec<R>, Error> {
        let status = self.pool.status();
        if status.size < status.max_size {
            // Add a new isolate to the pool if it is not full.
            // Ignore any "pool already full" errors due to race conditions.
            let _ = self.pool.try_add(IsolateThread::spawn());
        }

        let (invoke, res_rx) =
            JsInvoke::<R>::new(filename.into(), script.into(), function.into(), params);

        let isolate_thread = self.pool.get().await?;

        // Unwrapping a `try_send` could panic in two cases:
        //
        // 1. The channel is full. This cannot happen as we only return the isolate to the pool after
        //    the response is received, so the channel will be empty here.
        //
        // 2. The receiver was dropped. This also cannot happen, as the isolate thread only terminates
        //    if we drop the sender. It cannot have panicked it catches panics.
        isolate_thread
            .invoke_tx
            .try_send(Box::new(invoke) as Box<dyn DynJsInvoke>)
            .unwrap();

        // Unwrap: See point 2 above comment on isolate panics.
        match res_rx.await.unwrap() {
            Ok(result) => Ok(result),
            Err(e @ Error::Panic(_)) => {
                // In case of panic, it is safer to not reuse the isolate.
                let _ = Object::take(isolate_thread);
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug)]
struct IsolateThread {
    invoke_tx: mpsc::Sender<Box<dyn DynJsInvoke>>,
}

impl IsolateThread {
    fn spawn() -> Self {
        let (invoke_tx, mut invoke_rx) = tokio::sync::mpsc::channel::<Box<dyn DynJsInvoke>>(1);
        thread::spawn(move || {
            let mut isolate = Isolate::new();
            loop {
                let invoke = match invoke_rx.blocking_recv() {
                    Some(req) => req,
                    None => break, // Sender dropped, terminate the thread
                };

                invoke.invoke(&mut isolate);
            }
        });
        Self { invoke_tx }
    }
}
