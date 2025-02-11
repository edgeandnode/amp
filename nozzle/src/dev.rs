mod artifacts;
mod nozzle;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use alloy::{
    primitives::BlockNumber, rpc::client::ReqwestClient as RpcClient,
    transports::http::reqwest::Url,
};
use anyhow::Context as _;
use axum::{http::Response, serve::ListenerExt as _};
use nozzle::Nozzle;
use tokio::{net::TcpListener, sync::watch};

use crate::{handle_jsonl_request, json_error};

pub async fn run(artifact_dir: &Path, nozzle_dir: PathBuf, rpc_url: Url) -> anyhow::Result<()> {
    let addr: SocketAddr = ([0, 0, 0, 0], 1610).into();

    let nozzle = Arc::new(tokio::sync::Mutex::new(Nozzle::new(nozzle_dir)?));
    nozzle
        .lock()
        .await
        .add_rpc_dataset("anvil", rpc_url.as_str())?;

    let manifests = artifacts::load_manifests(&artifact_dir, &*nozzle.lock().await).await?;
    for manifest in manifests {
        nozzle.lock().await.add_manifest_dataset(manifest)?;
    }

    {
        let rpc = RpcClient::new_http(rpc_url);
        let mut chain_head = watch_chain_head(rpc);
        let nozzle = nozzle.clone();
        tokio::spawn(async move {
            loop {
                let chain_head = match chain_head.changed().await {
                    Ok(()) => match *chain_head.borrow() {
                        Some(block) => block,
                        None => break,
                    },
                    Err(_) => break,
                };
                match nozzle.lock().await.dump_datasets(Some(chain_head)).await {
                    Ok(()) => (),
                    Err(err) => log::error!("dump error: {err}"),
                };
            }
        });
    }

    let router = axum::Router::new()
        .route(
            "/sql",
            axum::routing::post(
                |axum::extract::State(nozzle): axum::extract::State<
                    Arc<tokio::sync::Mutex<Nozzle>>,
                >,
                 request| async move {
                    let service = match nozzle.lock().await.service() {
                        Ok(service) => service,
                        Err(err) => return Response::new(json_error(err).into()),
                    };
                    handle_jsonl_request(&service, request).await
                },
            )
            .with_state(nozzle.clone()),
        )
        .layer(
            tower_http::compression::CompressionLayer::new()
                .br(true)
                .gzip(true),
        );
    let listener = TcpListener::bind(addr)
        .await?
        .tap_io(|tcp_stream| tcp_stream.set_nodelay(true).unwrap());
    axum::serve(listener, router).await?;
    Ok(())
}

fn watch_chain_head(rpc: RpcClient) -> watch::Receiver<Option<BlockNumber>> {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        loop {
            match fetch_latest_block_number(&rpc).await {
                Ok(latest_block) => {
                    tx.send_if_modified(|value| {
                        if *value == Some(latest_block) {
                            return false;
                        }
                        *value = Some(latest_block);
                        true
                    });
                }
                Err(err) => {
                    log::error!("RPC error: {err}");
                }
            };
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    rx
}

async fn fetch_latest_block_number(rpc: &RpcClient) -> anyhow::Result<BlockNumber> {
    #[derive(Debug, serde::Deserialize)]
    struct Response {
        number: String,
    }
    let response: Response = rpc
        .request("eth_getBlockByNumber", ("latest", false))
        .await?;
    u64::from_str_radix(response.number.trim_start_matches("0x"), 16)
        .context("failed to parse RPC response")
}
