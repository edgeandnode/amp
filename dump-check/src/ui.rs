use std::sync::Arc;

use dump_check::metrics::MetricsRegistry;
use human_bytes::human_bytes;
use indicatif::{ProgressBar, ProgressStyle};

pub(crate) async fn ui(blocks: u64, metrics: Arc<MetricsRegistry>) {
    if atty::is(atty::Stream::Stdout) {
        nice_ui(blocks, metrics).await;
    } else {
        log_ui(blocks, metrics).await;
    }
}

async fn log_ui(blocks: u64, metrics: Arc<MetricsRegistry>) {
    while metrics.blocks_read.get() < blocks {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        tracing::info!(
            "Progress: {:.2}%, Read {} blocks, {} arrow data",
            metrics.blocks_read.get() as f64 / blocks as f64 * 100.0,
            metrics.blocks_read.get(),
            human_bytes(metrics.bytes_read.get() as f64)
        );
    }
}

async fn nice_ui(blocks: u64, metrics: Arc<MetricsRegistry>) {
    let pb = ProgressBar::new(blocks);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:60.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    while metrics.blocks_read.get() < blocks {
        pb.set_position(metrics.blocks_read.get());
        pb.set_message(format!(
            "{:>10}",
            human_bytes(metrics.bytes_read.get() as f64)
        ));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    pb.finish_with_message(format!(
        "{:>10}",
        human_bytes(metrics.bytes_read.get() as f64)
    ));
}
