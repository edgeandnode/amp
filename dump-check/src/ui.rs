use dump_check::metrics::METRICS;
use human_bytes::human_bytes;
use indicatif::{ProgressBar, ProgressStyle};

pub(crate) async fn ui(blocks: u64) {
    if atty::is(atty::Stream::Stdout) {
        nice_ui(blocks).await;
    } else {
        log_ui(blocks).await;
    }
}

async fn log_ui(blocks: u64) {
    while METRICS.blocks_read.get() < blocks as f64 {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        tracing::info!(
            "Progress: {:.2}%, Read {} blocks, {} arrow data",
            METRICS.blocks_read.get() / blocks as f64 * 100.0,
            METRICS.blocks_read.get(),
            human_bytes(METRICS.bytes_read.get())
        );
    }
}

async fn nice_ui(blocks: u64) {
    let pb = ProgressBar::new(blocks);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:60.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    while METRICS.blocks_read.get() < blocks as f64 {
        pb.set_position(METRICS.blocks_read.get() as u64);
        pb.set_message(format!("{:>10}", human_bytes(METRICS.bytes_read.get())));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    pb.finish_with_message(format!("{:>10}", human_bytes(METRICS.bytes_read.get())));
}
