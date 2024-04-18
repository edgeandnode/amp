use std::sync::{Arc, Mutex};

pub trait StatsUpdater {
    fn inc_blocks(&self, blocks: u64);
    fn inc_bytes(&self, bytes: u64);
}

pub trait StatsGetter {
    fn get_blocks(&self) -> u64;
    fn get_bytes(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct Stats {
    pub bytes: u64,
    pub blocks: u64,
}
impl StatsUpdater for Arc<Mutex<Stats>> {
    fn inc_bytes(&self, bytes: u64) {
        let mut stats = self.lock().unwrap();
        stats.bytes += bytes;
    }

    fn inc_blocks(&self, blocks: u64) {
        let mut stats = self.lock().unwrap();
        stats.blocks += blocks;
    }
}

impl StatsGetter for Arc<Mutex<Stats>> {
    fn get_blocks(&self) -> u64 {
        let stats = self.lock().unwrap();
        stats.blocks
    }
    fn get_bytes(&self) -> u64 {
        let stats = self.lock().unwrap();
        stats.bytes
    }
}