use std::{sync::{Arc, Mutex}, time::Duration};

pub trait StatsUpdater {
    fn inc_blocks(&self, blocks: u64);
    fn inc_bytes(&self, bytes: u64);
    fn inc_duration1(&self, time: Duration);
    fn inc_duration2(&self, time: Duration);
}

pub trait StatsGetter {
    fn get_blocks(&self) -> u64;
    fn get_bytes(&self) -> u64;
    fn get_duration1(&self) -> Duration;
    fn get_duration2(&self) -> Duration;
}

#[derive(Debug, Default)]
pub struct Stats {
    pub bytes: u64,
    pub blocks: u64,
    pub duration1: Duration,
    pub duration2: Duration,
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

   fn inc_duration1(&self, time: Duration) {
        let mut stats = self.lock().unwrap();
        stats.duration1 += time;
   }

    fn inc_duration2(&self, time: Duration) {
          let mut stats = self.lock().unwrap();
          stats.duration2 += time;
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
    fn get_duration1(&self) -> Duration {
        let stats = self.lock().unwrap();
        stats.duration1
    }
    fn get_duration2(&self) -> Duration {
        let stats = self.lock().unwrap();
        stats.duration2
    }
}