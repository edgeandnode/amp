use std::collections::VecDeque;

use crate::SolanaSlotAndBlock;

/// TODO: Explain how this size is calculated (N * 1.5, where N is number of new Solana
/// blocks produced in the time it takes to refresh archived blocks).
const SOLANA_SUBSCRIPTION_RING_BUFFER_SIZE: usize = 1024;

/// A ring buffer for storing recent Solana slots and their corresponding confirmed blocks.
pub struct SolanaSlotRingBuffer(VecDeque<SolanaSlotAndBlock>);

impl SolanaSlotRingBuffer {
    pub fn new() -> Self {
        let inner = VecDeque::with_capacity(SOLANA_SUBSCRIPTION_RING_BUFFER_SIZE);
        Self(inner)
    }

    /// Pushes a new slot and block into the ring buffer. If the buffer is full, the oldest entry is removed.
    pub fn push(&mut self, item: SolanaSlotAndBlock) {
        if self.0.len() == self.0.capacity() {
            debug_assert_eq!(self.0.capacity(), SOLANA_SUBSCRIPTION_RING_BUFFER_SIZE);
            self.0.pop_front();
        }
        self.0.push_back(item);
    }

    /// Pops the oldest entry from the ring buffer.
    pub fn pop(&mut self) -> Option<SolanaSlotAndBlock> {
        self.0.pop_front()
    }

    /// Peeks at the oldest entry in the ring buffer without removing it.
    pub fn peek(&self) -> Option<&SolanaSlotAndBlock> {
        self.0.front()
    }

    /// Returns a clone of the current contents of the ring buffer.
    pub fn to_vec(&self) -> Vec<SolanaSlotAndBlock> {
        self.0.iter().cloned().collect()
    }
}

impl Default for SolanaSlotRingBuffer {
    fn default() -> Self {
        Self::new()
    }
}
