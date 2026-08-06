#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

const ABI_VERSION: u32 = 1;
const STATUS_OK: u32 = 0;
const STATUS_FULL: u32 = 1;
const STATUS_EMPTY: u32 = 2;
const STATUS_INVALID: u32 = 3;
const STATUS_INTERNAL_ERROR: u32 = 4;

struct Entry {
    priority: u64,
    sequence: u64,
    value: Vec<u8>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

struct Values {
    next_sequence: u64,
    heap: BinaryHeap<Entry>,
}

struct QueueState {
    capacity: usize,
    max_value_size: usize,
    values: Mutex<Values>,
}

struct MpscItem {
    priority: u64,
    value: Vec<u8>,
}

const SLOT_EMPTY: u8 = 0;
const SLOT_PUBLISHED: u8 = 1;

struct MpscSlot {
    state: AtomicU8,
    item: UnsafeCell<Option<MpscItem>>,
}

impl MpscSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            item: UnsafeCell::new(None),
        }
    }
}

// Each slot is accessed by exactly one producer and the sole consumer. The
// state release/acquire handoff makes the non-atomic item access safe.
unsafe impl Sync for MpscSlot {}

struct MpscLane {
    slots: Box<[MpscSlot]>,
    next_slot: AtomicUsize,
}

impl MpscLane {
    fn new(slot_count: usize) -> Result<Self, u32> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..slot_count {
            slots.push(MpscSlot::new());
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
            next_slot: AtomicUsize::new(0),
        })
    }

    fn claim_empty_slot(&self) -> Option<&MpscSlot> {
        let slot_count = self.slots.len();
        let start = self.next_slot.load(AtomicOrdering::Relaxed) % slot_count;
        for offset in 0..slot_count {
            let index = (start + offset) % slot_count;
            let slot = &self.slots[index];
            if slot.state.load(AtomicOrdering::Acquire) == SLOT_EMPTY {
                self.next_slot
                    .store((index + 1) % slot_count, AtomicOrdering::Relaxed);
                return Some(slot);
            }
        }
        None
    }
}

struct MpscState {
    capacity: usize,
    max_value_size: usize,
    occupied: AtomicUsize,
    lanes: Box<[MpscLane]>,
}

impl MpscState {
    fn new(
        capacity: usize,
        max_value_size: usize,
        producer_count: u32,
    ) -> Result<Self, u32> {
        let slot_count = capacity
            .checked_add(1)
            .ok_or(STATUS_INTERNAL_ERROR)?;
        let lane_count = usize::try_from(producer_count).map_err(|_| STATUS_INVALID)?;
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(lane_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..lane_count {
            lanes.push(MpscLane::new(slot_count)?);
        }
        Ok(Self {
            capacity,
            max_value_size,
            occupied: AtomicUsize::new(0),
            lanes: lanes.into_boxed_slice(),
        })
    }

    fn reserve_occupancy(&self) -> bool {
        let mut occupied = self.occupied.load(AtomicOrdering::Acquire);
        loop {
            if occupied >= self.capacity {
                return false;
            }
            match self.occupied.compare_exchange_weak(
                occupied,
                occupied + 1,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => occupied = actual,
            }
        }
    }

    fn release_occupancy(&self) {
        let previous = self.occupied.fetch_sub(1, AtomicOrdering::Release);
        debug_assert!(previous > 0);
    }

    fn try_enqueue(&self, producer_id: u32, priority: u64, data: &[u8]) -> u32 {
        if data.len() > self.max_value_size {
            return STATUS_INVALID;
        }
        if !self.reserve_occupancy() {
            return STATUS_FULL;
        }

        let mut value = Vec::new();
        if value.try_reserve_exact(data.len()).is_err() {
            self.release_occupancy();
            return STATUS_INTERNAL_ERROR;
        }
        value.extend_from_slice(data);

        let lane = &self.lanes[producer_id as usize];
        let Some(slot) = lane.claim_empty_slot() else {
            // A successful reservation guarantees a free slot because every
            // lane has one more slot than the total queue capacity. Releasing
            // the reservation keeps a failed internal path leak-free.
            self.release_occupancy();
            return STATUS_INTERNAL_ERROR;
        };
        unsafe {
            *slot.item.get() = Some(MpscItem { priority, value });
        }
        slot.state
            .store(SLOT_PUBLISHED, AtomicOrdering::Release);
        STATUS_OK
    }

    fn minimum_published_slot(&self) -> Result<Option<&MpscSlot>, u32> {
        let mut best = None;
        let mut best_priority = 0_u64;
        for lane in self.lanes.iter() {
            for slot in lane.slots.iter() {
                if slot.state.load(AtomicOrdering::Acquire) != SLOT_PUBLISHED {
                    continue;
                }
                let priority = unsafe {
                    (*slot.item.get())
                        .as_ref()
                        .ok_or(STATUS_INTERNAL_ERROR)?
                        .priority
                };
                if best.is_none() || priority < best_priority {
                    best = Some(slot);
                    best_priority = priority;
                }
            }
        }
        Ok(best)
    }

    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        let Some(slot) = self.minimum_published_slot()? else {
            return Err(STATUS_EMPTY);
        };
        let value_length = unsafe {
            (*slot.item.get())
                .as_ref()
                .ok_or(STATUS_INTERNAL_ERROR)?
                .value
                .len()
        };
        if value_length > output.len() {
            return Err(STATUS_INVALID);
        }

        let item = unsafe { (*slot.item.get()).take() }.ok_or(STATUS_INTERNAL_ERROR)?;
        output[..value_length].copy_from_slice(&item.value);
        drop(item);
        slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
        self.release_occupancy();
        Ok(value_length)
    }
}

enum Backend {
    Generic(QueueState),
    Mpsc(MpscState),
}

pub struct Queue {
    producer_count: u32,
    consumer_count: u32,
    backend: Arc<Backend>,
}

impl Queue {
    fn new(
        capacity: u64,
        max_value_size: u64,
        producer_count: u32,
        consumer_count: u32,
    ) -> Result<Self, u32> {
        if capacity == 0 || max_value_size == 0 || producer_count == 0 || consumer_count == 0 {
            return Err(STATUS_INVALID);
        }
        let capacity = usize::try_from(capacity).map_err(|_| STATUS_INVALID)?;
        let max_value_size = usize::try_from(max_value_size).map_err(|_| STATUS_INVALID)?;
        let backend = if producer_count > 1 && consumer_count == 1 {
            Backend::Mpsc(MpscState::new(capacity, max_value_size, producer_count)?)
        } else {
            let mut heap = BinaryHeap::new();
            heap.try_reserve_exact(capacity)
                .map_err(|_| STATUS_INTERNAL_ERROR)?;
            Backend::Generic(QueueState {
                capacity,
                max_value_size,
                values: Mutex::new(Values {
                    next_sequence: 0,
                    heap,
                }),
            })
        };
        Ok(Self {
            producer_count,
            consumer_count,
            backend: Arc::new(backend),
        })
    }

    fn producer(&self, id: u32) -> Result<Producer, u32> {
        (id < self.producer_count)
            .then(|| Producer {
                backend: Arc::clone(&self.backend),
                id,
            })
            .ok_or(STATUS_INVALID)
    }

    fn consumer(&self, id: u32) -> Result<Consumer, u32> {
        (id < self.consumer_count)
            .then(|| Consumer {
                backend: Arc::clone(&self.backend),
            })
            .ok_or(STATUS_INVALID)
    }
}

pub struct Producer {
    backend: Arc<Backend>,
    id: u32,
}

impl Producer {
    fn try_enqueue(&self, priority: u64, data: &[u8]) -> u32 {
        match self.backend.as_ref() {
            Backend::Generic(state) => {
                if data.len() > state.max_value_size {
                    return STATUS_INVALID;
                }
                let Ok(mut values) = state.values.lock() else {
                    return STATUS_INTERNAL_ERROR;
                };
                if values.heap.len() == state.capacity {
                    return STATUS_FULL;
                }
                let mut value = Vec::new();
                if value.try_reserve_exact(data.len()).is_err() {
                    return STATUS_INTERNAL_ERROR;
                }
                value.extend_from_slice(data);
                let sequence = values.next_sequence;
                values.next_sequence = values.next_sequence.wrapping_add(1);
                values.heap.push(Entry {
                    priority,
                    sequence,
                    value,
                });
                STATUS_OK
            }
            Backend::Mpsc(state) => state.try_enqueue(self.id, priority, data),
        }
    }
}

pub struct Consumer {
    backend: Arc<Backend>,
}

impl Consumer {
    fn try_dequeue(&self, output: &mut [u8]) -> Result<usize, u32> {
        match self.backend.as_ref() {
            Backend::Generic(state) => {
                let Ok(mut values) = state.values.lock() else {
                    return Err(STATUS_INTERNAL_ERROR);
                };
                let Some(entry) = values.heap.peek() else {
                    return Err(STATUS_EMPTY);
                };
                if entry.value.len() > output.len() {
                    return Err(STATUS_INVALID);
                }
                output[..entry.value.len()].copy_from_slice(&entry.value);
                let length = entry.value.len();
                values.heap.pop();
                Ok(length)
            }
            Backend::Mpsc(state) => state.try_dequeue(output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_core_is_bounded_priority_queue() {
        let queue = Queue::new(3, 8, 1, 1).unwrap();
        let producer = queue.producer(0).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer.try_enqueue(7, b"later"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"first"), STATUS_OK);
        assert_eq!(producer.try_enqueue(2, b"second"), STATUS_OK);
        assert_eq!(producer.try_enqueue(0, b"full"), STATUS_FULL);

        let mut output = [0_u8; 8];
        for expected in [b"first".as_slice(), b"second", b"later"] {
            assert_eq!(consumer.try_dequeue(&mut output), Ok(expected.len()));
            assert_eq!(&output[..expected.len()], expected);
        }
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
    }

    #[test]
    fn mpsc_backend_copies_and_selects_minimum_priority() {
        let queue = Queue::new(4, 8, 2, 1).unwrap();
        let producer0 = queue.producer(0).unwrap();
        let producer1 = queue.producer(1).unwrap();
        let consumer = queue.consumer(0).unwrap();
        assert_eq!(producer0.try_enqueue(9, b"later"), STATUS_OK);
        assert_eq!(producer0.try_enqueue(1, b"first"), STATUS_OK);
        assert_eq!(producer1.try_enqueue(5, b"middle"), STATUS_OK);
        assert_eq!(producer1.try_enqueue(3, b"next"), STATUS_OK);
        assert_eq!(producer0.try_enqueue(0, b"full"), STATUS_FULL);

        let mut output = [0_u8; 8];
        for expected in [b"first".as_slice(), b"next", b"middle", b"later"] {
            let length = consumer.try_dequeue(&mut output).unwrap();
            assert_eq!(&output[..length], expected);
        }
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
    }

    #[test]
    fn mpsc_backend_conserves_values_under_concurrency() {
        use std::sync::{Arc as TestArc, Mutex as TestMutex};
        use std::thread;

        const PRODUCERS: usize = 3;
        const ITEMS_PER_PRODUCER: usize = 2_000;
        let queue = Queue::new(8, 8, PRODUCERS as u32, 1).unwrap();
        let producers = (0..PRODUCERS)
            .map(|id| (id, queue.producer(id as u32).unwrap()))
            .collect::<Vec<_>>();
        let consumer = queue.consumer(0).unwrap();
        let seen = TestArc::new(TestMutex::new(vec![false; PRODUCERS * ITEMS_PER_PRODUCER]));

        thread::scope(|scope| {
            for (producer_id, producer) in producers {
                scope.spawn(move || {
                    let mut successes = 0;
                    while successes < ITEMS_PER_PRODUCER {
                        let token = ((producer_id as u64) << 32) | successes as u64;
                        let status = producer.try_enqueue(
                            (successes as u64 * 17 + producer_id as u64) % 31,
                            &token.to_le_bytes(),
                        );
                        match status {
                            STATUS_OK => successes += 1,
                            STATUS_FULL => thread::yield_now(),
                            status => panic!("unexpected enqueue status {status}"),
                        }
                    }
                });
            }

            let seen = TestArc::clone(&seen);
            scope.spawn(move || {
                let consumer = consumer;
                let mut output = [0_u8; 8];
                for _ in 0..PRODUCERS * ITEMS_PER_PRODUCER {
                    loop {
                        match consumer.try_dequeue(&mut output) {
                            Ok(8) => {
                                let token = u64::from_le_bytes(output);
                                let producer_id = (token >> 32) as usize;
                                let sequence = (token & u64::from(u32::MAX)) as usize;
                                assert!(producer_id < PRODUCERS);
                                assert!(sequence < ITEMS_PER_PRODUCER);
                                let index = producer_id * ITEMS_PER_PRODUCER + sequence;
                                let mut seen = seen.lock().unwrap();
                                assert!(!seen[index]);
                                seen[index] = true;
                                break;
                            }
                            Err(STATUS_EMPTY) => thread::yield_now(),
                            result => panic!("unexpected dequeue result {result:?}"),
                        }
                    }
                }
            });
        });

        assert!(seen.lock().unwrap().iter().all(|item| *item));
    }
}
