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
const SLOT_RESERVED: u8 = 1;
const SLOT_PUBLISHED: u8 = 2;

// The high bit is an internal commit lock. The remaining bits contain the
// number of committed items. MPSC producers never expose a count change until
// the corresponding lane slot is ready for acquire readers. Allocation and
// copying happen before this short atomic commit section; the generic fallback
// remains the only path with a queue-wide Mutex.
const OCCUPANCY_LOCK: usize = 1usize << (usize::BITS - 1);
const OCCUPANCY_MASK: usize = !OCCUPANCY_LOCK;

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
    publication_slots: Box<[AtomicUsize]>,
    next_slot: AtomicUsize,
    next_publication: AtomicUsize,
    publication_tail: AtomicUsize,
    publication_head: AtomicUsize,
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
        let mut publication_slots = Vec::new();
        publication_slots
            .try_reserve_exact(slot_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..slot_count {
            publication_slots.push(AtomicUsize::new(usize::MAX));
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
            publication_slots: publication_slots.into_boxed_slice(),
            next_slot: AtomicUsize::new(0),
            next_publication: AtomicUsize::new(0),
            publication_tail: AtomicUsize::new(0),
            publication_head: AtomicUsize::new(0),
        })
    }

    fn claim_empty_slot(&self) -> Option<(usize, &MpscSlot)> {
        let slot_count = self.slots.len();
        let start = self.next_slot.load(AtomicOrdering::Relaxed) % slot_count;
        for offset in 0..slot_count {
            let index = (start + offset) % slot_count;
            let slot = &self.slots[index];
            if slot
                .state
                .compare_exchange(
                    SLOT_EMPTY,
                    SLOT_RESERVED,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                self.next_slot
                    .store((index + 1) % slot_count, AtomicOrdering::Relaxed);
                return Some((index, slot));
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

#[derive(Clone, Copy)]
struct MpscCandidate {
    priority: u64,
    sequence: usize,
    lane: usize,
    slot: usize,
}

impl PartialEq for MpscCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.sequence == other.sequence
            && self.lane == other.lane
            && self.slot == other.slot
    }
}

impl Eq for MpscCandidate {}

impl PartialOrd for MpscCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MpscCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.lane.cmp(&self.lane))
            .then_with(|| other.slot.cmp(&self.slot))
    }
}

struct MpscConsumerState {
    next_seen: Vec<usize>,
    heap: BinaryHeap<MpscCandidate>,
}

impl MpscConsumerState {
    fn new(lane_count: usize, capacity: usize) -> Result<Self, u32> {
        let mut next_seen = Vec::new();
        next_seen
            .try_reserve_exact(lane_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        next_seen.resize(lane_count, 0);
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        Ok(Self { next_seen, heap })
    }
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
        if capacity >= OCCUPANCY_LOCK {
            return Err(STATUS_INVALID);
        }
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

    fn acquire_occupancy_lock(&self) -> usize {
        loop {
            let occupied = self.occupied.load(AtomicOrdering::Acquire);
            if occupied & OCCUPANCY_LOCK != 0 {
                std::hint::spin_loop();
                continue;
            }
            match self.occupied.compare_exchange_weak(
                occupied,
                occupied | OCCUPANCY_LOCK,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            ) {
                Ok(_) => return occupied | OCCUPANCY_LOCK,
                Err(_) => continue,
            }
        }
    }

    fn unlock_occupancy(&self) {
        loop {
            let occupied = self.occupied.load(AtomicOrdering::Acquire);
            if occupied & OCCUPANCY_LOCK == 0 {
                return;
            }
            if self
                .occupied
                .compare_exchange_weak(
                    occupied,
                    occupied & OCCUPANCY_MASK,
                    AtomicOrdering::Release,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    fn release_occupancy(&self) {
        loop {
            let occupied = self.occupied.load(AtomicOrdering::Acquire);
            if occupied & OCCUPANCY_LOCK != 0 {
                std::hint::spin_loop();
                continue;
            }
            let count = occupied & OCCUPANCY_MASK;
            debug_assert!(count > 0);
            if self
                .occupied
                .compare_exchange_weak(
                    occupied,
                    count.saturating_sub(1),
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    fn try_enqueue_impl(
        &self,
        producer_id: u32,
        priority: u64,
        data: &[u8],
        pause_after_lane_reservation: Option<&dyn Fn()>,
    ) -> u32 {
        if data.len() > self.max_value_size {
            return STATUS_INVALID;
        }

        let lane = &self.lanes[producer_id as usize];
        let Some((slot_index, slot)) = lane.claim_empty_slot() else {
            return STATUS_INTERNAL_ERROR;
        };
        if let Some(pause) = pause_after_lane_reservation {
            pause();
        }

        let mut value = Vec::new();
        if value.try_reserve_exact(data.len()).is_err() {
            slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
            return STATUS_INTERNAL_ERROR;
        }
        value.extend_from_slice(data);

        unsafe {
            *slot.item.get() = Some(MpscItem { priority, value });
        }
        let locked = self.acquire_occupancy_lock();
        let count = locked & OCCUPANCY_MASK;
        if count >= self.capacity {
            self.unlock_occupancy();
            slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
            let item = unsafe { (*slot.item.get()).take() };
            drop(item);
            return STATUS_FULL;
        }
        self.occupied.store(
            (count + 1) | OCCUPANCY_LOCK,
            AtomicOrdering::Release,
        );
        let publication_sequence = lane.next_publication.load(AtomicOrdering::Relaxed);
        let publication_head = lane.publication_head.load(AtomicOrdering::Acquire);
        if publication_sequence.wrapping_sub(publication_head)
            >= lane.publication_slots.len()
        {
            self.occupied.store(count | OCCUPANCY_LOCK, AtomicOrdering::Release);
            self.unlock_occupancy();
            slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
            let item = unsafe { (*slot.item.get()).take() };
            drop(item);
            return STATUS_INTERNAL_ERROR;
        }
        lane.next_publication
            .store(publication_sequence.wrapping_add(1), AtomicOrdering::Relaxed);
        slot.state.store(SLOT_PUBLISHED, AtomicOrdering::Release);
        lane.publication_slots[publication_sequence % lane.publication_slots.len()]
            .store(slot_index, AtomicOrdering::Release);
        lane.publication_tail
            .store(publication_sequence.wrapping_add(1), AtomicOrdering::Release);
        self.unlock_occupancy();
        STATUS_OK
    }

    fn try_enqueue(&self, producer_id: u32, priority: u64, data: &[u8]) -> u32 {
        self.try_enqueue_impl(producer_id, priority, data, None)
    }

    #[cfg(test)]
    fn try_enqueue_paused_after_lane_reservation(
        &self,
        producer_id: u32,
        priority: u64,
        data: &[u8],
        reached: &std::sync::Barrier,
        resume: &std::sync::Barrier,
    ) -> u32 {
        let pause = || {
            reached.wait();
            resume.wait();
        };
        self.try_enqueue_impl(producer_id, priority, data, Some(&pause))
    }

    fn ingest_published(&self, consumer: &mut MpscConsumerState) -> Result<(), u32> {
        for (lane_index, lane) in self.lanes.iter().enumerate() {
            let tail = lane.publication_tail.load(AtomicOrdering::Acquire);
            let mut next = consumer.next_seen[lane_index];
            while next < tail {
                let publication_index = next % lane.publication_slots.len();
                let slot_index = lane.publication_slots[publication_index]
                    .load(AtomicOrdering::Acquire);
                if slot_index == usize::MAX || slot_index >= lane.slots.len() {
                    return Err(STATUS_INTERNAL_ERROR);
                }
                let slot = &lane.slots[slot_index];
                if slot.state.load(AtomicOrdering::Acquire) != SLOT_PUBLISHED {
                    return Err(STATUS_INTERNAL_ERROR);
                }
                let priority = unsafe {
                    (*slot.item.get())
                        .as_ref()
                        .ok_or(STATUS_INTERNAL_ERROR)?
                        .priority
                };
                consumer.heap.push(MpscCandidate {
                    priority,
                    sequence: next,
                    lane: lane_index,
                    slot: slot_index,
                });
                lane.publication_slots[publication_index]
                    .store(usize::MAX, AtomicOrdering::Release);
                next = next.wrapping_add(1);
            }
            lane.publication_head.store(next, AtomicOrdering::Release);
            consumer.next_seen[lane_index] = tail;
        }
        Ok(())
    }

    fn minimum_published_slot<'a>(
        &'a self,
        consumer: &MpscConsumerState,
    ) -> Option<(&'a MpscSlot, MpscCandidate)> {
        let candidate = *consumer.heap.peek()?;
        let lane = self.lanes.get(candidate.lane)?;
        let slot = lane.slots.get(candidate.slot)?;
        Some((slot, candidate))
    }

    fn try_dequeue(
        &self,
        output: &mut [u8],
        consumer: &mut MpscConsumerState,
    ) -> Result<usize, u32> {
        self.ingest_published(consumer)?;
        let Some((slot, _candidate)) = self.minimum_published_slot(consumer) else {
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

        let _ = consumer.heap.pop();
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
        if id >= self.consumer_count {
            return Err(STATUS_INVALID);
        }
        let mpsc = match self.backend.as_ref() {
            Backend::Mpsc(state) => Some(MpscConsumerState::new(
                state.lanes.len(),
                state.capacity,
            )?),
            Backend::Generic(_) => None,
        };
        Ok(Consumer {
            backend: Arc::clone(&self.backend),
            mpsc,
        })
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

    #[cfg(test)]
    fn try_enqueue_paused_after_lane_reservation(
        &self,
        priority: u64,
        data: &[u8],
        reached: &std::sync::Barrier,
        resume: &std::sync::Barrier,
    ) -> u32 {
        match self.backend.as_ref() {
            Backend::Mpsc(state) => state.try_enqueue_paused_after_lane_reservation(
                self.id, priority, data, reached, resume,
            ),
            Backend::Generic(_) => STATUS_INTERNAL_ERROR,
        }
    }
}

pub struct Consumer {
    backend: Arc<Backend>,
    mpsc: Option<MpscConsumerState>,
}

impl Consumer {
    fn try_dequeue(&mut self, output: &mut [u8]) -> Result<usize, u32> {
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
            Backend::Mpsc(state) => {
                let Some(mpsc) = self.mpsc.as_mut() else {
                    return Err(STATUS_INTERNAL_ERROR);
                };
                state.try_dequeue(output, mpsc)
            }
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
        let mut consumer = queue.consumer(0).unwrap();
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
        let mut consumer = queue.consumer(0).unwrap();
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
    fn mpsc_capacity_one_hides_unpublished_lane_reservation() {
        use std::sync::{Arc as TestArc, Barrier};
        use std::thread;

        let queue = Queue::new(1, 8, 2, 1).unwrap();
        let producer0 = queue.producer(0).unwrap();
        let producer1 = queue.producer(1).unwrap();
        let mut consumer = queue.consumer(0).unwrap();
        let reached = TestArc::new(Barrier::new(2));
        let resume = TestArc::new(Barrier::new(2));
        let producer_reached = TestArc::clone(&reached);
        let producer_resume = TestArc::clone(&resume);

        thread::scope(|scope| {
            let paused = scope.spawn(move || {
                producer0.try_enqueue_paused_after_lane_reservation(
                    10,
                    b"paused",
                    &producer_reached,
                    &producer_resume,
                )
            });
            reached.wait();

            // The paused producer has only claimed its private lane slot; it
            // has not reserved queue capacity. A strict-MPSC implementation
            // must not answer FULL while the consumer would observe EMPTY.
            assert_eq!(producer1.try_enqueue(1, b"visible"), STATUS_OK);
            let mut output = [0_u8; 8];
            assert_eq!(consumer.try_dequeue(&mut output), Ok(7));
            assert_eq!(&output[..7], b"visible");
            assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));

            resume.wait();
            assert_eq!(paused.join().unwrap(), STATUS_OK);
            assert_eq!(consumer.try_dequeue(&mut output), Ok(6));
            assert_eq!(&output[..6], b"paused");
            assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
        });
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
                let mut consumer = consumer;
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
