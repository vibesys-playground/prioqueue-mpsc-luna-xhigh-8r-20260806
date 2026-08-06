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
const SLOT_WRITING: u8 = 1;
const SLOT_READY: u8 = 2;

// MPSC occupancy is represented by exactly `capacity` independent atomic
// tokens. The owner and phase are in the same atomic word, so claiming a token
// is the global capacity reservation linearization point. A RESERVED token is
// already backed by a fully copied lane item and can be published by the sole
// consumer if its producer pauses. There is deliberately no queue-wide lock or
// spin-wait in this protocol.
const TOKEN_RESERVED_TAG: usize = 1usize << (usize::BITS - 2);
const TOKEN_PUBLISHED_TAG: usize = 2usize << (usize::BITS - 2);
const TOKEN_TAG_MASK: usize = 3usize << (usize::BITS - 2);
const TOKEN_OWNER_MASK: usize = !TOKEN_TAG_MASK;
const TOKEN_FREE: usize = 0;

struct MpscOccupancyToken {
    state: AtomicUsize,
}

impl MpscOccupancyToken {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(TOKEN_FREE),
        }
    }
}

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

// A bounded SPSC publication lane: its producer is the producer handle for
// this lane and its reader is the sole MPSC consumer.
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
                    SLOT_WRITING,
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
    // These tokens are the sole authority for MPSC occupancy and publication.
    // A producer claims one only after its lane slot is fully copied and READY.
    // The consumer treats either RESERVED or PUBLISHED as visible.
    occupancy_tokens: Box<[MpscOccupancyToken]>,
    lanes: Box<[MpscLane]>,
}

#[derive(Clone, Copy)]
struct MpscCandidate {
    priority: u64,
    token: usize,
    lane: usize,
    slot: usize,
}

struct MpscConsumerState;

impl MpscConsumerState {
    fn new() -> Self {
        Self
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

        let owner_count = lane_count
            .checked_mul(slot_count)
            .ok_or(STATUS_INTERNAL_ERROR)?;
        if owner_count > TOKEN_OWNER_MASK {
            return Err(STATUS_INVALID);
        }
        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(lane_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..lane_count {
            lanes.push(MpscLane::new(slot_count)?);
        }

        let mut occupancy_tokens = Vec::new();
        occupancy_tokens
            .try_reserve_exact(capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..capacity {
            occupancy_tokens.push(MpscOccupancyToken::new());
        }

        Ok(Self {
            capacity,
            max_value_size,
            occupancy_tokens: occupancy_tokens.into_boxed_slice(),
            lanes: lanes.into_boxed_slice(),
        })
    }

    fn owner_code(&self, lane: usize, slot: usize) -> Result<usize, u32> {
        let linear = lane
            .checked_mul(self.lanes[lane].slots.len())
            .and_then(|value| value.checked_add(slot))
            .and_then(|value| value.checked_add(1))
            .ok_or(STATUS_INTERNAL_ERROR)?;
        if linear > TOKEN_OWNER_MASK {
            Err(STATUS_INTERNAL_ERROR)
        } else {
            Ok(linear)
        }
    }

    fn decode_owner(&self, owner: usize) -> Result<(usize, usize), u32> {
        if owner == 0 {
            return Err(STATUS_INTERNAL_ERROR);
        }
        let linear = owner - 1;
        let lane = linear / self.lanes[0].slots.len();
        let slot = linear % self.lanes[0].slots.len();
        if lane >= self.lanes.len() {
            return Err(STATUS_INTERNAL_ERROR);
        }
        Ok((lane, slot))
    }

    fn claim_occupancy_token(&self, owner: usize) -> Option<usize> {
        // There is intentionally no separate count or publication hint here.
        // The FREE->RESERVED CAS is the only occupancy reservation point; if
        // every token fails that CAS, FULL can be linearized at the final
        // failed scan without depending on stale derived state.
        for token_index in 0..self.capacity {
            let token = &self.occupancy_tokens[token_index];
            if token
                .state
                .compare_exchange(
                    TOKEN_FREE,
                    owner | TOKEN_RESERVED_TAG,
                    AtomicOrdering::AcqRel,
                    AtomicOrdering::Acquire,
                )
                .is_ok()
            {
                return Some(token_index);
            }
        }
        None
    }

    fn try_enqueue_impl(
        &self,
        producer_id: u32,
        priority: u64,
        data: &[u8],
        pause_after_lane_reservation: Option<&dyn Fn()>,
        pause_after_occupancy_reservation: Option<&dyn Fn()>,
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

        // The item is complete before it can consume one of the global
        // capacity tokens. A RESERVED token therefore always has safe data for
        // another operation to publish.
        slot.state.store(SLOT_READY, AtomicOrdering::Release);
        let owner = match self.owner_code(producer_id as usize, slot_index) {
            Ok(owner) => owner,
            Err(status) => {
                let item = unsafe { (*slot.item.get()).take() };
                drop(item);
                slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
                return status;
            }
        };
        let Some(token_index) = self.claim_occupancy_token(owner) else {
            let item = unsafe { (*slot.item.get()).take() };
            drop(item);
            slot.state.store(SLOT_EMPTY, AtomicOrdering::Release);
            return STATUS_FULL;
        };

        if let Some(pause) = pause_after_occupancy_reservation {
            pause();
        }

        // Publication is best effort. The consumer may have selected and
        // released this token while this producer was paused, in which case
        // the CAS simply fails. The successful claim above is still the
        // enqueue's linearization point and must always produce STATUS_OK.
        let reserved_state = owner | TOKEN_RESERVED_TAG;
        let published_state = owner | TOKEN_PUBLISHED_TAG;
        let _ = self.occupancy_tokens[token_index].state.compare_exchange(
            reserved_state,
            published_state,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        );
        STATUS_OK
    }

    fn try_enqueue(&self, producer_id: u32, priority: u64, data: &[u8]) -> u32 {
        self.try_enqueue_impl(producer_id, priority, data, None, None)
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
        self.try_enqueue_impl(producer_id, priority, data, Some(&pause), None)
    }

    #[cfg(test)]
    fn try_enqueue_paused_after_occupancy_reservation(
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
        self.try_enqueue_impl(producer_id, priority, data, None, Some(&pause))
    }

    fn find_minimum_ready_token(&self) -> Option<MpscCandidate> {
        let mut minimum = None;
        for token_index in 0..self.capacity {
            let state = self.occupancy_tokens[token_index]
                .state
                .load(AtomicOrdering::Acquire);
            if !matches!(
                state & TOKEN_TAG_MASK,
                TOKEN_RESERVED_TAG | TOKEN_PUBLISHED_TAG
            ) {
                continue;
            }

            let owner = state & TOKEN_OWNER_MASK;
            let Ok((lane_index, slot_index)) = self.decode_owner(owner) else {
                continue;
            };
            let slot = &self.lanes[lane_index].slots[slot_index];
            if slot.state.load(AtomicOrdering::Acquire) != SLOT_READY {
                continue;
            }
            let Some(priority) = (unsafe { (*slot.item.get()).as_ref() }).map(|item| item.priority)
            else {
                continue;
            };

            let candidate = MpscCandidate {
                priority,
                token: token_index,
                lane: lane_index,
                slot: slot_index,
            };
            if minimum
                .map(|current: MpscCandidate| {
                    (candidate.priority, candidate.token) < (current.priority, current.token)
                })
                .unwrap_or(true)
            {
                minimum = Some(candidate);
            }
        }
        minimum
    }

    fn try_dequeue(
        &self,
        output: &mut [u8],
        _consumer: &mut MpscConsumerState,
    ) -> Result<usize, u32> {
        let Some(candidate) = self.find_minimum_ready_token() else {
            return Err(STATUS_EMPTY);
        };
        let slot = &self.lanes[candidate.lane].slots[candidate.slot];
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
        // Do not make the token FREE until the lane slot is empty. A producer
        // may claim the token immediately and must never race a consumer still
        // reading or clearing that slot.
        self.occupancy_tokens[candidate.token]
            .state
            .store(TOKEN_FREE, AtomicOrdering::Release);
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
            Backend::Mpsc(_) => Some(MpscConsumerState::new()),
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

    #[cfg(test)]
    fn try_enqueue_paused_after_occupancy_reservation(
        &self,
        priority: u64,
        data: &[u8],
        reached: &std::sync::Barrier,
        resume: &std::sync::Barrier,
    ) -> u32 {
        match self.backend.as_ref() {
            Backend::Mpsc(state) => state.try_enqueue_paused_after_occupancy_reservation(
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
    fn mpsc_paused_after_token_claim_does_not_block_or_fail_after_reuse() {
        use std::sync::{mpsc, Arc as TestArc, Barrier};
        use std::thread;
        use std::time::Duration;

        let queue = Queue::new(1, 8, 3, 1).unwrap();
        let producer0 = queue.producer(0).unwrap();
        let producer1 = queue.producer(1).unwrap();
        let producer2 = queue.producer(2).unwrap();
        let consumer = queue.consumer(0).unwrap();
        let mut consumer_reuse = queue.consumer(0).unwrap();
        let reached = TestArc::new(Barrier::new(2));
        let resume = TestArc::new(Barrier::new(2));
        let producer_reached = TestArc::clone(&reached);
        let producer_resume = TestArc::clone(&resume);

        thread::scope(|scope| {
            let paused = scope.spawn(move || {
                producer0.try_enqueue_paused_after_occupancy_reservation(
                    10,
                    b"paused",
                    &producer_reached,
                    &producer_resume,
                )
            });
            reached.wait();

            let (enqueue_tx, enqueue_rx) = mpsc::channel();
            scope.spawn(move || {
                enqueue_tx
                    .send(producer1.try_enqueue(1, b"other"))
                    .unwrap();
            });
            assert_eq!(
                enqueue_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                STATUS_FULL
            );

            let (dequeue_tx, dequeue_rx) = mpsc::channel();
            scope.spawn(move || {
                let mut consumer = consumer;
                let mut output = [0_u8; 8];
                let result = consumer.try_dequeue(&mut output);
                dequeue_tx.send((result, output)).unwrap();
            });
            let (result, output) = dequeue_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(result, Ok(6));
            assert_eq!(&output[..6], b"paused");

            // Reuse and drain the token before the original producer resumes.
            // Its best-effort publication CAS must then fail harmlessly, and
            // its already-successful enqueue must still return STATUS_OK.
            assert_eq!(producer2.try_enqueue(1, b"reused"), STATUS_OK);
            let mut output = [0_u8; 8];
            assert_eq!(consumer_reuse.try_dequeue(&mut output), Ok(6));
            assert_eq!(&output[..6], b"reused");
            assert_eq!(consumer_reuse.try_dequeue(&mut output), Err(STATUS_EMPTY));

            resume.wait();
            assert_eq!(paused.join().unwrap(), STATUS_OK);
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

    #[test]
    fn mpsc_repeated_small_capacity_stress_conserves_and_drains() {
        use std::sync::{Arc as TestArc, Mutex as TestMutex};
        use std::thread;

        const PRODUCERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = 128;
        const REPETITIONS: usize = 8;

        for capacity in [1_usize, 2, 3] {
            for repetition in 0..REPETITIONS {
                let queue = Queue::new(capacity as u64, 8, PRODUCERS as u32, 1).unwrap();
                let producers = (0..PRODUCERS)
                    .map(|id| (id, queue.producer(id as u32).unwrap()))
                    .collect::<Vec<_>>();
                let consumer = queue.consumer(0).unwrap();
                let seen = TestArc::new(TestMutex::new(vec![false;
                    PRODUCERS * ITEMS_PER_PRODUCER
                ]));

                thread::scope(|scope| {
                    for (producer_id, producer) in producers {
                        scope.spawn(move || {
                            for sequence in 0..ITEMS_PER_PRODUCER {
                                let token = ((producer_id as u64) << 32) | sequence as u64;
                                let priority = (sequence as u64 * 13
                                    + producer_id as u64 * 7
                                    + repetition as u64)
                                    % 19;
                                loop {
                                    match producer.try_enqueue(priority, &token.to_le_bytes()) {
                                        STATUS_OK => break,
                                        STATUS_FULL => thread::yield_now(),
                                        status => panic!(
                                            "capacity {}, repetition {}: unexpected enqueue status {}",
                                            capacity,
                                            repetition,
                                            status
                                        ),
                                    }
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
                                    result => panic!(
                                        "capacity {}, repetition {}: unexpected dequeue result {:?}",
                                        capacity,
                                        repetition,
                                        result
                                    ),
                                }
                            }
                        }
                    });
                });

                assert!(seen.lock().unwrap().iter().all(|item| *item));
                let mut drain = queue.consumer(0).unwrap();
                let mut output = [0_u8; 8];
                assert_eq!(drain.try_dequeue(&mut output), Err(STATUS_EMPTY));
            }
        }
    }
}
