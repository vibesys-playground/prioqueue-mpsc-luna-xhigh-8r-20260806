#![deny(unsafe_op_in_unsafe_fn)]

mod ffi;

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
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

const SLOT_EMPTY: u8 = 0;
const SLOT_WRITING: u8 = 1;
const SLOT_READY: u8 = 2;
const TOKEN_HINT_NONE: usize = usize::MAX;

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
    // Advisory reverse lookup for the token claimed by this slot. The store
    // happens before the token CAS, so a producer paused immediately after a
    // successful claim has still left the consumer a bounded way to find it.
    token_hint: AtomicUsize,
    publication_sequence: AtomicUsize,
    // This storage is allocated exactly once while the MPSC state is built.
    // The producer and sole consumer hand access to it through `state`; the
    // buffer itself is never moved, grown, taken, or dropped on an operation.
    buffer: UnsafeCell<Box<[u8]>>,
    priority: AtomicU64,
    length: AtomicUsize,
}

impl MpscSlot {
    fn new(max_value_size: usize) -> Result<Self, u32> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(max_value_size)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        buffer.resize(max_value_size, 0);
        Ok(Self {
            state: AtomicU8::new(SLOT_EMPTY),
            token_hint: AtomicUsize::new(TOKEN_HINT_NONE),
            publication_sequence: AtomicUsize::new(0),
            buffer: UnsafeCell::new(buffer.into_boxed_slice()),
            priority: AtomicU64::new(0),
            length: AtomicUsize::new(0),
        })
    }

    fn reset_for_reuse(&self) {
        self.length.store(0, AtomicOrdering::Relaxed);
        self.priority.store(0, AtomicOrdering::Relaxed);
        self.token_hint
            .store(TOKEN_HINT_NONE, AtomicOrdering::Relaxed);
        self.state.store(SLOT_EMPTY, AtomicOrdering::Release);
    }
}

// Each slot is accessed by exactly one producer and the sole consumer. The
// state release/acquire handoff makes the non-atomic buffer access safe.
unsafe impl Sync for MpscSlot {}

// A bounded SPSC publication lane: its producer is the producer handle for
// this lane and its reader is the sole MPSC consumer.
struct MpscLane {
    slots: Box<[MpscSlot]>,
    next_slot: AtomicUsize,
    // A bounded SPSC publication ring. It contains successful lane
    // publications in FIFO order; at most capacity items and one in-flight
    // publication can exist in the ring.
    publications: Box<[MpscPublicationRecord]>,
    publication_tail: AtomicUsize,
}

struct MpscPublicationRecord {
    sequence: AtomicUsize,
    slot: AtomicUsize,
}

impl MpscPublicationRecord {
    fn new() -> Self {
        Self {
            sequence: AtomicUsize::new(0),
            slot: AtomicUsize::new(0),
        }
    }
}

impl MpscLane {
    fn new(slot_count: usize, max_value_size: usize) -> Result<Self, u32> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(slot_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..slot_count {
            slots.push(MpscSlot::new(max_value_size)?);
        }
        let mut publications = Vec::new();
        publications
            .try_reserve_exact(slot_count)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        for _ in 0..slot_count {
            publications.push(MpscPublicationRecord::new());
        }
        Ok(Self {
            slots: slots.into_boxed_slice(),
            next_slot: AtomicUsize::new(0),
            publications: publications.into_boxed_slice(),
            publication_tail: AtomicUsize::new(0),
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

    fn begin_publication(&self, slot_index: usize) -> usize {
        let sequence = self.publication_tail.load(AtomicOrdering::Relaxed);
        let slot = &self.slots[slot_index];
        slot.publication_sequence
            .store(sequence, AtomicOrdering::Release);
        let record = &self.publications[sequence % self.publications.len()];
        record.slot.store(slot_index, AtomicOrdering::Relaxed);
        record.sequence.store(sequence, AtomicOrdering::Release);
        // This is an advisory producer head. The consumer will not advance
        // its read cursor until the authoritative occupancy token validates.
        self.publication_tail
            .store(sequence.wrapping_add(1), AtomicOrdering::Release);
        sequence
    }

    fn rollback_publication(&self, sequence: usize) {
        // Only the lane producer writes this cursor. A consumer that saw the
        // temporary tail will leave its read cursor at `sequence` because the
        // token was not valid, so reusing the record is safe.
        self.publication_tail
            .store(sequence, AtomicOrdering::Release);
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

#[derive(Clone, Copy, PartialEq, Eq)]
struct MpscCandidate {
    priority: u64,
    token: usize,
    lane: usize,
    slot: usize,
    publication: usize,
}

impl Ord for MpscCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.token.cmp(&self.token))
            .then_with(|| other.lane.cmp(&self.lane))
            .then_with(|| other.slot.cmp(&self.slot))
            .then_with(|| other.publication.cmp(&self.publication))
    }
}

impl PartialOrd for MpscCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct MpscConsumerState {
    // One FIFO read cursor per producer publication lane. These cursors are
    // consumer-local and are never used as occupancy authority.
    publication_reads: Vec<usize>,
    // A candidate is present in this table exactly while its heap record is
    // live. It prevents duplicate heap entries and makes stale entries
    // discardable after slot reuse.
    indexed_slots: Vec<Vec<Option<MpscCandidate>>>,
    heap: BinaryHeap<MpscCandidate>,
}

impl MpscConsumerState {
    fn new(lane_count: usize, slot_count: usize) -> Result<Self, u32> {
        let heap_capacity = lane_count
            .checked_mul(slot_count)
            .ok_or(STATUS_INTERNAL_ERROR)?;
        let mut heap = BinaryHeap::new();
        heap.try_reserve_exact(heap_capacity)
            .map_err(|_| STATUS_INTERNAL_ERROR)?;
        Ok(Self {
            publication_reads: vec![0; lane_count],
            indexed_slots: vec![vec![None; slot_count]; lane_count],
            heap,
        })
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
            lanes.push(MpscLane::new(slot_count, max_value_size)?);
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

    fn claim_occupancy_token(&self, owner: usize, slot: &MpscSlot) -> Option<usize> {
        // There is intentionally no separate count or publication hint here.
        // The FREE->RESERVED CAS is the only occupancy reservation point; if
        // every token fails that CAS, FULL can be linearized at the final
        // failed scan without depending on stale derived state.
        for token_index in 0..self.capacity {
            let token = &self.occupancy_tokens[token_index];
            // Publish the candidate before attempting the CAS. The hint is
            // deliberately advisory; the consumer validates the token's
            // owner/state before indexing the slot.
            slot.token_hint
                .store(token_index, AtomicOrdering::Release);
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

        unsafe {
            (&mut *slot.buffer.get())[..data.len()].copy_from_slice(data);
        }
        slot.priority.store(priority, AtomicOrdering::Relaxed);
        slot.length
            .store(data.len(), AtomicOrdering::Relaxed);

        // The item is complete before it can consume one of the global
        // capacity tokens. A RESERVED token therefore always has safe data for
        // another operation to publish.
        slot.state.store(SLOT_READY, AtomicOrdering::Release);
        let owner = match self.owner_code(producer_id as usize, slot_index) {
            Ok(owner) => owner,
            Err(status) => {
                slot.reset_for_reuse();
                return status;
            }
        };
        let lane = &self.lanes[producer_id as usize];
        let publication = lane.begin_publication(slot_index);
        let Some(token_index) = self.claim_occupancy_token(owner, slot) else {
            lane.rollback_publication(publication);
            slot.reset_for_reuse();
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

    fn candidate_from_slot(
        &self,
        lane_index: usize,
        slot_index: usize,
    ) -> Option<MpscCandidate> {
        let lane = &self.lanes[lane_index];
        let slot = &lane.slots[slot_index];
        if slot.state.load(AtomicOrdering::Acquire) != SLOT_READY {
            return None;
        }

        let owner = self.owner_code(lane_index, slot_index).ok()?;
        let publication = slot
            .publication_sequence
            .load(AtomicOrdering::Acquire);
        let mut token_index = slot.token_hint.load(AtomicOrdering::Acquire);
        for _ in 0..2 {
            if token_index >= self.capacity {
                return None;
            }
            let state = self.occupancy_tokens[token_index]
                .state
                .load(AtomicOrdering::Acquire);
            if state & TOKEN_OWNER_MASK == owner
                && matches!(
                    state & TOKEN_TAG_MASK,
                    TOKEN_RESERVED_TAG | TOKEN_PUBLISHED_TAG
                )
            {
                return Some(MpscCandidate {
                    priority: slot.priority.load(AtomicOrdering::Acquire),
                    token: token_index,
                    lane: lane_index,
                    slot: slot_index,
                    publication,
                });
            }
            // The first hint load can race the release store immediately
            // before the producer's successful token CAS. Re-read the
            // advisory hint once after observing the token state so a fresh
            // owner is not mistaken for an empty lane head.
            let refreshed = slot.token_hint.load(AtomicOrdering::Acquire);
            if refreshed == token_index {
                return None;
            }
            token_index = refreshed;
        }
        None
    }

    fn candidate_from_authoritative_token(
        &self,
        lane_index: usize,
        slot_index: usize,
    ) -> Option<MpscCandidate> {
        let lane = &self.lanes[lane_index];
        let slot = &lane.slots[slot_index];
        if slot.state.load(AtomicOrdering::Acquire) != SLOT_READY {
            return None;
        }
        let owner = self.owner_code(lane_index, slot_index).ok()?;
        let publication = slot
            .publication_sequence
            .load(AtomicOrdering::Acquire);
        for token_index in 0..self.capacity {
            let state = self.occupancy_tokens[token_index]
                .state
                .load(AtomicOrdering::Acquire);
            if state & TOKEN_OWNER_MASK != owner
                || !matches!(
                    state & TOKEN_TAG_MASK,
                    TOKEN_RESERVED_TAG | TOKEN_PUBLISHED_TAG
                )
            {
                continue;
            }
            return Some(MpscCandidate {
                priority: slot.priority.load(AtomicOrdering::Acquire),
                token: token_index,
                lane: lane_index,
                slot: slot_index,
                publication,
            });
        }
        None
    }

    fn index_candidate(&self, consumer: &mut MpscConsumerState, candidate: MpscCandidate) {
        let indexed = &mut consumer.indexed_slots[candidate.lane][candidate.slot];
        if *indexed != Some(candidate) {
            *indexed = Some(candidate);
            consumer.heap.push(candidate);
        }
    }

    fn repair_lane_index(&self, consumer: &mut MpscConsumerState, lane_index: usize, tail: usize) {
        // This is intentionally off the ordinary path. A missing/overwritten
        // publication record causes one bounded lane/token repair pass; the
        // normal dequeue path consumes only the SPSC publication cursors.
        let mut pending: Option<usize> = None;
        for slot_index in 0..self.lanes[lane_index].slots.len() {
            if let Some(candidate) = self.candidate_from_slot(lane_index, slot_index) {
                self.index_candidate(consumer, candidate);
                continue;
            }
            let slot = &self.lanes[lane_index].slots[slot_index];
            if let Some(candidate) =
                self.candidate_from_authoritative_token(lane_index, slot_index)
            {
                self.index_candidate(consumer, candidate);
            } else if slot.state.load(AtomicOrdering::Acquire) == SLOT_READY {
                // The item is fully copied but its token hint may be stale
                // while the producer is between token attempts. Preserve the
                // oldest such sequence for the next bounded repair pass.
                let sequence = slot
                    .publication_sequence
                    .load(AtomicOrdering::Acquire);
                if sequence.wrapping_sub(tail) > usize::MAX / 2
                    || sequence == tail
                    || sequence.wrapping_add(1) == tail
                {
                    pending = Some(pending.map_or(sequence, |old| old.min(sequence)));
                }
            }
        }
        consumer.publication_reads[lane_index] = pending.unwrap_or(tail);
    }

    fn refresh_lane_index(&self, consumer: &mut MpscConsumerState, lane_index: usize) {
        let lane = &self.lanes[lane_index];
        let mut sequence = consumer.publication_reads[lane_index];
        let tail = lane.publication_tail.load(AtomicOrdering::Acquire);
        while sequence != tail {
            let record = &lane.publications[sequence % lane.publications.len()];
            if record.sequence.load(AtomicOrdering::Acquire) != sequence {
                self.repair_lane_index(consumer, lane_index, tail);
                return;
            }
            let slot_index = record.slot.load(AtomicOrdering::Relaxed);
            if slot_index >= lane.slots.len() {
                self.repair_lane_index(consumer, lane_index, tail);
                return;
            }
            let slot = &lane.slots[slot_index];
            let publication = slot
                .publication_sequence
                .load(AtomicOrdering::Acquire);
            if publication != sequence
                || slot.state.load(AtomicOrdering::Acquire) == SLOT_EMPTY
            {
                // A bounded repair may have indexed this publication before
                // its FIFO record was reached. If it was then dequeued, its
                // slot is empty (or has already been reused for a newer
                // publication), so this old record is safe to consume.
                sequence = sequence.wrapping_add(1);
                continue;
            }
            let Some(candidate) = self.candidate_from_slot(lane_index, slot_index) else {
                // The producer publishes the tail before the token CAS so a
                // paused claim is visible here. Do not advance past an
                // unvalidated record; a later refresh will observe the token.
                return;
            };
            self.index_candidate(consumer, candidate);
            sequence = sequence.wrapping_add(1);
        }
        // A repair can legitimately move the FIFO cursor to the producer
        // tail after indexing a live record out of order. Re-check the tail
        // record before declaring the lane fully indexed; this is also the
        // bounded publication witness for a producer paused after its token
        // CAS.
        if tail != 0 {
            let latest = tail.wrapping_sub(1);
            let record = &lane.publications[latest % lane.publications.len()];
            let slot_index = record.slot.load(AtomicOrdering::Relaxed);
            if record.sequence.load(AtomicOrdering::Acquire) == latest
                && slot_index < lane.slots.len()
                && lane.slots[slot_index]
                    .publication_sequence
                    .load(AtomicOrdering::Acquire)
                    == latest
            {
                if let Some(candidate) = self.candidate_from_slot(lane_index, slot_index) {
                    self.index_candidate(consumer, candidate);
                } else if lane.slots[slot_index]
                    .state
                    .load(AtomicOrdering::Acquire)
                    == SLOT_READY
                {
                    consumer.publication_reads[lane_index] = latest;
                    return;
                }
            }
        }
        consumer.publication_reads[lane_index] = sequence;
    }

    fn refresh_head_index(&self, consumer: &mut MpscConsumerState) {
        for lane_index in 0..self.lanes.len() {
            self.refresh_lane_index(consumer, lane_index);
        }
    }

    fn has_ready_publication_hint(&self) -> bool {
        // This is a bounded repair trigger, not the dequeue selection path.
        // A normal non-empty dequeue already has heap entries; an empty heap
        // with a READY lane slot means an advisory record needs repair.
        self.lanes.iter().any(|lane| {
            lane.slots
                .iter()
                .any(|slot| slot.state.load(AtomicOrdering::Acquire) == SLOT_READY)
        })
    }

    fn candidate_is_ready(&self, candidate: MpscCandidate) -> bool {
        let slot = &self.lanes[candidate.lane].slots[candidate.slot];
        if slot.state.load(AtomicOrdering::Acquire) != SLOT_READY
            || slot
                .publication_sequence
                .load(AtomicOrdering::Acquire)
                != candidate.publication
        {
            return false;
        }
        let state = self.occupancy_tokens[candidate.token]
            .state
            .load(AtomicOrdering::Acquire);
        let Ok(owner) = self.owner_code(candidate.lane, candidate.slot) else {
            return false;
        };
        if state & TOKEN_OWNER_MASK != owner
            || !matches!(
                state & TOKEN_TAG_MASK,
                TOKEN_RESERVED_TAG | TOKEN_PUBLISHED_TAG
            )
        {
            return false;
        }
        slot.priority.load(AtomicOrdering::Acquire) == candidate.priority
    }

    fn minimum_ready_candidate(&self, consumer: &mut MpscConsumerState) -> Option<MpscCandidate> {
        self.refresh_head_index(consumer);
        if consumer.heap.is_empty() && self.has_ready_publication_hint() {
            // A producer may have reused a slot after a bounded repair moved
            // the FIFO cursor to tail. The per-lane tail witness makes this a
            // repair-only path; ordinary dequeues are served by the heap.
            for lane_index in 0..self.lanes.len() {
                let tail = self.lanes[lane_index]
                    .publication_tail
                    .load(AtomicOrdering::Acquire);
                self.repair_lane_index(consumer, lane_index, tail);
            }
            self.refresh_head_index(consumer);
        }
        loop {
            let candidate = *consumer.heap.peek()?;
            if consumer.indexed_slots[candidate.lane][candidate.slot] != Some(candidate) {
                consumer.heap.pop();
                continue;
            }
            if self.candidate_is_ready(candidate) {
                return Some(candidate);
            }
            consumer.heap.pop();
            consumer.indexed_slots[candidate.lane][candidate.slot] = None;
            if let Some(replacement) = self.candidate_from_slot(candidate.lane, candidate.slot)
                .or_else(|| {
                    self.candidate_from_authoritative_token(candidate.lane, candidate.slot)
                })
            {
                self.index_candidate(consumer, replacement);
            }
            self.refresh_lane_index(consumer, candidate.lane);
        }
    }

    fn try_dequeue(
        &self,
        output: &mut [u8],
        consumer: &mut MpscConsumerState,
    ) -> Result<usize, u32> {
        let Some(candidate) = self.minimum_ready_candidate(consumer) else {
            return Err(STATUS_EMPTY);
        };
        let slot = &self.lanes[candidate.lane].slots[candidate.slot];
        let value_length = slot.length.load(AtomicOrdering::Acquire);
        if value_length > output.len() {
            return Err(STATUS_INVALID);
        }

        consumer.heap.pop();
        consumer.indexed_slots[candidate.lane][candidate.slot] = None;
        unsafe {
            output[..value_length].copy_from_slice(&(&*slot.buffer.get())[..value_length]);
        }
        slot.reset_for_reuse();
        // Do not make the token FREE until the lane slot is empty. A producer
        // may claim the token immediately and must never race a consumer still
        // reading or clearing that slot.
        self.occupancy_tokens[candidate.token]
            .state
            .store(TOKEN_FREE, AtomicOrdering::Release);
        // Refresh both newly published records and the slot just made
        // reusable before returning. This keeps the consumer-local index hot
        // without consulting every occupancy token.
        self.refresh_head_index(consumer);
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
                state.lanes[0].slots.len(),
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
    fn mpsc_undersized_output_retains_the_minimum_item() {
        let queue = Queue::new(3, 8, 2, 1).unwrap();
        let producer0 = queue.producer(0).unwrap();
        let producer1 = queue.producer(1).unwrap();
        let mut consumer = queue.consumer(0).unwrap();
        assert_eq!(producer0.try_enqueue(9, b"later"), STATUS_OK);
        assert_eq!(producer1.try_enqueue(1, b"first"), STATUS_OK);

        let mut undersized = [0xa5_u8; 2];
        assert_eq!(consumer.try_dequeue(&mut undersized), Err(STATUS_INVALID));
        assert_eq!(undersized, [0xa5_u8; 2]);

        let mut output = [0_u8; 8];
        assert_eq!(consumer.try_dequeue(&mut output), Ok(5));
        assert_eq!(&output[..5], b"first");
        assert_eq!(consumer.try_dequeue(&mut output), Ok(5));
        assert_eq!(&output[..5], b"later");
        assert_eq!(consumer.try_dequeue(&mut output), Err(STATUS_EMPTY));
    }

    #[test]
    fn mpsc_reuses_slots_for_arbitrary_lengths_and_copies_ownership() {
        const MAX_VALUE_SIZE: usize = 37;
        const REPETITIONS: usize = 16;

        let queue = Queue::new(2, MAX_VALUE_SIZE as u64, 2, 1).unwrap();
        let producer0 = queue.producer(0).unwrap();
        let producer1 = queue.producer(1).unwrap();
        let mut consumer = queue.consumer(0).unwrap();
        let mut output = vec![0xa5_u8; MAX_VALUE_SIZE];

        for round in 0..(MAX_VALUE_SIZE + 1) * REPETITIONS {
            let length = round % (MAX_VALUE_SIZE + 1);
            let mut input = (0..length)
                .map(|index| (index as u8).wrapping_add(round as u8))
                .collect::<Vec<_>>();
            let expected = input.clone();
            let producer = if round % 2 == 0 {
                &producer0
            } else {
                &producer1
            };

            assert_eq!(producer.try_enqueue(round as u64, &input), STATUS_OK);
            input.fill(0x5a);

            if length > 0 {
                let mut undersized = vec![0xc3_u8; length - 1];
                assert_eq!(consumer.try_dequeue(&mut undersized), Err(STATUS_INVALID));
                assert!(undersized.iter().all(|byte| *byte == 0xc3));
            }

            output.fill(0xa5);
            assert_eq!(consumer.try_dequeue(&mut output), Ok(length));
            assert_eq!(&output[..length], expected.as_slice());
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

        const PRODUCERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = 10_000;
        let queue = Queue::new(1024, 8, PRODUCERS as u32, 1).unwrap();
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
    fn mpsc_sustained_contention_conserves_through_final_drain() {
        use std::sync::atomic::{AtomicUsize, Ordering as TestOrdering};
        use std::sync::{Arc as TestArc, Barrier};
        use std::thread;
        use std::time::{Duration, Instant};

        const PRODUCERS: usize = 4;
        let queue = Queue::new(1024, 8, PRODUCERS as u32, 1).unwrap();
        let producers = (0..PRODUCERS)
            .map(|id| (id, queue.producer(id as u32).unwrap()))
            .collect::<Vec<_>>();
        let consumer = queue.consumer(0).unwrap();
        let start = TestArc::new(Barrier::new(PRODUCERS + 2));
        let enqueued = TestArc::new(AtomicUsize::new(0));
        let dequeued = TestArc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + Duration::from_secs(1);

        thread::scope(|scope| {
            for (producer_id, producer) in producers {
                let start = TestArc::clone(&start);
                let enqueued = TestArc::clone(&enqueued);
                scope.spawn(move || {
                    let mut sequence = 0_u64;
                    let mut payload = [0_u8; 8];
                    start.wait();
                    while Instant::now() < deadline {
                        payload[..4].copy_from_slice(&(producer_id as u32).to_le_bytes());
                        payload[4..].copy_from_slice(&sequence.to_le_bytes()[..4]);
                        let priority = sequence.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                        match producer.try_enqueue(priority, &payload) {
                            STATUS_OK => {
                                enqueued.fetch_add(1, TestOrdering::Relaxed);
                                sequence = sequence.wrapping_add(1);
                            }
                            STATUS_FULL => thread::yield_now(),
                            status => panic!("unexpected enqueue status {status}"),
                        }
                    }
                });
            }

            let start_consumer = TestArc::clone(&start);
            let dequeued_consumer = TestArc::clone(&dequeued);
            scope.spawn(move || {
                let mut consumer = consumer;
                let mut output = [0_u8; 8];
                start_consumer.wait();
                while Instant::now() < deadline {
                    match consumer.try_dequeue(&mut output) {
                        Ok(8) => {
                            dequeued_consumer.fetch_add(1, TestOrdering::Relaxed);
                        }
                        Err(STATUS_EMPTY) => thread::yield_now(),
                        result => panic!("unexpected dequeue result {result:?}"),
                    }
                }
            });

            start.wait();
        });

        let mut drain = queue.consumer(0).unwrap();
        let mut output = [0_u8; 8];
        let mut drained = 0;
        loop {
            match drain.try_dequeue(&mut output) {
                Ok(8) => drained += 1,
                Err(STATUS_EMPTY) => break,
                result => panic!("unexpected drain result {result:?}"),
            }
        }
        assert_eq!(
            enqueued.load(TestOrdering::Relaxed),
            dequeued.load(TestOrdering::Relaxed) + drained
        );
    }

    #[test]
    fn mpsc_repeated_small_capacity_stress_conserves_and_drains() {
        use std::sync::{Arc as TestArc, Mutex as TestMutex};
        use std::thread;

        const PRODUCERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = 128;
        const REPETITIONS: usize = 8;

        for capacity in [1_usize, 2, 3, 1024] {
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
