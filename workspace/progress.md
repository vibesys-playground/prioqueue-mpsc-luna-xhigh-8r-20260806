# Progress

## Round 1 plan

```json
{
  "task": "Implement M1 in src/lib.rs (and supporting Rust files only if needed): add an MPSC-specialized backend selected when consumer_count == 1 and producer_count > 1, while preserving the current generic fallback for other worker configurations. Replace the MPSC queue-wide Mutex<BinaryHeap> operation path with one bounded per-producer SPSC publication lane per producer, an atomic global occupancy reservation, and consumer-side selection of the lowest-priority published lane head. Capture the producer id in each producer handle; use release/acquire publication so the producer copies the borrowed value before making its slot visible, and have the sole consumer copy a value out before freeing its slot and releasing occupancy. Handle full/empty, invalid lengths, allocation failure, and final drain without leaking reservations or retaining caller pointers. Keep the copying C ABI in _evaluator/priority-queue/CANDIDATE_CONTRACT.md unchanged.",
  "pass_criteria": "For the MPSC configuration (multiple producers, one consumer), the candidate uses independent bounded producer lanes and atomic occupancy rather than a queue-wide Mutex on the enqueue/dequeue hot path, and successfully transfers copied values with lower numeric priorities first. The trusted MPSC accuracy checker must pass full/empty boundaries, arbitrary configured value lengths and copied input/output ownership, concurrent histories, item conservation, and final drain with no fabricated or duplicated values; existing generic fallback tests for non-MPSC configurations must remain intact. The trusted end-to-end MPSC benchmark must complete with a valid nonzero total_ops_per_sec and improve on the current single-Mutex baseline, with no benchmark-specific shortcut branch in the candidate implementation.",
  "revert_to_round": null,
  "reasoning": "This is a fresh workspace with no prior measurements or roadmap work. The baseline serializes every producer and consumer operation under one Mutex and allocates a Vec for each successful enqueue. The first structural win should remove producer contention while retaining a simple consumer scan of producer heads; reusable payload storage and a faster head index are staged as separate follow-on Majors so this round remains implementable and diagnosable."
}
```
## Round 1 — Orchestrator (plan)
- **reasoning**: The fresh baseline serializes every producer and consumer operation under one mutex and allocates each enqueue value. Removing producer contention is the first structural throughput opportunity; reusable storage and faster consumer indexing remain staged follow-up Majors.

### Task
Implement M1 in src/lib.rs: add an MPSC-specialized backend for producer_count > 1 and consumer_count == 1, preserving the generic fallback otherwise. Replace the MPSC queue-wide Mutex<BinaryHeap> path with bounded per-producer SPSC publication lanes, atomic global occupancy reservation, release/acquire publication, and consumer-side minimum-priority selection. Preserve copied-value ownership, full/empty semantics, draining, and the documented C ABI.

### Pass criteria
MPSC accuracy passes all priority, capacity, ownership, concurrency, conservation, and drain checks. The MPSC hot path uses independent producer lanes and atomic occupancy without a queue-wide mutex; non-MPSC fallback behavior remains intact. The trusted end-to-end benchmark reports valid nonzero total_ops_per_sec and improves over the current single-mutex baseline.

## Round 1 — Implementer summary

Implemented the MPSC backend in `src/lib.rs`. Queues with more than one producer and exactly one consumer now use independent bounded per-producer publication lanes, reusable atomic-state slots, release/acquire value publication, an atomic global occupancy reservation, and consumer-side minimum-priority selection across published values. Producer handles retain their producer id, borrowed values are copied before publication, invalid/full/empty paths preserve the ABI contract, and the consumer frees a slot only after copying and dropping the value. Other worker configurations continue to use the existing mutex-backed binary heap.

Validation: release Rust tests passed, including concurrent three-producer conservation; native ABI probes passed for MPSC/SPSC/MPMC-shaped configurations and 1 MiB values; native fallback benchmarks completed successfully. The MPSC benchmark reported nonzero throughput of approximately 4.1–4.9 million successful operations/second versus approximately 0.95 million for the comparable single-mutex reference run. Go evaluator tests could not run because the bundled Go toolchain is missing the required `cmp`/`slices` standard packages and dependency downloads are unavailable in this environment.
## Round 1 — Implementer (attempt 1)
- **expected_behavior**: MPSC queues avoid the queue-wide mutex, preserve capacity, priority, ownership, conservation, and draining semantics; non-MPSC configurations retain existing behavior.

### Summary
Implemented the MPSC lane backend with atomic occupancy, release/acquire publication, copied ownership, minimum-priority dequeue, and preserved generic fallback/C ABI. Rust and native validation passed.

## Round 1 — Judge (attempt 1)
- **verdict**: fail

### Analysis
Build, ABI export, Rust tests, evaluator tests, native probes, the exact 50-trial MPSC check, and the trusted benchmark all succeed. The candidate reaches 691,764 median successful ops/s versus 658,476 for the single-mutex reference. However, the MPSC implementation is not generally linearizable: `reserve_occupancy()` increments `occupied` before allocation/copy/publication. With capacity 1, producer A can reserve and pause; producer B then returns FULL while consumer C observes no published slot and returns EMPTY; A later publishes and returns OK. Since B completes before C starts, FULL requires A to precede B, while EMPTY requires C to precede A, which is impossible. The strict reservation-aware behavior is permitted only for MPMC, not MPSC. The finite accuracy runs did not expose this rare scheduling window.

### Feedback
Fix the MPSC reservation/publication protocol so FULL and EMPTY cannot observe contradictory states during an in-flight enqueue. In particular, do not make capacity reservations visible to strict-MPSC FULL responses while the item remains unpublished, or add a nonblocking commit protocol that preserves linearizability. Add an adversarial capacity-1 test covering: successful enqueue paused after reservation, concurrent FULL enqueue, then EMPTY dequeue before publication.

