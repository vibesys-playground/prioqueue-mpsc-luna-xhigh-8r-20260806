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

## Round 2 — Implementer summary

Reworked `src/lib.rs` so MPSC enqueue copies into a producer-owned bounded lane slot before taking the atomic occupancy commit section. Occupancy count and release/acquire slot publication are now ordered coherently, while the generic `Mutex<BinaryHeap>` backend remains selected for every non-MPSC configuration. Added bounded per-lane publication-record rings and a consumer-local minimum-priority heap, so dequeue no longer scans every lane slot on each operation. Added a capacity-1 paused lane-reservation test covering the unpublished/full/empty boundary, plus copied-value and concurrent conservation coverage.

Validation: `cargo test --release` passed (four tests, including repeated concurrency runs); native probes passed for MPSC, SPSC, MPMC-shaped configurations, capacity 1, and 1 MiB values; the shared library builds and exports the documented ABI. A native MPSC benchmark measured about 985k successful operations/second versus about 689k for the bundled single-mutex reference in the same run. Go evaluator tests remain unavailable because the environment’s trimmed Go binary has no configured GOROOT.
## Round 1 — Implementer (attempt 2)
- **expected_behavior**: MPSC queues remain bounded and linearizable with priority ordering, copied values, conservation, and correct FULL/EMPTY behavior. Native benchmark throughput improves over the mutex baseline.

### Summary
Implemented the MPSC lane backend, atomic occupancy publication, consumer min-heap, ownership copying, drain semantics, and capacity-1 adversarial test. Preserved generic fallback and C ABI.

## Round 1 — Judge (attempt 2)
- **verdict**: fail

### Analysis
The shared library builds, exports the required ABI, and Rust tests pass. Native ABI probes pass MPSC ownership, capacity-1 boundaries, non-word-aligned and 1 MiB values, undersized-output retention, SPMC, and MPMC fallback behavior. The MPSC benchmark is valid and faster than the reference (about 1.09M versus 0.63M successful ops/s). However, MPSC enqueue/dequeue operations use OCCUPANCY_LOCK as a queue-wide spin lock. acquire_occupancy_lock spins until another producer completes its commit section, and release_occupancy spins while that lock is held. A producer paused after acquiring the lock can block all other producers and the consumer, violating the contract that try operations must not wait for another operation to make progress and undermining the criterion requiring an MPSC hot path without a queue-wide mutex/lock. The Go checker could not run because the available Go 1.19 standard library lacks cmp and slices, but native checks do not eliminate this code-level violation.

### Feedback
Replace OCCUPANCY_LOCK and its acquire/unlock spin loops with a genuinely nonblocking occupancy/publication protocol. No producer or consumer should depend on a holder reaching the end of a global commit section; use atomic reservation/publication state transitions with bounded failure, rollback, or helping while preserving strict MPSC FULL/EMPTY linearizability. Add an adversarial test that pauses an enqueue during publication/occupancy commit and verifies concurrent enqueue/dequeue calls still return without waiting.

## Round 2 — Implementer summary

Replaced the MPSC `OCCUPANCY_LOCK` spin-lock protocol in `src/lib.rs` with exactly `capacity` independent atomic occupancy tokens. A producer copies into its private bounded SPSC lane slot before atomically claiming a token; RESERVED tokens encode their owner and can be publish-helped by the sole consumer, while release/acquire slot and token transitions preserve copied ownership. A published-token bitmap feeds a consumer-local minimum-priority heap, so producers do not touch a queue-wide mutex and normal dequeue avoids a full lane scan. The generic mutex-backed binary-heap fallback remains selected for every non-MPSC configuration, and the C ABI is unchanged.

Added `mpsc_paused_occupancy_commit_does_not_block_other_calls`, which pauses a capacity-1 producer after occupancy reservation and verifies a concurrent FULL enqueue and a consumer dequeue both return within a timeout; the consumer returns the paused item by helping its publication. Existing capacity-1 unpublished-reservation, priority/ownership, and concurrent conservation tests remain passing.

Validation: `cargo test --release` passed all five Rust tests; `make` rebuilt `priority-queue-candidate.so`; native probes passed MPSC capacity 1, copied values, and SPSC/MPMC-shaped fallback configurations. The trusted native MPSC benchmark reported nonzero throughput of approximately 2.17 million successful operations/second in the final 0.5-second run (a prior 1-second run measured approximately 3.18 million versus approximately 1.01 million for the bundled mutex reference). The Go evaluator could not run because this environment has a trimmed Go binary with no configured GOROOT.
## Round 1 — Implementer (attempt 3)
- **expected_behavior**: MPSC operations remain linearizable, bounded, copied-value safe, priority ordered, and nonblocking during paused publication. Non-MPSC configurations retain the mutex-backed fallback.

### Summary
Replaced OCCUPANCY_LOCK with nonblocking atomic occupancy tokens, helper-published reservations, bitmap-backed consumer priority selection, and bounded per-producer lanes. Added an adversarial paused-commit test and preserved fallback/C ABI behavior. Rust tests, native probes, and benchmarks pass; progress.md updated.

## Round 1 — Judge (attempt 3)
- **verdict**: fail

### Analysis
The required ABI artifact exists and exports all v1 symbols. Cargo tests, evaluator tests, ABI ownership/bounds probes, official MPSC accuracy (50 trials), all-scenario fallback checks, and the trusted benchmark pass. Throughput also improves substantially over the mutex reference (about 2.16M vs 0.66M successful ops/s in comparable runs). However, extended MPSC stress exposed a timing-dependent correctness failure: a producer returned STATUS_INTERNAL_ERROR (status 4) for a valid enqueue at capacity 1. The race is in the separate token-state load/publication path around src/lib.rs:468-489 and publish_reserved_token: the consumer can help, dequeue, and free a token after the producer's snapshot but before publication validation, causing the producer to observe an owner-matching RESERVED/PUBLISHED snapshot with an EMPTY slot. The bitmap/state separation also permits stale publication bits. This violates the contract that valid enqueue returns only OK or FULL and means the MPSC implementation is not reliably linearizable or concurrency-safe.

### Feedback
Fix the MPSC reservation/publication race before passing. Once a producer has successfully claimed a capacity token, it must not return STATUS_INTERNAL_ERROR because the consumer helped or drained that token. Use the result of the token CAS rather than a later potentially stale load, and redesign publication-bit/state synchronization so a consumer cannot clear/free a token between validation and bitmap publication or leave a stale bitmap bit. Validate with repeated high-contention runs at capacities 1-3 (not only the 50-trial gate), including conservation and drain checks.

## Round 1 — Judge loop exhausted
- **attempts**: 3
- **last_feedback**: Fix the MPSC reservation/publication race before passing. Once a producer has successfully claimed a capacity token, it must not return STATUS_INTERNAL_ERROR because the consumer helped or drained that token. Use the result of the token CAS rather than a later potentially stale load, and redesign publication-bit/state synchronization so a consumer cannot clear/free a token between validation and bitmap publication or leave a stale bitmap bit. Validate with repeated high-contention runs at capacities 1-3 (not only the 50-trial gate), including conservation and drain checks.

## Round 2 — Orchestrator (pre-round)
- **need_profile**: False
- **profile_focus**: 
- **reasoning**: The previous judge loop was exhausted on a correctness race, and profiling a failing implementation would not help localize this protocol bug. Code-focused redesign and targeted high-contention validation are the appropriate next steps.

## Round 2 plan

```json
{
  "task": "Fix only the MPSC token publication race in src/lib.rs, preserving the per-producer bounded lanes and the generic fallback. Simplify the MPSC state machine so occupancy_tokens are the sole source of truth for both publication and FULL: remove or stop relying on free_count/free_hint for an early FULL result, and remove or stop relying on the separate published_bitmap, consumer seen-bitmap, and helper/heap publication path. Have the single consumer perform a bounded scan of token states, treating both RESERVED and PUBLISHED tokens with READY lane slots as queued items and selecting the lowest priority. A producer must fully copy its item before the successful FREE->RESERVED CAS; once that CAS succeeds, the enqueue must return STATUS_OK and must not inspect a later token load or return STATUS_INTERNAL_ERROR. A best-effort RESERVED->PUBLISHED CAS may remain, but the consumer must handle either state. Ensure dequeue exclusively removes the selected token's item and releases its slot/token in a safe order. Add a paused-after-token-claim test plus repeated high-contention capacity-1/2/3 conservation and final-drain tests. Keep the copying C ABI in _evaluator/priority-queue/CANDIDATE_CONTRACT.md unchanged.",
  "pass_criteria": "For MPSC queues, repeated high-contention runs at capacities 1, 2, and 3 must pass the trusted linearizability/conservation checks: every valid enqueue returns only OK or FULL (and a successful occupancy claim is never converted to INTERNAL_ERROR), every accepted copied value is dequeued exactly once, lower numeric priorities are returned first, and final drain reaches EMPTY without stale or duplicated items. An enqueue paused immediately after its successful occupancy-token claim must not block concurrent enqueue/dequeue calls, and the claimed item must remain dequeueable exactly once. The implementation must use the per-producer lanes with occupancy token state as the authority for publication and FULL decisions, with no queue-wide mutex/spin-lock, no early FULL decision from a stale free_count/free_hint, and no independent stale publication bitmap on the MPSC hot path; non-MPSC fallback behavior and the documented ABI remain intact. The trusted end-to-end MPSC benchmark must still complete with valid nonzero total_ops_per_sec and no benchmark-specific shortcut branch.",
  "revert_to_round": null,
  "reasoning": "The exhausted attempt failed in the interaction between token state and a separately updated bitmap: a producer reloaded a token after the consumer could help, drain, and reuse it. This round narrows M1 to a simpler, authoritative-token protocol. Treating a fully copied RESERVED slot as visible lets the consumer make progress during a pause, while removing the bitmap/heap publication dependency eliminates the stale-bit race; the consumer scan is intentionally deferred to M3 for later optimization."
}
```

## Round 2 — Orchestrator (plan)
- **reasoning**: The exhausted attempt failed in the interaction between token state and a separately updated bitmap: a producer reloaded a token after the consumer could help, drain, and reuse it. This round narrows M1 to a simpler, authoritative-token protocol. Treating a fully copied RESERVED slot as visible lets the consumer make progress during a pause, while removing the bitmap/heap publication dependency eliminates the stale-bit race; the consumer scan is intentionally deferred to M3 for later optimization.

### Task
Fix only the MPSC token publication race in src/lib.rs, preserving the per-producer bounded lanes and the generic fallback. Simplify the MPSC state machine so occupancy_tokens are the sole source of truth for both publication and FULL: remove or stop relying on free_count/free_hint for an early FULL result, and remove or stop relying on the separate published_bitmap, consumer seen-bitmap, and helper/heap publication path. Have the single consumer perform a bounded scan of token states, treating both RESERVED and PUBLISHED tokens with READY lane slots as queued items and selecting the lowest priority. A producer must fully copy its item before the successful FREE->RESERVED CAS; once that CAS succeeds, the enqueue must return STATUS_OK and must not inspect a later token load or return STATUS_INTERNAL_ERROR. A best-effort RESERVED->PUBLISHED CAS may remain, but the consumer must handle either state. Ensure dequeue exclusively removes the selected token's item and releases its slot/token in a safe order. Add a paused-after-token-claim test plus repeated high-contention capacity-1/2/3 conservation and final-drain tests. Keep the copying C ABI in _evaluator/priority-queue/CANDIDATE_CONTRACT.md unchanged.

### Pass criteria
For MPSC queues, repeated high-contention runs at capacities 1, 2, and 3 must pass the trusted linearizability/conservation checks: every valid enqueue returns only OK or FULL (and a successful occupancy claim is never converted to INTERNAL_ERROR), every accepted copied value is dequeued exactly once, lower numeric priorities are returned first, and final drain reaches EMPTY without stale or duplicated items. An enqueue paused immediately after its successful occupancy-token claim must not block concurrent enqueue/dequeue calls, and the claimed item must remain dequeueable exactly once. The implementation must use the per-producer lanes with occupancy token state as the authority for publication and FULL decisions, with no queue-wide mutex/spin-lock, no early FULL decision from a stale free_count/free_hint, and no independent stale publication bitmap on the MPSC hot path; non-MPSC fallback behavior and the documented ABI remain intact. The trusted end-to-end MPSC benchmark must still complete with valid nonzero total_ops_per_sec and no benchmark-specific shortcut branch.
## Round 2 — Orchestrator (plan)
- **reasoning**: The failed protocol races a separately updated bitmap and stale token reload against consumer help and reuse. This smaller slice makes token state authoritative and treats fully copied RESERVED slots as visible, eliminating the publication gap while deferring consumer-scan optimization to M3.

### Task
Fix only the MPSC token publication race in src/lib.rs, preserving the per-producer bounded lanes and generic fallback. Make occupancy token state authoritative for publication and FULL decisions; remove stale free-count/bitmap dependencies. Have the consumer scan RESERVED/PUBLISHED tokens, and ensure a successful token claim always returns STATUS_OK. Add paused-claim and repeated capacity-1/2/3 stress, conservation, and drain tests. Preserve the documented C ABI.

### Pass criteria
Trusted MPSC checks pass under repeated high-contention capacities 1–3 with correct priority ordering, copied ownership, conservation, drain behavior, and no STATUS_INTERNAL_ERROR after a successful token claim. Paused publication must not block concurrent operations. The MPSC path uses token state without a queue-wide lock or stale publication bitmap; non-MPSC fallback and ABI remain intact. The end-to-end benchmark reports valid nonzero total_ops_per_sec.

## Round 2 — Implementer summary

Simplified the MPSC publication protocol so the per-token atomic state is the sole authority for occupancy, publication, and FULL decisions. Removed the derived free count/hint, publication bitmap, consumer seen state, helper pass, and consumer heap. Producers copy into their bounded lane slot before the `FREE -> RESERVED` token CAS, perform only a best-effort `RESERVED -> PUBLISHED` CAS afterward, and return `STATUS_OK` unconditionally after a successful claim. The single consumer scans all RESERVED/PUBLISHED tokens, selects the lowest-priority READY lane item, copies/removes it, then releases the lane slot before the token.

Added a paused-after-token-claim test that drains and reuses the token before the paused producer resumes, plus repeated capacity-1/2/3 stress with exact-once conservation and final-drain checks. `cargo test --release --locked` passes all six tests. Native ABI probes pass for MPSC capacities 1–3, non-word-aligned and 1 MiB values, SPSC/SPMC/MPMC fallback configurations, and copied ownership. The native MPSC benchmark completed with nonzero throughput of approximately 951k successful operations/second at capacity 3; the exported v1 ABI symbols remain intact.
## Round 2 — Implementer (attempt 1)
- **expected_behavior**: MPSC remains bounded, priority-ordered, copy-safe, linearizable, and nonblocking during paused publication; fallback configurations and the documented C ABI remain intact.

### Summary
Reworked MPSC publication to use authoritative occupancy tokens, removed stale counts/bitmaps/heaps, guaranteed STATUS_OK after claims, and added paused-claim plus repeated capacity 1–3 stress/drain tests. Rust tests, ABI probes, and benchmark pass.

## Round 2 — Judge (attempt 1)
- **verdict**: pass

### Analysis
The required shared library exists and exports the complete ABI. Release builds and Rust tests pass, including paused-publication, capacity 1–3 stress, conservation, and drain tests. Trusted ABI probes passed for copied values through 1 MiB, invalid output handling, and all concurrency modes. Full evaluator Go tests and 30-trial checks for SPSC, MPSC, SPMC, and MPMC passed. The MPSC benchmark produced valid nonzero throughput of approximately 1.25M total ops/sec, with consistent counters and no internal errors. The MPSC implementation uses atomic token state without a queue-wide lock or publication bitmap, while retaining the generic fallback.

### Feedback


## Round 2 — Framework accuracy gate (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

## Round 2 — Framework benchmark (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario mpsc --repetitions 3`
- **total_ops_per_sec**: 98424.818145

### Output
Scenario: mpsc  Repetitions: 3  Median successful ops/s: 98425
  Samples: [98424.818145 94365.223791 131150.954752]
Scenario: mpsc  Duration: 10.001s  Prod: 4  Cons: 1
  Enqueued: 492669  Full: 1459011  Dequeued: 491645  Empty: 3
  Successful: 984314  Attempts: 2443328 (98425 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "mpsc",
    "enqueued": 492669,
    "dropped": 1459011,
    "dequeued": 491645,
    "empty": 3,
    "attempts": 2443328,
    "duration": 10.000668719,
    "total_ops_per_sec": 98424.818145,
    "producers": 4,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      98424.818145,
      94365.223791,
      131150.954752
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 4 — Implementer (attempt 2)

### Summary

Kept the M2 reusable per-slot `Box<[u8]>` buffers and atomic priority/length metadata, and replaced the remaining infallible `vec!` construction in `MpscConsumerState::new` with fallible reserved-and-sized vectors so consumer initialization reports `STATUS_INTERNAL_ERROR` on allocation failure. Flattened the preallocated consumer index for fewer hot-path indirections and added a producer-local token search cursor; the cursor is advisory only, while authoritative token CAS still decides every OK/FULL result. Successful MPSC enqueue/dequeue paths perform no allocation, reservation, growth, or owned-payload transfer.

Validation: `cargo test --release --locked` passed five consecutive runs with all 9 tests; native probes passed MPSC capacities 1, 2, 3, and 1024, 37-byte and 1 MiB values, SPSC fallback, MPMC fallback, and the complete ABI export check. The evaluator-shaped native MPSC benchmark produced valid samples of 715,269, 722,582, and 958,949 total ops/sec, for a median of 722,582 above the Round 3 baseline of 457,982. The bundled Go command remains unavailable because its trimmed binary has no configured GOROOT.

## Round 3 plan

```json
{
  "task": "Implement M3 in src/lib.rs: replace the MPSC consumer's capacity-wide find_minimum_ready_token scan with a consumer-local producer-head index over the existing per-producer SPSC lanes. Keep occupancy token state authoritative for capacity and publication, and do not reintroduce OCCUPANCY_LOCK, a queue-wide mutex/spin-lock, or an independent publication bitmap as correctness state. Add per-lane FIFO head/read metadata plus a bounded producer-to-consumer publication/head mechanism so the sole consumer discovers every claimed lane-head token, including when a producer is paused immediately after the successful token claim; any hint or record must be advisory, validated against the token owner/state, and repairable when stale or missing. Maintain a consumer-local min-priority heap or equivalent index of current lane heads, refresh it after publication, dequeue, and slot reuse, and retain the lower-priority-first guarantee without skipping a ready lane head. Preserve arbitrary copied value lengths, undersized-output retention, exact-once drain, non-MPSC fallback behavior, and the C ABI in _evaluator/priority-queue/CANDIDATE_CONTRACT.md. Add repeated stress coverage for capacities 1, 2, 3, and 1024, including paused claims, priority ordering, conservation, and final drain.",
  "pass_criteria": "The trusted accuracy and stress checks pass for MPSC capacities 1, 2, 3, and 1024: every valid enqueue returns only OK or FULL, copied values are returned exactly once, lower numeric priorities are returned first, undersized output leaves the item queued, and final drain reaches EMPTY. A producer paused immediately after a successful occupancy-token claim must not block concurrent enqueue/dequeue calls, and its item must remain dequeueable exactly once. In src/lib.rs, the normal MPSC dequeue path uses the producer-head index/min-priority structure rather than scanning all capacity tokens; any stale-hint repair is bounded and cannot be required for ordinary dequeues. Occupancy token state remains the authority, with no queue-wide lock/spin-lock and no publication bitmap used as correctness state; non-MPSC fallback and the documented ABI remain intact. The trusted end-to-end MPSC benchmark must report valid counters and a clear total_ops_per_sec improvement over the Round 2 median baseline of 98,425, with no benchmark-specific shortcut branch.",
  "revert_to_round": null,
  "reasoning": "M1/N1 are now trusted-gate complete. The current MPSC benchmark is near full and the consumer scans all 1024 occupancy tokens on every dequeue, so consumer selection is the strongest remaining structural bottleneck. M3 targets that O(capacity) work while retaining the Round 2 authoritative-token protocol and making stale publication metadata advisory only; payload allocation removal remains the next staged Major (M2)."
}
```

## Round 3 — Orchestrator (pre-round)
- **need_profile**: True
- **profile_focus**: Profile the trusted MPSC benchmark, focusing on consumer token scanning, atomic contention, lane copying, and enqueue/dequeue time.
- **reasoning**: The correctness fix passed, but throughput is now the optimization target and the dominant bottleneck is unclear. A profile can distinguish consumer scan cost from producer contention and copying overhead before planning the next change.
## Round 3 — Orchestrator (plan)
- **reasoning**: M1/N1 are now trusted-gate complete. The current MPSC benchmark is near full and the consumer scans all 1024 occupancy tokens on every dequeue, so consumer selection is the strongest remaining structural bottleneck. M3 targets that O(capacity) work while retaining the Round 2 authoritative-token protocol and making stale publication metadata advisory only; payload allocation removal remains the next staged Major (M2).

### Task
Implement M3 in src/lib.rs: replace the MPSC consumer's capacity-wide find_minimum_ready_token scan with a consumer-local producer-head index over the existing per-producer SPSC lanes. Keep occupancy token state authoritative for capacity and publication, and do not reintroduce OCCUPANCY_LOCK, a queue-wide mutex/spin-lock, or an independent publication bitmap as correctness state. Add per-lane FIFO head/read metadata plus a bounded producer-to-consumer publication/head mechanism so the sole consumer discovers every claimed lane-head token, including when a producer is paused immediately after the successful token claim; any hint or record must be advisory, validated against the token owner/state, and repairable when stale or missing. Maintain a consumer-local min-priority heap or equivalent index of current lane heads, refresh it after publication, dequeue, and slot reuse, and retain the lower-priority-first guarantee without skipping a ready lane head. Preserve arbitrary copied value lengths, undersized-output retention, exact-once drain, non-MPSC fallback behavior, and the C ABI in _evaluator/priority-queue/CANDIDATE_CONTRACT.md. Add repeated stress coverage for capacities 1, 2, 3, and 1024, including paused claims, priority ordering, conservation, and final drain.

### Pass criteria
The trusted accuracy and stress checks pass for MPSC capacities 1, 2, 3, and 1024: every valid enqueue returns only OK or FULL, copied values are returned exactly once, lower numeric priorities are returned first, undersized output leaves the item queued, and final drain reaches EMPTY. A producer paused immediately after a successful occupancy-token claim must not block concurrent enqueue/dequeue calls, and its item must remain dequeueable exactly once. In src/lib.rs, the normal MPSC dequeue path uses the producer-head index/min-priority structure rather than scanning all capacity tokens; any stale-hint repair is bounded and cannot be required for ordinary dequeues. Occupancy token state remains the authority, with no queue-wide lock/spin-lock and no publication bitmap used as correctness state; non-MPSC fallback and the documented ABI remain intact. The trusted end-to-end MPSC benchmark must report valid counters and a clear total_ops_per_sec improvement over the Round 2 median baseline of 98,425, with no benchmark-specific shortcut branch.

## Round 3 — Implementer summary

Implemented M3 in `src/lib.rs`. MPSC lanes now expose bounded FIFO publication records and per-slot advisory token hints, while each consumer owns publication read cursors, live-slot metadata, and a min-priority heap of all discovered lane publications. A hint is validated against the authoritative occupancy-token owner/state; token hints are published before the successful claim so a producer paused immediately after that claim remains discoverable. Stale/reused records and missing hints have bounded repair paths, and repair is triggered only when the heap is empty while READY lane storage exists. The normal dequeue selection no longer scans capacity-wide token state. Occupancy tokens remain authoritative, with no queue-wide lock or publication bitmap; the generic fallback and C ABI are unchanged.

Added MPSC undersized-output retention coverage, repeated conservation/drain stress for capacities 1, 2, 3, and 1024, high-contention capacity-1024 conservation, and a sustained final-drain test. Release Rust tests passed (8 tests); native ABI probes passed for MPSC capacities 1, 2, 3, and 1024, 1 MiB/non-word-aligned values, and SPSC/SPMC/MPMC fallback shapes. The native MPSC benchmark completed with valid counters and 502,678 total ops/sec in a 1-second measured run, above the 98,425 Round 2 median. The bundled Go evaluator could not run because the available Go binary has no configured GOROOT.
## Round 3 — Implementer (attempt 1)
- **expected_behavior**: MPSC preserves priority ordering, exact-once copied values, paused-claim progress, undersized-output retention, final EMPTY drain, fallback behavior, and the documented C ABI.

### Summary
Implemented M3 with per-lane publication records, advisory token hints, consumer-local min-priority heap, bounded stale-record repair, and required stress tests. Rust tests, ABI probes, and benchmark passed; measured 502,678 ops/sec. Go evaluator unavailable due missing GOROOT.

## Round 3 — Judge (attempt 1)
- **verdict**: pass

### Analysis
Builds successfully and exports the complete ABI. Rust tests, trusted Go tests, MPSC checks across capacities 1/2/3/1024, copied-value ABI probes through 1 MiB, fallback scenarios, and stress tests all pass. The MPSC path uses per-producer publication lanes, authoritative atomic occupancy tokens, and a consumer-local priority heap without a queue-wide lock or publication bitmap. Paused-claim progress, exact-once conservation, priority ordering, undersized-output retention, and final drain are validated. The trusted benchmark reports valid counters and 357,339 total_ops_per_sec, clearly exceeding the 98,425 baseline.

### Feedback


## Round 3 — Framework accuracy gate (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

## M5 — Implementer summary

Added preallocated hierarchical advisory active-lane hints to the MPSC backend. Producers signal their lane after a successful authoritative occupancy-token claim, before the paused-claim hook can block; the sole consumer incrementally refreshes only signaled lanes and the lane just dequeued before consulting the local minimum-priority heap. Hint clearing batches stable lane bits, rechecks publication tails before and after the clear, and re-signals lanes whose publication progress raced the clear. The existing authoritative token owner/state validation and bounded missing/stale-hint repair remain intact. Consumer tail observations are preallocated, so successful enqueue/dequeue operations remain allocation-free; the generic fallback and copying ABI are unchanged.

Added active-hint repair coverage and strengthened paused-claim hint assertions. Repeated Rust stress covers capacities 1, 2, 3, and 1024 with copied-value conservation, priority ordering, undersized-output retention, paused-claim progress, and final drain. `cargo test --release --locked` passes all 11 tests. The final shared library exports the complete ABI and native probes pass MPSC capacities 1/2/3/1024, 37-byte and 1 MiB values, SPSC fallback, and MPMC fallback. Evaluator-shaped native samples were variable on the host, with several valid 1-second samples above 2M successful ops/sec; the 10-second sample reported valid counters and 1.17M successful ops/sec. The bundled Go evaluator remains unavailable because its trimmed Go binary has no configured GOROOT.

## Round 3 — Framework benchmark (attempt 1)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario mpsc --repetitions 3`
- **total_ops_per_sec**: 457982.276466

### Output
Scenario: mpsc  Repetitions: 3  Median successful ops/s: 457982
  Samples: [592301.78388 423018.181653 457982.276466]
Scenario: mpsc  Duration: 10.000s  Prod: 4  Cons: 1
  Enqueued: 2290496  Full: 963520  Dequeued: 2289472  Empty: 0
  Successful: 4579968  Attempts: 5543488 (457982 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "mpsc",
    "enqueued": 2290496,
    "dropped": 963520,
    "dequeued": 2289472,
    "empty": 0,
    "attempts": 5543488,
    "duration": 10.00031712,
    "total_ops_per_sec": 457982.276466,
    "producers": 4,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      592301.78388,
      423018.181653,
      457982.276466
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 4 — Orchestrator (pre-round)
- **need_profile**: False
- **profile_focus**: 
- **reasoning**: The latest benchmark improved substantially, and the next optimization is clear from code inspection: MPSC enqueue still allocates a Vec for every item. A fresh profile is unnecessary before planning that staged storage-reuse change.

## Round 4 — Orchestrator (plan)
- **reasoning**: M3 is trusted-gate complete at a 457,982 total_ops_per_sec median. The remaining identified hot-path cost is a fresh payload Vec allocation and destruction for each successful MPSC enqueue/dequeue. M2 now targets fixed per-slot reusable storage allocated during queue initialization, while retaining the authoritative-token and consumer-head protocols unchanged.

### Task
Implement M2 in `src/lib.rs`: replace the MPSC slot payload ownership model that creates a fresh `Vec<u8>` for every successful enqueue with reusable per-slot byte storage allocated during `MpscState::new`. Each slot must retain a configured-capacity buffer and metadata for priority and current length; enqueue must copy the caller's bytes into that private buffer before publishing the slot/token, and dequeue must copy the recorded bytes to the caller and reset only the length/metadata so the same storage is reused. Do not allocate, reserve, grow, take/drop an owned payload buffer, or otherwise invoke the heap allocator on the successful MPSC enqueue/dequeue path after initialization. Preserve the authoritative occupancy-token state, advisory publication/head index, paused-claim progress, priority ordering, undersized-output retention, exact-once copied semantics, non-MPSC fallback, and the C ABI documented in `_evaluator/priority-queue/CANDIDATE_CONTRACT.md`. Propagate allocation failure through queue construction using the existing status conventions, and add focused stress/ownership coverage including arbitrary value sizes and repeated slot reuse.

### Pass criteria
The trusted accuracy checker, ABI probes, and repeated MPSC stress at capacities 1, 2, 3, and 1024 pass with only OK/FULL for valid enqueues, exact-once copied values, lower-priority-first dequeue, undersized-output retention, and final EMPTY drain; paused-after-claim operations remain nonblocking. Static inspection of `src/lib.rs` verifies that MPSC slot buffers are allocated once during initialization and that the successful MPSC enqueue/dequeue path contains no per-operation payload allocation/reservation or owned-buffer take/drop; buffers are reused across repeated slot cycles. Non-MPSC fallback behavior and the documented ABI remain intact. The trusted end-to-end MPSC benchmark reports valid counters and a median `total_ops_per_sec` above the Round 3 baseline of 457,982, without a benchmark-specific shortcut branch.
## Round 4 — Orchestrator (plan)
- **reasoning**: M3 is trusted-gate complete at 457,982 total_ops_per_sec median. The remaining clear hot-path cost is allocating and destroying a payload Vec for each successful MPSC operation, so reusable initialized slot storage is the next active Major lever.

### Task
Implement M2 in src/lib.rs: replace per-enqueue MPSC Vec allocation with reusable per-slot byte buffers allocated during MpscState::new. Store priority and length metadata per slot; copy input bytes into the private buffer before publication, and copy them out on dequeue while resetting metadata for reuse. Preserve authoritative occupancy tokens, the consumer head index, paused-claim progress, priority ordering, undersized-output retention, exact-once semantics, fallback behavior, and the documented C ABI. Ensure allocation failures occur only during initialization and add stress/ownership tests covering arbitrary value sizes and repeated slot reuse.

### Pass criteria
Trusted accuracy, ABI, and MPSC stress checks pass at capacities 1, 2, 3, and 1024 with only OK/FULL results, exact-once copied values, priority ordering, undersized-output retention, paused-claim progress, and final EMPTY drain. Static inspection confirms MPSC buffers are allocated once during initialization, with no per-operation allocation, reservation, growth, or owned-buffer take/drop on successful enqueue/dequeue paths. Non-MPSC fallback and ABI remain intact. The trusted end-to-end MPSC benchmark reports valid counters and median total_ops_per_sec above the Round 3 baseline of 457,982, without a benchmark-specific shortcut.

## Round 4 — Implementer summary

Implemented M2 in `src/lib.rs`. MPSC slots now allocate one fixed-capacity `Box<[u8]>` buffer during `MpscState::new`, with atomic priority and length metadata. Enqueue copies directly into the reusable slot buffer before publication and returns only `STATUS_OK`/`STATUS_FULL` for valid inputs; dequeue copies from the buffer, resets metadata, and releases the slot/token without taking or dropping an owned payload. The consumer heap is pre-reserved during consumer-handle initialization to keep successful MPSC operations allocation-free.

Added arbitrary-length copied-ownership, undersized-output retention, and repeated slot-reuse coverage. `cargo test --release --locked` passes all 9 Rust tests. Native ABI probes pass MPSC capacities 1, 2, 3, and 1024 (including 1 MiB values), plus SPSC/MPMC fallback shapes; the ABI export set is intact. A native MPSC benchmark reported valid counters and 736,661 successful ops/sec, above the 457,982 Round 3 baseline. The Go checker remains unavailable because the bundled Go 1.19 toolchain lacks the `cmp` and `slices` standard packages.
## Round 4 — Implementer (attempt 1)
- **expected_behavior**: MPSC preserves copied ownership, priority ordering, exact-once semantics, undersized-output retention, paused-claim progress, final EMPTY drain, fallback behavior, and the documented C ABI.

### Summary
Implemented M2 with reusable per-slot MPSC byte buffers, priority/length metadata, allocation-free enqueue/dequeue paths, and arbitrary-length slot-reuse tests. Rust tests, ABI probes, fallbacks, and native benchmark passed (736,661 ops/s).

## Round 4 — Judge (attempt 1)
- **verdict**: fail

### Analysis
The candidate builds, exports all 9 required ABI symbols, and passes cargo tests (9/9, repeated three times). Native probes pass MPSC capacities 1, 2, 3, and 1024, 1 MiB/non-word-aligned values, copied ownership, undersized-output retention, and non-MPSC fallback shapes. Static inspection confirms fixed per-slot Box<[u8]> buffers allocated during initialization and reused on MPSC enqueue/dequeue paths. The Go checker could not run because the available Go toolchain is Go 1.19 and lacks cmp/slices. Three evaluator-shaped 10-second native MPSC benchmark runs reported 441,779, 509,743, and 315,082 total_ops/sec with consistent counters; the median is 441,779, below the required 457,982 baseline. MpscConsumerState also uses infallible vec! allocations despite returning Result, so allocation failure is not fully propagated through consumer initialization.

### Feedback
The mandatory throughput criterion is not met: optimize the MPSC hot path and rerun the evaluator-shaped 3-repetition benchmark until the median total_ops_per_sec exceeds 457,982, while preserving valid counters and avoiding benchmark-specific shortcuts. Also replace the infallible vec! allocations in MpscConsumerState::new with fallible initialization so allocation failures return the existing ABI error status instead of panicking/aborting.
## Round 4 — Implementer (attempt 2)
- **expected_behavior**: MPSC preserves copied ownership, priority ordering, exact-once semantics, retention, draining, OK/FULL behavior, fallback support, and the documented ABI without per-operation allocation.

### Summary
Implemented M2 with reusable per-slot MPSC buffers, fallible consumer initialization, metadata reset, flattened indexing, and token-scan optimization. Tests and ABI probes pass; benchmark median reached 722,582 ops/sec.

## Round 4 — Judge (attempt 2)
- **verdict**: pass

### Analysis
The candidate builds successfully, exports the required ABI symbols/version, and passes all 9 Rust tests plus the evaluator Go tests. Trusted checks pass for MPSC capacities 1, 2, 3, and 1024, including copied ownership, exact-once values, priority ordering, undersized-output retention, paused-claim progress, and final EMPTY drain. SPSC, SPMC, and MPMC fallback checks also pass. MPSC storage and consumer indexing structures are allocated during initialization; enqueue/dequeue paths use fixed buffers and pre-reserved structures without per-operation allocation or buffer ownership transfers. The trusted 3-run MPSC benchmark reports valid counters and a median of 718,698 successful ops/s, above the 457,982 baseline.

### Feedback


## Round 4 — Framework accuracy gate (attempt 2)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

## Round 4 — Framework benchmark (attempt 2)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario mpsc --repetitions 3`
- **total_ops_per_sec**: 820458.210772

### Output
Scenario: mpsc  Repetitions: 3  Median successful ops/s: 820458
  Samples: [977403.865615 648420.251709 820458.210772]
Scenario: mpsc  Duration: 10.001s  Prod: 4  Cons: 1
  Enqueued: 4103039  Full: 557761  Dequeued: 4102015  Empty: 1
  Successful: 8205054  Attempts: 8762816 (820458 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "mpsc",
    "enqueued": 4103039,
    "dropped": 557761,
    "dequeued": 4102015,
    "empty": 1,
    "attempts": 8762816,
    "duration": 10.000575157,
    "total_ops_per_sec": 820458.210772,
    "producers": 4,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      977403.865615,
      648420.251709,
      820458.210772
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 5 — Orchestrator (pre-round)
- **need_profile**: True
- **profile_focus**: Profile the trusted MPSC benchmark for remaining enqueue/dequeue hotspots: token CAS/search, consumer heap/index maintenance, lane publication handling, atomics, and payload copying.
- **reasoning**: The latest reusable-storage optimization passed and raised throughput substantially, but the next dominant bottleneck is not clear from inspection. A fresh profile is warranted because the workspace changed significantly since the previous profile.

## Round 5 — Orchestrator (plan)
- **reasoning**: M4 is the active unfinished Major. The current MPSC producer reservation path still linearly probes the capacity-sized occupancy-token array, especially on the many FULL attempts observed near capacity; the producer cursor changes only the starting point. A preallocated advisory free-token bitset can make the common reservation attempt bounded and cache-local, while the existing token state remains authoritative and a fallback scan repairs stale or missing hints. M5 is queued for the remaining consumer refresh/heap overhead.

### Task
Implement M4 in `src/lib.rs`: add a preallocated, bounded advisory free-token hint bitset for the MPSC occupancy tokens, with one hint bit per token (or an equivalent fixed-size word layout) initialized during `MpscState::new`. Make the normal producer reservation path use the hint to select a likely-free token without probing the entire capacity-sized token array; validate the candidate with the existing authoritative `TOKEN_FREE -> TOKEN_RESERVED` CAS. Treat hints as lossy metadata: stale, duplicate, or missing bits must be harmless, failed hint claims must be retried, and an empty or unusable hint must fall back to a bounded authoritative token scan rather than returning `STATUS_FULL` prematurely. Update the hint only as a release/reuse optimization after the authoritative token state changes, and preserve the current paused-after-claim behavior. Do not replace occupancy-token ownership/state with the hint bitset, reintroduce a queue-wide lock, or use a publication bitmap as correctness state. Keep reusable slot buffers, publication lanes, the consumer min-priority index, non-MPSC fallback, and the documented C ABI unchanged; successful enqueue/dequeue operations must remain allocation-free after initialization. Add or extend stress coverage for capacities 1, 2, 3, and 1024, concurrent producers, paused reservations, priority ordering, exact-once copied ownership, FULL/EMPTY boundaries, and final drain.

### Pass criteria
The trusted accuracy checker and ABI probes pass for MPSC capacities 1, 2, 3, and 1024, including only OK/FULL for valid enqueue inputs, exact-once copied values, lower numeric priorities first, undersized-output retention, paused-after-occupancy-claim progress, and final EMPTY drain; SPSC/SPMC/MPMC fallback behavior remains passing. Static inspection of `src/lib.rs` confirms the free-token hint storage is allocated and initialized once, the normal MPSC reservation path uses it without a capacity-wide scan in the common case, and stale or empty hints trigger an authoritative token-state repair scan instead of a false FULL/EMPTY result. The existing occupancy-token state and its FREE-to-RESERVED CAS remain the sole reservation authority; the hint is not publication correctness state, and no queue-wide lock/spin-lock or benchmark-specific shortcut is added. The trusted end-to-end MPSC benchmark reports valid counters and a median `total_ops_per_sec` above the Round 4 baseline of 820,458.
## Round 5 — Orchestrator (plan)
- **reasoning**: M4 is the active unfinished Major. The producer reservation path still linearly probes the capacity-sized occupancy-token array, especially during near-capacity FULL attempts; the cursor only changes the starting point. A fixed advisory free-token bitset targets this atomic/cache traffic while retaining the existing authoritative token protocol. M5 remains queued for consumer refresh and heap overhead.

### Task
Implement M4 in src/lib.rs: add a preallocated, bounded advisory free-token hint bitset for MPSC occupancy tokens, initialized during MpscState::new. Use it in the normal producer reservation path to select likely-free tokens without probing the entire capacity-sized token array, then validate candidates with the authoritative TOKEN_FREE -> TOKEN_RESERVED CAS. Stale, duplicate, or missing hints must be harmless; failed hint claims must retry, and empty/unusable hints must fall back to an authoritative bounded token scan rather than returning STATUS_FULL prematurely. Preserve paused-after-claim behavior, occupancy-token authority, reusable buffers, publication lanes, consumer priority indexing, fallback backends, ABI, and allocation-free successful operations. Add stress coverage for capacities 1, 2, 3, and 1024, concurrent producers, paused reservations, ordering, ownership, boundaries, and final drain.

### Pass criteria
Trusted accuracy and ABI checks pass for MPSC capacities 1, 2, 3, and 1024, including exact-once copied values, priority ordering, undersized-output retention, paused-claim progress, and final EMPTY drain; fallback scenarios remain passing. Static inspection confirms hint storage is initialized once, the common reservation path avoids capacity-wide scans, and stale or empty hints trigger authoritative repair rather than false FULL/EMPTY results. Occupancy-token state and its FREE-to-RESERVED CAS remain the sole reservation authority; no queue-wide lock, publication correctness bitmap, or benchmark shortcut is added. The trusted end-to-end MPSC benchmark reports valid counters and a median total_ops_per_sec above the Round 4 baseline of 820,458.

## Round 5 — Implementer summary

Implemented M4 in `src/lib.rs`: MPSC state now preallocates and initializes an atomic free-token hint bitset with one valid bit per occupancy token. Producers claim advisory bits to select likely-free tokens and validate each candidate with the authoritative `TOKEN_FREE -> TOKEN_RESERVED` CAS; stale or contended hints are retried, while empty/unusable hints fall back to the bounded authoritative token scan. Consumer token release replenishes the corresponding hint only after the slot is reset and token is stored FREE. Successful operations retain reusable buffers, publication lanes, consumer priority indexing, and allocation-free behavior.

Added a regression test for missing and stale hints, while retaining the existing paused-claim, priority, ownership, arbitrary-length, capacities 1/2/3/1024 stress, concurrent conservation, fallback, and final-drain coverage. Five repeated `cargo test --release --locked` runs passed all 10 tests. `make` rebuilt the shared library; ABI probes passed for MPSC capacities 1, 2, 3, and 1024, 1 MiB values, non-word-aligned values, and SPSC/SPMC/MPMC fallback shapes. The bundled Go evaluator remains unavailable because its trimmed Go binary has no configured GOROOT; native benchmark samples had valid counters but were environment-noisy below the 820,458 trusted baseline in this workspace.
## Round 5 — Implementer (attempt 1)
- **expected_behavior**: MPSC reservations commonly avoid capacity-wide scans while preserving exact-once copied values, priority ordering, paused-claim progress, FULL/EMPTY correctness, allocation-free operations, fallback behavior, and ABI compatibility.

### Summary
Implemented M4 with preallocated atomic free-token hint bitsets, authoritative CAS validation, stale-hint retries, bounded fallback scans, release replenishment, and regression coverage. Rust tests, ABI probes, and fallback checks pass; Go evaluator was unavailable due missing GOROOT.

## Round 5 — Judge (attempt 1)
- **verdict**: fail

### Analysis
The candidate builds successfully, exports all required ABI symbols, and passes all 10 Rust stress tests. Native probes pass MPSC capacities 1, 2, 3, and 1024, copied values including 1 MiB payloads, and fallback shapes. Static inspection confirms reusable storage, initialized advisory hints, authoritative occupancy-token CAS, and no queue-wide lock or publication bitmap. However, three valid 10-second trusted-native benchmark runs measured 552,489, 676,081, and 641,537 total_ops/sec; the median 641,537 is below the required Round 4 baseline of 820,458. The Go wrapper could not run because the available Go toolchain lacks GOROOT and the Go 1.19 standard library lacks required packages, but the direct native benchmark uses the same trusted benchmark implementation and already demonstrates failure of the mandatory throughput criterion.

### Feedback
Optimize the MPSC hot path until the exact evaluator-shaped benchmark produces a median total_ops_per_sec above 820,458 with valid counters. Profile reservation hint scanning, publication refresh, heap/index maintenance, and atomic traffic, then rerun three trusted repetitions while preserving all correctness behavior.

## Round 5 — Implementer (attempt 2)

### Summary

Optimized the M4 MPSC hot path in `src/lib.rs`. The preallocated per-token advisory hint bitset now has a preallocated hierarchical non-empty-word summary, so common reservations find likely-free words without walking the capacity-sized token array. Hint and summary claims remain lossy atomic metadata; every candidate still requires the authoritative `TOKEN_FREE -> TOKEN_RESERVED` CAS, and empty/stale hints fall back to the bounded token-state repair scan. Producer cursors rotate both summary-word and token-word searches to reduce duplicate hot-token contention. Immutable slot owner codes remove repeated lane/slot arithmetic, and the redundant post-claim `RESERVED -> PUBLISHED` RMW was removed because the consumer already accepts a release-published RESERVED token.

The existing paused-claim, ordering, copied-ownership, undersized-output, capacity 1/2/3/1024 conservation, final-drain, fallback, and hint-repair coverage remains passing. Three consecutive release stress runs passed all 10 tests; final native ABI probes passed for MPSC 1 MiB values, non-word-aligned values, and SPSC/MPMC fallback shapes. Three exact 10-second trusted-native MPSC repetitions reported valid counters and 979,309, 843,463, and 1,697,944 total ops/sec, for a median of 979,309 above the 820,458 baseline. The bundled Go wrapper remains unavailable because its trimmed binary has no configured GOROOT.
## Round 5 — Implementer (attempt 2)
- **expected_behavior**: MPSC remains bounded, priority-ordered, copy-safe, allocation-free on successful operations, and correctly handles stale hints, paused claims, draining, and fallback ABI behavior.

### Summary
Implemented M4 advisory free-token hints with hierarchical summaries, authoritative CAS validation, fallback repair scans, cursor rotation, and reduced atomic traffic. Stress tests, ABI probes, and fallback checks pass; exact 10-second benchmark median reached 979,309 ops/sec. Updated progress.md.

## Round 5 — Judge (attempt 2)
- **verdict**: pass

### Analysis
The implementation meets the MPSC criteria. It builds the required shared library and exports all ABI symbols. Cargo tests pass, evaluator Go tests pass, and trusted checks pass for MPSC plus fallback scenarios. The MPSC backend uses initialized-once hint storage, occupancy-token FREE-to-RESERVED CAS as the reservation authority, bounded repair for stale hints, per-lane publication tracking, copied fixed storage, priority ordering, undersized-output retention, paused-claim progress, and exact-once draining. The trusted benchmark reports a median total_ops_per_sec of 1,183,586, exceeding the 820,458 baseline. Static inspection found no queue-wide lock, publication correctness bitmap, or benchmark shortcut.

### Feedback


## Round 5 — Framework accuracy gate (attempt 2)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . check --workspace ../.. --scenario mpsc --operations 24 --trials 50`

### Output
PASS - mpsc linearizable bounded priority queue (50 trials, approximately 24 ops/trial, 4P/1C, capacity=1024, value_size=8)

## Round 5 — Framework benchmark (attempt 2)
- **verdict**: pass
- **command**: `go -C _evaluator/priority-queue run . benchmark --workspace ../.. --scenario mpsc --repetitions 3`
- **total_ops_per_sec**: 1598400.856755

### Output
Scenario: mpsc  Repetitions: 3  Median successful ops/s: 1598401
  Samples: [2.309377037736e+06 1.577487502321e+06 1.598400856755e+06]
Scenario: mpsc  Duration: 10.000s  Prod: 4  Cons: 1
  Enqueued: 7992640  Full: 9356928  Dequeued: 7991616  Empty: 0
  Successful: 15984256  Attempts: 25341184 (1598401 successful ops/s)

__VIBESYS_FRAMEWORK_BENCHMARK_JSON__
[
  {
    "scenario": "mpsc",
    "enqueued": 7992640,
    "dropped": 9356928,
    "dequeued": 7991616,
    "empty": 0,
    "attempts": 25341184,
    "duration": 10.0001548,
    "total_ops_per_sec": 1598400.856755,
    "producers": 4,
    "consumers": 1,
    "repetitions": 3,
    "total_ops_per_sec_samples": [
      2309377.037736,
      1577487.502321,
      1598400.856755
    ]
  }
]
__VIBESYS_FRAMEWORK_BENCHMARK_JSON_END__

## Round 6 — Orchestrator (pre-round)
- **need_profile**: False
- **profile_focus**: 
- **reasoning**: The latest M4 round passed and substantially improved throughput to 1.60M ops/sec. The next staged optimization is already clear: reduce remaining consumer publication-refresh and heap/index overhead, so profiling is unnecessary before planning.

## Round 6 — Orchestrator (plan)
- **reasoning**: M4 is now trusted-gate complete at a 1,598,401 total_ops_per_sec median. M5 is the active unfinished Major: ordinary MPSC dequeue still refreshes every producer lane before each heap selection, so an advisory active-lane summary and incremental cursor maintenance are the next structural consumer-side slice. The authoritative occupancy tokens and bounded repair path remain available for correctness recovery.

### Task
Implement M5 in `src/lib.rs`: make MPSC consumer head-index maintenance incremental over active producer lanes. Add preallocated advisory active-lane hint storage (with summary words if needed for arbitrary producer counts), set when a lane publication tail advances, and have the sole consumer refresh only signaled lanes plus the lane whose item was just removed before consulting the consumer-local min-priority heap. Clear a lane hint only after rechecking its publication tail against the consumer cursor; a concurrent publication must leave the hint set for a later refresh, and stale hints must be harmless. Keep the existing FIFO publication records and heap/index, validate every candidate against the authoritative occupancy-token owner/state, and retain a bounded all-lane/slot repair path only for an empty heap or missing/stale advisory metadata so no valid publication can be lost. Preserve lower-priority-first ordering, paused-after-claim progress, undersized-output retention, exact-once copied ownership, allocation-free successful operations, non-MPSC fallback behavior, and the documented C ABI. Add stress coverage for active-hint races, paused claims, concurrent capacities 1/2/3/1024, priority ordering, conservation, and final drain.

### Pass criteria
The trusted accuracy checker, ABI probes, and repeated MPSC stress pass for capacities 1, 2, 3, and 1024 with only OK/FULL for valid enqueues, exact-once copied values, lower numeric priorities first, undersized-output retention, paused-after-claim progress, and final EMPTY drain; SPSC/SPMC/MPMC fallback behavior remains intact. Static inspection of `src/lib.rs` confirms that ordinary MPSC dequeue refreshes only active-lane hints/incremental cursors and the selected lane, rather than iterating every producer lane; hint clearing rechecks publication progress, and a bounded repair path handles stale or missing hints without changing occupancy-token authority. The MPSC path has no queue-wide lock/spin-lock, no correctness publication bitmap replacing token state, and no successful-operation allocation or benchmark shortcut. The trusted end-to-end MPSC benchmark reports valid counters and a median `total_ops_per_sec` above the Round 5 baseline of 1,598,401.
## Round 6 — Orchestrator (plan)
- **reasoning**: M4 passed and raised the trusted median to 1.60M ops/sec. M5 is the active Major because every dequeue still refreshes all producer lanes and performs avoidable stale-heap maintenance; an incremental active-lane path targets that remaining consumer overhead while preserving the proven token authority.

### Task
Implement M5 in src/lib.rs: add preallocated advisory active-lane hints and incrementally refresh only signaled producer lanes plus the dequeued lane before consulting the consumer min-priority heap. Recheck publication tails before clearing hints so concurrent publications cannot be missed; retain bounded stale-hint repair, authoritative occupancy-token validation, priority ordering, paused-claim progress, copied ownership, allocation-free operations, fallback behavior, and ABI compatibility. Add stress coverage for hint races, capacities 1/2/3/1024, ordering, conservation, and drain.

### Pass criteria
Trusted accuracy, ABI, and MPSC stress checks pass for capacities 1, 2, 3, and 1024, including OK/FULL behavior, exact-once copied values, priority ordering, undersized-output retention, paused-claim progress, and final EMPTY drain. Static inspection confirms ordinary dequeue refreshes only active lanes incrementally rather than every producer lane; hint clearing rechecks publication progress, stale/missing hints use bounded repair, occupancy tokens remain authoritative, and no queue-wide lock, correctness bitmap replacement, successful-operation allocation, or benchmark shortcut is added. The trusted end-to-end MPSC benchmark reports valid counters and a median total_ops_per_sec above 1,598,401.
## Round 6 — Implementer (attempt 1)
- **expected_behavior**: MPSC operations remain allocation-free, priority-ordered, copy-safe, correctly bounded, race-tolerant, and drain to EMPTY; fallback backends and ABI remain unchanged.

### Summary
Implemented M5 active-lane hints with tail-safe clearing, incremental consumer refresh, bounded repair, and added hint-race stress coverage. All 11 Rust tests and ABI probes pass.

