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

