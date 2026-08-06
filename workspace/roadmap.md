# Roadmap

You (the Orchestrator) own this file end-to-end. Update it every round
*before* deciding the round's task. The framework reads it back into
your next prompt — it does not parse it, so format it however you find
useful, but follow these conventions so the structure stays legible:

- **Major** items: structural changes expected to move the headline
  performance metric meaningfully (e.g. "Implement EAGLE3 speculative
  decoding", "Add CUDA graphs to verifier decode", "Replace manual
  attention with FlashAttention"). Usually 1-3 rounds each.
- **Minor** items: bug fixes, polish, gates (correctness recoveries,
  tiny kernel swaps, accuracy bumps). Usually 1 round each.
- Use one of these four statuses, and note rounds spent on each
  in-progress item:
  - `todo` — not started.
  - `in_progress` — actively being worked on this round (or recent rounds).
  - `done` — implemented, profiler-verified, hitting (close to) predicted impact.
  - `parked` — implementation is buggy or incomplete, but you believe the
    *direction* is sound. Returnable to `in_progress` later. Use this when
    the metric isn't moving for an *implementation* reason (zero acceptance
    on EAGLE3, capture failures on CUDA graphs, …) rather than a workload
    reason.
  - `abandoned` — the *direction* itself doesn't fit this workload. Strict
    requirement (see below) before flipping to this state.
- For each item include a one-line *why* (predicted impact, what
  bottleneck it addresses).

If any Major item is `todo` or `in_progress`, this round's task should
serve it. Do NOT drop into Minor work while a Major sits unfinished
unless that Minor is genuinely blocking the Major (state the dependency
explicitly when you do).

## `parked` vs `abandoned` — get this distinction right

These two are not the same thing and the loop's behavior degrades if you
treat them as one bucket:

- **`parked`** is the right call when (a) you predicted the technique
  would help, (b) the implementation passes the judge / pytest / accuracy
  gate, but (c) the headline metric didn't move *because the implementation
  appears to have a bug or is incomplete*. Symptoms: zero acceptance on a
  speculative decoder, all-fallback paths on what should be the fast path,
  a CUDA graph capturing but never replaying, etc. The direction itself is
  still believable; you just couldn't make the implementation good enough
  in the rounds you spent. Mark `parked` and move to a different Major;
  return to it when (i) you have a hypothesis for the bug, or (ii) other
  levers are exhausted.

- **`abandoned`** is the right call only when the *direction itself* is
  the wrong fit for this workload. Examples: continuous batching on a
  workload contractually limited to single-batch, MTP on a model that
  doesn't ship MTP heads, paged attention when the engine's fixed-shape
  KV path is already optimal at this batch size. Each requires a
  *mechanism-level* autopsy explaining why the technique cannot help
  *here* (not "it didn't pay off in 3 rounds"). If you can't write that
  mechanism, the right status is `parked`, not `abandoned`.

**Hard rule for `abandoned` autopsies:** you must name a code-level or
hardware-level mechanism — not a behavioral observation. A perf number
("+0% TPOT") is not a mechanism; "the workload is single-batch by
contract so continuous batching cannot raise arithmetic intensity" is.
If acceptance on a speculative decoder is zero, that is *not* a reason
to abandon — it's a debugging task, and the right status is `parked`
with a hypothesis. Spec decode acceptance debugging has a checklist in
`references/algorithms/speculative-decoding.md` ("Debugging 0
acceptance"); read it before parking or abandoning.

## Major

- [ ] M1 `in_progress` (rounds 1-2): Replace the queue-wide producer mutex for the MPSC configuration with per-producer bounded SPSC publication lanes plus an atomic global occupancy reservation; current corrective slice makes the occupancy token state authoritative for publication. Why: the current structure removes the dominant mutex contention, but the separate bitmap/token finalization protocol still has a rare high-contention race that must be made linearizable before optimization can proceed.
- [ ] M2 `todo`: Make MPSC slot payload storage reusable and allocation-free after initialization, while retaining the required copy semantics for arbitrary configured value sizes. Why: the baseline allocates a fresh `Vec<u8>` for every successful enqueue, adding allocator and ownership overhead to every measured operation.
- [ ] M3 `todo`: Optimize consumer-side minimum-priority selection over producer heads with a maintained head index/heap and cache-friendly slot layout. Why: once producers publish independently, repeatedly scanning all producer lanes can become the next consumer bottleneck.
- [ ] M4 `todo`: Add workload-guided layout and memory-ordering tuning for the MPSC fast path, validated against the trusted end-to-end benchmark. Why: atomics, false sharing, copying, and priority comparisons need measurement-driven refinement after the structural path is correct.

## Minor

- [ ] N1 `in_progress` (blocks: M1): Correctness/stress hardening for the new MPSC backend, including the reservation/publication race, capacities 1-3, full/empty boundaries, copied ownership, and final drain behavior. Why: the judge found a producer-side stale token/bitmap observation that can return STATUS_INTERNAL_ERROR or leave stale publication state, so M1 cannot be promoted until this gate is closed.

## Done

(none yet)

## Parked

(none yet)

## Abandoned

(none yet)
