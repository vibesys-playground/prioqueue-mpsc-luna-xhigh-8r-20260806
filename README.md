# prioqueue-mpsc-luna-xhigh-8r-20260806

VibeSys 8-round MPSC priority queue run, gpt-5.6-luna (xhigh effort), 2026-08-06.

VibeSys optimized a bounded priority queue (copying C ABI, Rust seed in
`workspace/src/lib.rs`) for the **MPSC** scenario (4 producers / 1 consumer).
Headline metric: `total_ops_per_sec` from the trusted evaluator (maximize).
Correctness (linearizability, priority order, no fabrication/duplication,
capacity) is a hard gate.

- Agent: Codex CLI, model `gpt-5.6-luna`, reasoning effort: xhigh
- Rounds: 8
- Hardware: single host, see `logs/gpu.json`

## Rounds

| Round | Commit | Correctness | total_ops_per_sec |
| --- | --- | --- | --- |
| 1 | `2d87727` (tag `round-1`) | fail | n/a |
| 2 | `f2837ae` (tag `round-2`) | pass | 98,425 |
| 3 | `9d68ba5` (tag `round-3`) | pass | 457,982 |
| 4 | `6dceffb` (tag `round-4`) | pass | 820,458 |
| 5 | `6fc1aef` (tag `round-5`) | pass | 1,598,401 |
| 6 | `f1c5bda` (tag `round-6`) | fail | n/a |
| 7 | `04a2b86` (tag `round-7`) | pass | 1,721,874 |
| 8 | `ddd0d52` (tag `round-8`) | pass | 1,348,456 |

## Layout

- `workspace/`: candidate workspace. One commit per agent step
  (`round-N-retry-M-{implementer,judge,framework-accuracy,framework-benchmark}`).
  `workspace/progress.md` and `workspace/roadmap.md` hold the agents' notes;
  `workspace/_evaluator/priority-queue/` is the trusted evaluator.
- `logs/run-*.log`: full orchestrator / implementer / judge transcripts.
- `logs/rounds.json`: per-round commit, gate result, and metric.
- `logs/usage.jsonl`: per-agent-call token usage and duration.
