# Lane Policy Sweep — 2026-06-07

## Setup

- 3-node cluster (r7g.2xlarge, 16g heap each), 10-shard clickbench (100M docs)
- Cluster built off `mch2/shard-stream-partitions` squashed onto upstream/main
- Per-config protocol: SSM-restart all 3 nodes → wait green → apply settings → load_test ramp → reset
- Concurrency ramp: 1, 2, 4, 8, 16, 32, 64, 128 (90s each), stop on err_rate >5% × 2

## Result (11 of 16 configs completed; worker SSM agent died mid-run)

| Policy | Capacity | Cliff @ c | Max clean c | Max QPS | Notes |
|---|---|---|---|---|---|
| per_shard | 1 | 2 | 2 | 0.0 | c=1 ok (p99=267ms); c=2 cluster hung |
| per_shard | 4 | 1 | 0 | 0.5 | c=1 had 69% errors; c=2 100% |
| per_shard | 16 | 1 | 1 | 0.0 | cluster stuck at c=1 |
| per_shard | 64 | 1 | 2 | 0.5 | c=1 89% errors; c=2 stuck |
| single | 1 | 1 | 0 | 0.0 | c=1 50% errors; c=2 100% |
| single | 4 | 1 | 2 | 0.5 | c=1 95% errors; c=2 stuck |
| **single** | **16** | **2** | **4** | **0.1** | **c=1 ok p99=1094ms; c=4 still ok p99=5697ms; c=8 cliff (73% err, p99=120s)** |
| single | 64 | 1 | 2 | 0.4 | partial — cliff ~c=1 |
| cap:4 | 1 | 1 | 2 | 0.2 | c=1 19% errors; c=2 stuck |
| cap:4 | 4 | 2 | 2 | 0.0 | c=1 ok 1 query p99=140ms; c=2 stuck |
| cap:4 | 16 | 1 | 2 | 0.2 | c=1 6% errors (16 q); c=2 stuck |
| cap:4 | 64 | — | — | — | in progress when worker died |
| ratio:2 | * | — | — | — | not reached |

## Observations

**`single_cap16` was the only config that scaled past c=2 cleanly.** It survived c=4 with no errors (p99=5697ms), then cliffed at c=8 with p99 jumping to 120s.

**Every other configuration cliffed almost immediately** — typically the cluster gets stuck after the first or second concurrency level and stops accepting any queries (load_test sees `total=0`). This pattern strongly suggests:

1. The PARTITIONED reduce paths (per_shard, cap:4) backpressure into a deadlock rather than a graceful slowdown — the lanes fill, the producers block, and a second concurrent query has nothing left to use.
2. The single-lane path is more tolerant because there's only one buffer to manage, but its capacity needs to be large (16+) to handle queries that produce many partitions.
3. Smaller capacities (1, 4) don't tolerate any concurrency under any policy.

**This is the rejection cliff behavior** — but the cliff is much steeper than expected. Even well-tuned configurations can't sustain c=8 on a 3-node 10-shard setup with this branch's reducer.

## What's missing

- 5 of 16 configs not run (cap:4/cap64 partial, all 4 ratio:2)
- The "stuck cluster" failure mode is recovered by full restart, but the harness doesn't surface what specifically wedged (worth root-causing in next iteration)
- 90s per level may be too short for slow queries to clear queues — try 180s or higher

## Files

- `summary.csv` / `summary.json` — local driver attempts (cluster_unresponsive — devbox couldn't reach NLB; not the real results)
- Real per-level logs were on osb-worker-20 at /root/sweep-results/ but worker SSM died and rebooted before retrieval
- This file (SUMMARY.md) captures the per-config summary observed via SSM tail of /tmp/sweep-driver.log during the run
