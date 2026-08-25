# Memory-Adaptive Sort — Live Demo Commands

Runnable, copy-paste commands to launch the cluster, load data, run a spilling
`ORDER BY`, and read the memory-adaptive log lines produced by our modified
`AbstractExternalSortRunGenerator` (simulated broker) and `AbstractFrameSorter`.

> Everything runs against the **pre-built** assembly in `target/` with only our one
> jar swapped in (`repo/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar`). No full rebuild.

---

## 0. Environment (run once per shell)

```bash
# JDK 26 is required: the assembly is Java-21 bytecode; the default shell Java (17)
# cannot load it. /opt/homebrew/opt/openjdk is OpenJDK 26.
export JAVA_HOME=/opt/homebrew/opt/openjdk

# Cluster dir (the sample cluster's opt/local), the assembly's jar folder, and the SQL++ endpoint.
export CLUSTER=/Users/ameen/Codebase/asterixdb/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
export REPO=/Users/ameen/Codebase/asterixdb/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/repo
export Q=http://127.0.0.1:19002/query/service

# Sanity: should print "openjdk version 26.x"
"$JAVA_HOME/bin/java" -version
```

---

## 0.5 Build & deploy a specific code version (e.g. after `git stash`)

**Important:** git changes only your *source*. The running cluster loads a pre-built jar, so
after any `git stash` / `git stash pop` / checkout you must **rebuild the jar and redeploy** or
the demo still runs the old code. Comment-only edits don't need this; code edits do.

To demo a particular version (for example, `git stash` to fall back to "Stage 1 + Random Broker"):

```bash
# 1. Switch source to the version you want to demo:
cd /Users/ameen/Codebase/asterixdb
git stash            # e.g. drop uncommitted Stage 2/3 -> back to committed Stage 1 + Random Broker
#   ... run the demo ...   then later:  git stash pop    # bring the changes back

# 2. Rebuild the one jar from the (now switched) source:
cd /Users/ameen/Codebase/asterixdb/hyracks-fullstack
mvn -o -pl hyracks/hyracks-dataflow-std package -DskipTests   # expect BUILD SUCCESS

# 3. Restart the cluster with the fresh jar:
( cd "$CLUSTER" && bin/stop-sample-cluster.sh )
cp hyracks/hyracks-dataflow-std/target/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar \
   "$REPO/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar"
( cd "$CLUSTER" && bin/start-sample-cluster.sh )              # expect "Cluster ... ACTIVE"
```

The 300k-row dataset persists in LSM storage across the restart, so you don't need to reload it
(§2 is only for a fresh instance). After redeploying, jump to §3 to run the query.

> Reminder: rebuild + redeploy after **every** `git stash` / `git stash pop`. The demo *output*
> (the `adaptive-sort:` log lines) looks the same across these versions — the "Stage 1 + Random
> Broker" story is about the *code structure* you show in the editor; the logs demonstrate the
> adaptive behavior.

---

## 1. Start the cluster

```bash
cd "$CLUSTER"
bin/start-sample-cluster.sh
# Expect: "INFO: Cluster started and is ACTIVE."   Web console: http://localhost:19001
```

---

## 2. Create & load the dataset (300,000 rows, generated — no download)

`id` is the primary key; `k` is a **non-key** field with pseudo-random order (so the
sort can't be optimized away); `payload` pads each row so the data spills.

```bash
# 2a. dataverse + type + dataset
curl -s "$Q" --data-urlencode 'statement=
DROP DATAVERSE test IF EXISTS;
CREATE DATAVERSE test;
USE test;
CREATE TYPE t AS open { id: int64 };
CREATE DATASET ds(t) PRIMARY KEY id;'

# 2b. load 300k rows via range() (padding string inflates row size)
PAD="padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_padding_pad"
curl -s "$Q" --data-urlencode "statement=
USE test;
INSERT INTO ds (
  SELECT VALUE { \"id\": x, \"k\": (x * 48271) % 2000000, \"payload\": \"$PAD\" }
  FROM range(1, 300000) AS x
);"

# 2c. verify count == 300000
curl -s "$Q" --data-urlencode 'statement=SELECT VALUE count(*) FROM test.ds;'
```

Data persists across cluster restarts (LSM storage), so you only load it once.

---

## 3. Run the query and capture *just this run's* logs

`ORDER BY r.k` (a non-key field) forces a real external sort; the result body is
discarded (`-o /dev/null`) — we only want the logs.

**Budget choice** (via `SET \`compiler.sortmemory\``):
- **`8MB`** — big runs; a single query exercises **all five decision paths** (use this for the demo).
- **`1MB`** — tiny budget; maximum spilling / many small runs.

The NC logs **accumulate across queries**, so we record the log's line count *before*
the query and read only the lines added *after* — that isolates one clean run:

```bash
LOG="$CLUSTER/logs/nc-asterix_nc1.log"
SINCE=$(wc -l < "$LOG")            # remember the log position BEFORE the query

curl -s "$Q" -o /dev/null -w 'query http=%{http_code}\n' \
  --data-urlencode 'statement=USE test; SET `compiler.sortmemory` "8MB"; SELECT VALUE r FROM ds AS r ORDER BY r.k;'

# snapshot ONLY this run's adaptive lines (decisions + proof), stripped to the clean message:
tail -n +$((SINCE+1)) "$LOG" | grep -o 'adaptive-sort.*' > /tmp/thisrun.log
```

> Re-running the query? Redo all three lines (reset `SINCE`, query, snapshot) so the
> snapshot stays a single run. (`nc2` is identical in structure — swap the filename to
> inspect the other partition.)

---

## 4. See every execution path

Two kinds of line:
- `adaptive-sort:` — the broker **decision** (from `AbstractExternalSortRunGenerator`). Five tags:
  - `grant-grow`  — not a victim + broker granted more → **grow the current run, no spill**
  - `victim-full` — victimized while full → spill and **shrink** the budget
  - `victim-periodic-shrink` — periodic poll caught a victim, but loaded data still fits the halved
    budget → **just tighten, NO spill** (keep accumulating)  ← the efficiency fix
  - `victim-periodic-spill`  — periodic poll caught a victim and loaded data exceeds the halved
    budget → **must spill** to give memory back, then shrink
  - `denied`      — denied (or at the cap) → spill, budget **unchanged**
- `adaptive-sort-run:` — the **proof** (from `AbstractFrameSorter.sort()`, once per run):
  frames the run *actually loaded* and what fraction of the budget it *actually used*.

Tunables (in `AbstractExternalSortRunGenerator`): `VICTIM_PROBABILITY` (0.3; grant
chance is `1 - VICTIM_PROBABILITY`) and `VICTIM_CHECK_INTERVAL` (poll every N frames;
currently 10 — small so the periodic paths show up readily).

All commands below read the single-run snapshot `/tmp/thisrun.log` from step 3.

```bash
# 4a. COUNTS per decision path -- at 8MB all five should be > 0
grep -oh 'adaptive-sort: [a-z-]*' /tmp/thisrun.log | sort | uniq -c

# 4b. FULL trace in order (decisions interleaved with proof lines)
head -40 /tmp/thisrun.log

# 4c. GRANT-GROW -- broker granted more; grew the CURRENT run (consecutive grows, no proof between)
grep 'grant-grow' /tmp/thisrun.log | head

# 4d. VICTIM-PERIODIC-SHRINK -- THE FIX: victimized mid-run but data still fit -> tighten, NO spill
grep 'victim-periodic-shrink' /tmp/thisrun.log | head

# 4e. VICTIM-PERIODIC-SPILL -- victimized and over the halved budget -> had to spill
grep 'victim-periodic-spill' /tmp/thisrun.log | head

# 4f. VICTIM-FULL / DENIED -- sorter filled up, asked the broker, got victimized / denied -> spill
grep -E 'victim-full|denied' /tmp/thisrun.log | head

# 4g. PROOF -- actual frames loaded + fill% per run (98-99% = a naturally-full spill)
grep 'adaptive-sort-run:' /tmp/thisrun.log | head
```

### How to read the trace (real lines)

**`grant-grow` enlarges the CURRENT run without spilling** — two grows with **no
`adaptive-sort-run` between them**, then a spill that loaded far more than the run
started with:
```
adaptive-sort: grant-grow budgetFrames=32 (min=16, max=124)
adaptive-sort: grant-grow budgetFrames=64 (min=16, max=124)
adaptive-sort-run: framesLoaded=56 bytesUsed=2070208 budgetBytes=2097152 fillPct=98 tuples=9800
```

**`victim-periodic-shrink` — the fix** — victimized mid-run, but what was loaded still
fit the halved budget, so it tightened and kept going (no forced spill at that moment;
the run later spills naturally at ~98%):
```
adaptive-sort: victim-periodic-shrink budgetFrames=63 (min=16, max=1020)
adaptive-sort-run: framesLoaded=55 bytesUsed=2033240 budgetBytes=2064384 fillPct=98 tuples=9625
```

**`victim-periodic-spill`** — victimized and already over the halved budget, so it had
to spill to give memory back:
```
adaptive-sort: victim-periodic-spill budgetFrames=62 (min=16, max=124)
adaptive-sort-run: framesLoaded=54 bytesUsed=1996272 budgetBytes=2031616 fillPct=98 tuples=9450
```

Key reads:
- **framesLoaded tracks the budget** — a run that grows mid-life loads far more than the
  budget it started with (proof `grant-grow` truly enlarged the live run, no spill).
- **fillPct is a lie detector** — ~98–99% means the run spilled because it was genuinely
  full; a low fillPct next to a periodic line would mean a *forced* early spill.
- **framesLoaded < budget frames** (e.g. 56 vs 64) — merge sort's byte budget is
  `frameBytes + 2×pointerBytes` per frame, so pointers eat ~12%; `bytesUsed/budgetBytes`
  is the honest ~98%.
- The `Random(0)` seed makes it deterministic (both NCs identical); change it to
  `new Random()` in `AbstractExternalSortRunGenerator` for live variation.

```bash
# optional: save this run's clean trace for slides
cp /tmp/thisrun.log adaptive_trace.txt
```

---

## 5. Stop the cluster

```bash
cd "$CLUSTER"
bin/stop-sample-cluster.sh
```

---

## Gotchas (why each choice matters)

- **`export JAVA_HOME=/opt/homebrew/opt/openjdk` first** — without it the launcher
  picks Java 17 and dies with `UnsupportedClassVersionError` (class file version 65).
- **`ORDER BY` a non-primary-key field** (`k`) — sorting by the PK lets the optimizer
  skip the sort (data is already stored PK-ordered).
- **Budget vs. paths** — `8MB` makes runs span many frames so the periodic poll lands
  mid-fill and you see all five tags in one query; `1MB` maximizes spilling. A sort that
  fits fully in memory never spills and never adapts.
- **No `LIMIT`** — a `LIMIT` routes to the top-K sorter (a different class we didn't
  modify), so no `adaptive-sort:` lines.
- **Logs accumulate** — always use the `SINCE`/`tail` snapshot from step 3 to view a
  single run; otherwise `grep` mixes lines from every previous query.
- **The jar** — the running cluster loads our code from
  `.../apache-asterixdb-0.9.10-SNAPSHOT/repo/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar`.
  To pick up new changes: rebuild it
  (`cd hyracks-fullstack && mvn -o -pl hyracks/hyracks-dataflow-std package -DskipTests`),
  copy it over that path, then restart the cluster.
```
