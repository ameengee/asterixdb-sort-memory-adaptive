# Memory-Adaptive Sort — Live Demo Commands

Runnable, copy-paste commands to launch the cluster, load data, run a spilling
`ORDER BY`, and read the memory-adaptive log lines produced by our modified
`AbstractExternalSortRunGenerator`.

> Everything runs against the **pre-built** assembly in `target/` with only our one
> jar swapped in (`repo/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar`). No full rebuild.

---

## 0. Environment (run once per shell)

```bash
# JDK 26 is required: the assembly is Java-21 bytecode; the default shell Java (17)
# cannot load it. /opt/homebrew/opt/openjdk is OpenJDK 26.
export JAVA_HOME=/opt/homebrew/opt/openjdk

# Cluster directory (the sample cluster's opt/local) and the SQL++ query endpoint.
export CLUSTER=/Users/ameen/Codebase/asterixdb/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT/opt/local
export Q=http://127.0.0.1:19002/query/service

# Sanity: should print "openjdk version 26.x"
"$JAVA_HOME/bin/java" -version
```

---

## 1. Start the cluster

```bash
cd "$CLUSTER"
bin/start-sample-cluster.sh
# Expect: "INFO: Cluster started and is ACTIVE."
# Web console: http://localhost:19001
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

---

## 3. Run the spilling `ORDER BY` (triggers the adaptation)

Small `compiler.sortmemory` ⇒ many runs ⇒ many adaptations. `ORDER BY r.k`
(non-key) forces a real external sort. Result body is discarded (`-o /dev/null`);
we only care about the logs.

```bash
curl -s "$Q" -o /dev/null -w 'http=%{http_code} bytes=%{size_download} time=%{time_total}s\n' \
  --data-urlencode 'statement=
USE test;
SET `compiler.sortmemory` "1MB";
SELECT VALUE r FROM ds AS r ORDER BY r.k;'
```

---

## 4. Read the adaptive log lines

There are **two** log lines, and they tell different halves of the story:

- `adaptive-sort:` — the *decision*: the budget chosen for the **next** run
  (emitted by `AbstractExternalSortRunGenerator`).
- `adaptive-sort-run:` — the *proof*: how many frames the run **actually loaded**
  and what fraction of the budget it **actually used** before spilling
  (emitted by `AbstractFrameSorter.sort()`, once per run).

```bash
# 4a. full log lines (path + timestamp + thread + message)
grep -E "adaptive-sort:|adaptive-sort-run:" "$CLUSTER"/logs/nc-asterix_nc*.log

# 4b. ISOLATED clean format (strip path/timestamp/thread; keep "INFO ... adaptive-sort... ...")
grep -hE "adaptive-sort:|adaptive-sort-run:" "$CLUSTER"/logs/nc-asterix_nc*.log | grep -o 'INFO.*'

# 4c. just the messages (roll/budget decisions AND actual per-run usage), latest run only.
#     NOTE: logs accumulate across query runs, so use tail (not head) to see the most recent run.
grep -hE "adaptive-sort:|adaptive-sort-run:" "$CLUSTER"/logs/nc-asterix_nc1.log | grep -o 'adaptive-sort.*' | tail -26

# 4d. just the PROOF line (actual frames loaded + fill %)
grep -oh 'adaptive-sort-run:.*' "$CLUSTER"/logs/nc-asterix_nc*.log
```

**Two log lines, paired** — each `adaptive-sort:` budget decision is followed by the
`adaptive-sort-run:` that consumed it:

```
adaptive-sort:     roll=2 nextRunFrames=16                                   <- decide: next run gets 16 frames
adaptive-sort-run: framesLoaded=14 bytesUsed=517552  budgetBytes=524288  fillPct=98   <- proof: loaded 14, used 98%
adaptive-sort:     roll=3 nextRunFrames=32
adaptive-sort-run: framesLoaded=28 bytesUsed=1035104 budgetBytes=1048576 fillPct=98
adaptive-sort:     roll=3 nextRunFrames=64
adaptive-sort-run: framesLoaded=56 bytesUsed=2070208 budgetBytes=2097152 fillPct=98
adaptive-sort:     roll=3 nextRunFrames=124
adaptive-sort-run: framesLoaded=109 bytesUsed=4029512 budgetBytes=4063232 fillPct=99
```

Reading it:
- **framesLoaded tracks the budget** (14 → 28 → 56 → 109, doubling with 16 → 32 → 64 → 124):
  the run really did absorb more/less data when the knob moved.
- **fillPct ≈ 98–99%**: each spilled run used essentially *all* the memory it had
  (a spill triggers exactly when the next frame no longer fits).
- **framesLoaded < budget frames** (14 vs 16): merge sort's byte budget is
  `frameBytes + 2×pointerBytes` per frame, so pointers consume ~12% of the byte
  budget; `bytesUsed/budgetBytes` is the honest ~98%.
- **The final run is a partial leftover** (e.g. `fillPct=35`): the last batch flushed
  at `close()` after input ended — it never fills up. Every *spilled* run is ~99%.

**Example output of 4b** (one NC; `roll` = 1 keep / 2 halve / 3 double,
`nextRunFrames` = budget for the next run in 32 KB frames, `min/max` = floor/cap):

```
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=1 nextRunFrames=31 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=2 nextRunFrames=16 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=2 nextRunFrames=16 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=32 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=64 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=1 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=1 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=124 (min=16, max=124)
INFO  org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunGenerator - adaptive-sort: roll=3 nextRunFrames=124 (min=16, max=124)
```

Reading it: start at nominal **31** frames → halve, clamped up to floor **16** →
doubles up to cap **124** → stays pinned. One trace exercises keep, halve, double,
and both clamps. The `Random(0)` seed makes it deterministic (both NCs identical),
so it reproduces every run — change `new Random(0)` to `new Random()` in
`AbstractExternalSortRunGenerator` for live variation.

```bash
# optional: save a clean trace for slides
grep -h "adaptive-sort:" "$CLUSTER"/logs/nc-asterix_nc*.log | grep -o 'INFO.*' > adaptive_trace.txt
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
- **Small `compiler.sortmemory`** — adaptation only fires at run boundaries; if the
  sort fits in memory it never spills and never adapts.
- **No `LIMIT`** — a `LIMIT` routes to the top-K sorter (a different class we didn't
  modify), so no `adaptive-sort:` lines.
- **The jar** — the running cluster loads our code from
  `.../apache-asterixdb-0.9.10-SNAPSHOT/repo/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar`.
  To pick up new changes: rebuild it
  (`cd hyracks-fullstack && mvn -o -pl hyracks/hyracks-dataflow-std package -DskipTests`)
  and copy it over that path, then restart the cluster.
```
