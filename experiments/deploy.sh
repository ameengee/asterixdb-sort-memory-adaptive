#!/usr/bin/env bash
# Memory-adaptive sort -- build, deploy, and restart the cluster with a given sorter/broker config.
#
# Every knob is a JVM system property read by the sorter, so one jar serves every experimental arm.
# They MUST be set via `jvm.args` under [nc] in cc.conf: the NCService spawns the NC processes, so
# JAVA_OPTS from your shell is silently ignored (see sort-testing-traps).
#
# Usage:
#   ./deploy.sh --broker none                              # E1 no-harm arm
#   ./deploy.sh --broker periodic --period 20 --fraction 0.5 --action reclaim   # E3/E4
#   ./deploy.sh --broker distribution --distribution t --mean 0.5 --stddev 0.2
#   ./deploy.sh --broker none --bucket-bytes 999999999     # Stage 1 disabled (stock sorter)
#   ./deploy.sh --broker none --partial-spill false        # Stage 2 disabled
#   ./deploy.sh --no-build --broker none                   # skip mvn, just reconfigure+restart
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
ASM="$REPO_ROOT/asterixdb/asterix-server/target/asterix-server-0.9.10-SNAPSHOT-binary-assembly/apache-asterixdb-0.9.10-SNAPSHOT"
CLUSTER="$ASM/opt/local"
JARREPO="$ASM/repo"
JAR="$REPO_ROOT/hyracks-fullstack/hyracks/hyracks-dataflow-std/target/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar"
# macOS default; on Linux set JAVA_HOME in the environment (the assembly needs JDK 21+).
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk ]]; then
  export JAVA_HOME=/opt/homebrew/opt/openjdk
fi

BROKER=random; PERIOD=10; ACTION=reclaim; FRACTION=0.5
VICTIM_PROB=0.3; SEED=0; DISTRIBUTION=normal; MEAN=0.5; STDDEV=0.15; DF=5
SCRIPT_PATH=""; BUCKET_BYTES=262144; MERGE_FAN_IN=2; PARTIAL_SPILL=true
VICTIM_INTERVAL=10; HEAP=4g; BUILD=1; JAR_OVERRIDE=""; PHASE_LOG=false; KWAY=false; CAPMULT=4; BUCKET_TUPLES=0; AUTOKEY=false; AUTOKEYINTS=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker) BROKER="$2"; shift 2;;
    --period) PERIOD="$2"; shift 2;;
    --action) ACTION="$2"; shift 2;;
    --fraction) FRACTION="$2"; shift 2;;
    --victim-prob) VICTIM_PROB="$2"; shift 2;;
    --seed) SEED="$2"; shift 2;;
    --distribution) DISTRIBUTION="$2"; shift 2;;
    --mean) MEAN="$2"; shift 2;;
    --stddev) STDDEV="$2"; shift 2;;
    --df) DF="$2"; shift 2;;
    --script) SCRIPT_PATH="$2"; shift 2;;
    --bucket-bytes) BUCKET_BYTES="$2"; shift 2;;
    --merge-fan-in) MERGE_FAN_IN="$2"; shift 2;;
    --partial-spill) PARTIAL_SPILL="$2"; shift 2;;
    --victim-interval) VICTIM_INTERVAL="$2"; shift 2;;
    --heap) HEAP="$2"; shift 2;;
    --no-build) BUILD=0; shift;;
    --jar) JAR_OVERRIDE="$2"; BUILD=0; shift 2;;
    --phase-log) PHASE_LOG="$2"; shift 2;;
    --kway) KWAY="$2"; shift 2;;
    --cap-mult) CAPMULT="$2"; shift 2;;
    --bucket-tuples) BUCKET_TUPLES="$2"; shift 2;;
    --auto-type-key) AUTOKEY="$2"; shift 2;;
    --auto-key-ints) AUTOKEYINTS="$2"; shift 2;;
    *) echo "unknown option: $1" >&2; exit 2;;
  esac
done

JVM_ARGS="-Xmx$HEAP"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker=$BROKER"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.period=$PERIOD"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.action=$ACTION"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.fraction=$FRACTION"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.victimProbability=$VICTIM_PROB"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.seed=$SEED"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.distribution=$DISTRIBUTION"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.mean=$MEAN"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.stddev=$STDDEV"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.df=$DF"
[[ -n "$SCRIPT_PATH" ]] && JVM_ARGS="$JVM_ARGS -Dhyracks.sort.broker.script=$SCRIPT_PATH"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.bucketTargetBytes=$BUCKET_BYTES"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.mergeFanIn=$MERGE_FAN_IN"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.partialSpill=$PARTIAL_SPILL"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.victimCheckInterval=$VICTIM_INTERVAL"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.phaseLog=$PHASE_LOG"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.kwayMerge=$KWAY"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.adaptCapMultiplier=$CAPMULT"
JVM_ARGS="$JVM_ARGS -Dhyracks.sort.bucketTargetTuples=$BUCKET_TUPLES"
# Runtime sort-key type detection is chosen at QUERY COMPILE time, which happens on the CC -- not
# the NC. The CC is launched by bin/asterixcc, which honours $JAVA_OPTS; the NCs are spawned by the
# NCService and only see jvm.args. Set it in BOTH: the CC picks the normalizer, the NC runs the
# sorter that consumes it.
AUTOKEY_ARGS="-Dasterix.sort.autoTypeKey=$AUTOKEY -Dasterix.sort.autoKeyInts=$AUTOKEYINTS"
JVM_ARGS="$JVM_ARGS $AUTOKEY_ARGS"
export JAVA_OPTS="${JAVA_OPTS:-} $AUTOKEY_ARGS"

echo "[deploy] jvm.args = $JVM_ARGS"

if [[ $BUILD -eq 1 ]]; then
  echo "[deploy] building hyracks-dataflow-std..."
  ( cd "$REPO_ROOT/hyracks-fullstack" && mvn -o -q -pl hyracks/hyracks-dataflow-std package -DskipTests )
fi

echo "[deploy] stopping cluster..."
( cd "$CLUSTER" && bin/stop-sample-cluster.sh >/dev/null 2>&1 || true )

if [[ -n "$JAR_OVERRIDE" ]]; then
  cp "$JAR_OVERRIDE" "$JARREPO/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar"
  echo "[deploy] jar deployed from override: $JAR_OVERRIDE"
elif [[ $BUILD -eq 1 ]]; then
  cp "$JAR" "$JARREPO/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar"
  echo "[deploy] jar deployed: $(ls -l "$JARREPO/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar" | awk '{print $6,$7,$8}')"
fi

# Rewrite the [nc] section's jvm.args line (adding it if absent).
CONF="$CLUSTER/conf/cc.conf"
[[ -f "$CONF.orig" ]] || cp "$CONF" "$CONF.orig"
python3 - "$CONF" "$JVM_ARGS" <<'PY'
import re, sys
conf, jvm = sys.argv[1], sys.argv[2]
s = open(conf).read()
s = re.sub(r'(?m)^jvm\.args=.*\n', '', s)          # drop any previous setting
s = re.sub(r'(?m)^(\[nc\]\n(?:[^\[]*?))(\n\[)', r'\1jvm.args=' + jvm.replace('\\', '\\\\') + r'\n\2', s, count=1)
open(conf, 'w').write(s)
PY
grep -A 5 '^\[nc\]$' "$CONF" | head -6

if [[ -n "$JAR_OVERRIDE" ]]; then
  WANT=$(md5sum "$JAR_OVERRIDE" | cut -d" " -f1)
  GOT=$(md5sum "$JARREPO/hyracks-dataflow-std-0.3.10-SNAPSHOT.jar" | cut -d" " -f1)
  if [[ "$WANT" != "$GOT" ]]; then
    echo "[deploy] ERROR: deployed jar md5 $GOT != requested $WANT" >&2; exit 1
  fi
  echo "[deploy] jar verified: md5 $GOT"
fi

echo "[deploy] starting cluster..."
( cd "$CLUSTER" && bin/start-sample-cluster.sh 2>&1 | tail -1 )

# Prove the configuration actually reached the NC JVMs rather than being silently ignored.
# The broker line is only emitted when a sort operator is constructed, so issue one throwaway
# sort and read ONLY the log lines added after this restart -- reading the whole log would show
# stale lines from a previous arm and give a false confirmation.
sleep 2
BEFORE=$(cat "$CLUSTER"/logs/nc-*.log 2>/dev/null | wc -l | tr -d ' ')
# Confirmation sort. Requirements: must construct the external sort operator (so the broker line
# is emitted) AND be fast, since this runs on EVERY deploy.
#   - sorting range() does NOT construct it (measured: 0 broker lines) -- the optimizer handles it
#     another way, so it silently confirms nothing
#   - the 10M-row table DOES, but took ~5 min per deploy -- more work than the experiment itself
#   - the 400k-row `small.ds` does, in ~1s
# Create small.ds once per instance (the correctness check does). If it is absent, the
# confirmation below simply reports that it could not verify, rather than failing the deploy.
curl -s -m 120 -o /dev/null "http://127.0.0.1:19002/query/service" \
  --data-urlencode 'statement=USE small; SELECT VALUE count(*) FROM (SELECT VALUE r FROM ds AS r ORDER BY r.k) AS x;' || true
# NOTE: deliberately no LIMIT -- a LIMIT routes to the top-K sorter (HeapSortRunGenerator), which
# never constructs the adaptive run generator, so the broker line would never appear.
# `|| true`: with `set -o pipefail`, grep finding nothing would return 1 and abort the script.
# The stock/master jar legitimately has no broker line, so "no match" is an expected outcome.
ACTUAL=$(cat "$CLUSTER"/logs/nc-*.log 2>/dev/null | tail -n +$((BEFORE+1)) \
         | grep -o 'adaptive-sort-broker: policy=[^ ]*' | tail -1 || true)
if [[ -n "$ACTUAL" ]]; then
  echo "[deploy] CONFIRMED in NC: $ACTUAL"
  if [[ "$ACTUAL" != *"policy=$BROKER"* ]]; then
    echo "[deploy] ERROR: requested broker '$BROKER' but NC reports '$ACTUAL'" >&2
    exit 1
  fi
elif [[ "$BROKER" == "stock" ]]; then
  echo "[deploy] stock/master jar deployed (no adaptive-sort-broker line expected)"
else
  echo "[deploy] WARNING: could not confirm broker policy from NC logs" >&2
fi
