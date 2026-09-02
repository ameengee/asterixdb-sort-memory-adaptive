#!/usr/bin/env bash
# Invalidate the page cache for the dataset between reps, so each rep actually reads from the
# device instead of replaying the previous rep's cached pages. Without this, rep 2..N measure a
# warm cache and the whole I/O experiment silently becomes another CPU experiment.
#
# `echo 3 > /proc/sys/vm/drop_caches` needs root, which we do not have on the lab machines. This
# uses posix_fadvise(DONTNEED) instead, which drops the cached pages for the files we name --
# and, unlike drop_caches, does not evict other users' data on this shared box.
set -uo pipefail
DIR=${1:?usage: drop_caches.sh <storage-dir>}
python3 - "$DIR" <<'PY'
import os, sys
root = sys.argv[1]
n = bytes_ = 0
for dirpath, _, files in os.walk(root):
    for fn in files:
        p = os.path.join(dirpath, fn)
        try:
            fd = os.open(p, os.O_RDONLY)
        except OSError:
            continue
        try:
            sz = os.fstat(fd).st_size
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            n += 1; bytes_ += sz
        except OSError:
            pass
        finally:
            os.close(fd)
print(f"dropped cache for {n} files, {bytes_/1024/1024/1024:.2f} GB")
PY
