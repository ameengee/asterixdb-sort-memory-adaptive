/*
 * Page-cache balloon. Every sort number we have measured so far is CPU-only: the 663MB dataset sits
 * entirely in a ~37GB page cache, so no run ever touches a disk. That setup can show what bucketing
 * and normalized keys COST but is structurally incapable of showing what overlapping I/O SAVES.
 *
 * This pins a large anonymous region in RAM so the kernel must evict file pages, forcing the sort's
 * reads to reach the device.
 *
 * Shared machine: size this so other users keep their memory. Pass the size in GB; the program
 * refuses to take so much that little is left, and releases everything on SIGINT/SIGTERM.
 *
 *   gcc -O2 -o balloon balloon.c && ./balloon 44
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/sysinfo.h>

static volatile sig_atomic_t stop = 0;
static void on_signal(int s) { (void) s; stop = 1; }

int main(int argc, char **argv) {
    if (argc != 2) { fprintf(stderr, "usage: %s <gigabytes>\n", argv[0]); return 2; }
    double gb = atof(argv[1]);
    if (gb <= 0) { fprintf(stderr, "size must be positive\n"); return 2; }

    struct sysinfo si;
    if (sysinfo(&si) != 0) { perror("sysinfo"); return 1; }
    double total_gb = (double) si.totalram * si.mem_unit / (1024.0 * 1024 * 1024);
    /* Leave headroom for the cluster, the OS, and anyone else on this shared box. */
    double max_gb = total_gb - 12.0;
    if (gb > max_gb) {
        fprintf(stderr, "refusing: %.1f GB of %.1f GB total leaves under 12 GB headroom "
                        "(max %.1f GB)\n", gb, total_gb, max_gb);
        return 2;
    }

    size_t bytes = (size_t) (gb * 1024 * 1024 * 1024);
    void *p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED) { perror("mmap"); return 1; }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /* Touch every page so the pages are really resident, not just reserved. */
    long page = sysconf(_SC_PAGESIZE);
    for (size_t off = 0; off < bytes; off += page) {
        ((char *) p)[off] = 1;
        if (stop) break;
    }
    if (mlock(p, bytes) != 0) {
        /* Not fatal: without mlock the pages can be swapped, but they are still resident and
         * still evict file cache, which is what we need. Report it so it is not a silent downgrade. */
        perror("mlock (continuing without it; raise RLIMIT_MEMLOCK to pin)");
    }
    printf("balloon: holding %.1f GB of %.1f GB total; Ctrl-C or SIGTERM to release\n", gb, total_gb);
    fflush(stdout);

    while (!stop) sleep(1);
    munmap(p, bytes);
    printf("balloon: released\n");
    return 0;
}
