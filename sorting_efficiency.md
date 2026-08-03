# Why Splitting a Sort into Buffers Does Not Change the Asymptotic Complexity

## Setup

Suppose we have:

| Symbol | Meaning |
| :----: | :------ |
| **N** | total records |
| **K** | buffers |
| **B** | records per buffer |

such that:

> **N = K × B**

For example:

> **N = 10,000**  •  **K = 10**  •  **B = 1,000**

---

## Traditional Sort

A comparison-based sort requires:

> **O(N · log N)**

work.

For **N = 10,000**:

> **10,000 × log₂(10,000)**

Since **log₂(10,000) ≈ 13.3**, the work is approximately:

> **≈ 133,000 comparison units**

---

## Buffered Sort

Instead of sorting all **N** records at once:

1. Split records into **K** buffers.
2. Sort each buffer independently.
3. Perform a **K**-way merge.

### Step 1: Sort Each Buffer

Each buffer contains **B** records, so sorting one buffer costs **B · log B** and sorting all **K** buffers costs:

> **K × (B · log B)**

Since **N = K · B**, this becomes:

> **N · log B**

For the example:

> **10 × (1,000 × log₂(1,000))**
> **= 10 × (1,000 × 10)**
> **= 100,000 comparison units**

---

### Step 2: K-Way Merge

A **K**-way merge processes every record once, and each extraction from the merge heap costs **log K**. Therefore the merge cost is:

> **N · log K**

For the example:

> **10,000 × log₂(10)**
> **= 10,000 × 3.3**
> **= 33,000 comparison units**

---

## Total Cost

Adding the buffer-sorting cost **N · log B** and the merge cost **N · log K**:

> **N · log B  +  N · log K**

Factor out **N**:

> **N · (log B + log K)**

Using the logarithm identity **log B + log K = log(B · K)**, we obtain:

> **N · log(B · K)**

Since **B · K = N**, the final result is:

> ## ➜ N · log N

---

## Conclusion

The total work of

```text
( sort each buffer independently )   →   N · log B
            +
( perform a K-way merge )            →   N · log K
```

is

> **O(N · log N)**

which is exactly the same asymptotic complexity as sorting all **N** records at once.

---

# What This Means for Memory-Adaptive Sorting

The benefit of splitting the sort into many buffers is **not** that it reduces sorting complexity.

The benefit is that it creates many small, independently manageable units of work.

This enables:

- Memory adaptation
- Frequent broker check-ins
- Predictable memory reclamation latency
- Safe points for resource management

while preserving the same asymptotic sorting cost:

> **O(N · log N)**

The improvement is therefore a **resource-management improvement**, not a **sorting-algorithm improvement**.
