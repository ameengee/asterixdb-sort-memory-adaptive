#!/usr/bin/env python3
"""
The U-curve figure: query time vs sort memory across the full range AsterixDB supports.

Combines two sources, both 10-40 trials per point:
  results/followups/lowmem.txt  512KB / 1MB / 2MB   (stock and k-way)
  results/noharm/timings.txt    3.2MB / 32MB / 320MB / 2GB (all three arms)

The point of the figure: stock has a genuine minimum at ~1MB and degrades monotonically above it,
reaching +45% by 2GB. AsterixDB's DEFAULT of 32MB already sits 22% past that optimum. The k-way
sorter does not move the minimum -- it flattens the right-hand branch.
"""
import collections, statistics as st, math
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

MB = {"512KB":0.5,"1MB":1.0,"2MB":2.0,"3200KB":3.2,"32MB":32.0,"320MB":320.0,"2048MB":2048.0}
LBL = {"3200KB":"3.2MB","2048MB":"2GB"}
DEFAULT_MB = 32.0

def load_noharm(path, discard=2):
    out=collections.defaultdict(lambda: collections.defaultdict(list))
    tmp=collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))
    for line in open(path):
        if not line.startswith("TIME"): continue
        p=line.split()
        if len(p)<5: continue
        tmp[p[1]][p[2]][p[3].split("_")[0]].append(float(p[4]))
    for arm in tmp:
        for lvl in tmp[arm]:
            for r,v in tmp[arm][lvl].items(): out[arm][lvl].extend(v[discard:])
    return out

def load_lowmem(path):
    out=collections.defaultdict(lambda: collections.defaultdict(list))
    for line in open(path):
        p=line.split()
        if p and p[0]=="LOWMEM":
            arm = "stock" if p[1]=="stock" else "adapt-kway"
            out[arm][p[2]].append(float(p[3]))
    return out

def ci95(xs):
    return 1.96*st.pstdev(xs)/math.sqrt(len(xs)) if len(xs)>1 else 0.0

nh=load_noharm("results/noharm/timings.txt")
lm=load_lowmem("results/followups/lowmem.txt")
data=collections.defaultdict(dict)
for arm in ("stock","adapt-kway"):
    for src in (lm,nh):
        for lvl,v in src.get(arm,{}).items():
            if v: data[arm][lvl]=v
for lvl,v in nh.get("adapt-eager",{}).items():
    if v: data["adapt-eager"][lvl]=v

STYLE={"stock":("#3B6EA5","o","stock AsterixDB"),
       "adapt-eager":("#7A9E3F","s","adaptive, eager cascade"),
       "adapt-kway":("#C2571A","^","adaptive, k-way merge")}

fig,ax=plt.subplots(figsize=(7.4,4.5))
for arm in ("stock","adapt-eager","adapt-kway"):
    if arm not in data: continue
    lv=sorted(data[arm], key=lambda l: MB[l])
    xs=[MB[l] for l in lv]; ys=[st.median(data[arm][l]) for l in lv]
    es=[ci95(data[arm][l]) for l in lv]
    c,m,lab=STYLE[arm]
    ax.errorbar(xs,ys,yerr=es,marker=m,ms=6,lw=1.9,capsize=4,color=c,label=lab)

s=data["stock"]; slv=sorted(s,key=lambda l:MB[l])
best=min(slv,key=lambda l: st.median(s[l])); bv=st.median(s[best])
ax.plot([MB[best]],[bv],marker="*",ms=17,color="#3B6EA5",zorder=6)
ax.annotate(f"stock optimum\n{best} ({bv:.1f}s)", xy=(MB[best],bv),
            xytext=(MB[best]*1.5, bv-1.5), fontsize=8, color="#3B6EA5",
            arrowprops=dict(arrowstyle="->", color="#3B6EA5", lw=0.9))
ax.axvline(DEFAULT_MB,color="#888888",lw=1,ls=":")
lo,hi=ax.get_ylim()
ax.annotate("AsterixDB\ndefault (32MB)", xy=(DEFAULT_MB*1.12, lo+(hi-lo)*0.03),
            fontsize=8, color="#666666", va="bottom")
ax.set_xscale("log")
allv=sorted({l for a in data for l in data[a]}, key=lambda l: MB[l])
ax.set_xticks([MB[l] for l in allv],[LBL.get(l,l) for l in allv])
ax.get_xaxis().set_minor_formatter(plt.NullFormatter())
ax.set_xlabel("sort memory per operator per partition (log scale)")
ax.set_ylabel("query time (seconds)")
ax.set_title("The sort-memory U-curve, and where it is flattened", fontsize=12, loc="left")
ax.grid(True,color="#DDDDDD",lw=0.7); ax.set_axisbelow(True)
ax.legend(fontsize=8.5,loc="upper center",bbox_to_anchor=(0.5,-0.16),ncol=3,frameon=False)
fig.text(0.5,0.015,"Median of 10-32 trials per point; bars are 95% CI. 10M rows, 2 partitions. "
         "Stock degrades +45% from its 1MB optimum by 2GB.",ha="center",fontsize=7.5,color="#555555")
fig.tight_layout(rect=[0,0.13,1,1])
for e in ("pdf","png"): fig.savefig(f"figures/fig7_ucurve.{e}",dpi=200)
print(f"{'level':>8}{'stock':>9}{'vs best':>10}{'k-way':>9}{'gain':>9}")
for l in slv:
    sv=st.median(s[l]); kv=st.median(data['adapt-kway'][l]) if l in data['adapt-kway'] else None
    print(f"{LBL.get(l,l):>8}{sv:>9.2f}{100*(sv-bv)/bv:>9.1f}%"
          + (f"{kv:>9.2f}{100*(kv-sv)/sv:>8.1f}%" if kv else f"{'--':>9}{'--':>9}"))
print("\nwrote figures/fig7_ucurve.{pdf,png}")
