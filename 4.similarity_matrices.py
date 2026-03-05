# cluster_logos.py
import os, shutil, csv
import numpy as np
import pandas as pd
from collections import defaultdict
from PIL import Image
import cv2

FEATURE_CSV   = "logo_features.csv"
TIGHT_DIR     = "preprocessed_logos_tight"
MASK_DIR      = "preprocessed_logos_mask"
OUTPUT_CSV    = "logo_groups.csv"
CLUSTERS_DIR  = "logo_clusters"
CLUSTERS_FLAT = "logo_clusters_flat"
MASTER_DIR    = "preprocessed_logos"

# -------------------- Knobs --------------------
# Gate floors (Pass-1)
#1)
TAU_SHAPE      = 0.66      # weighted pHash floor (do these logos still look the same if you blur them a lot)
#2) colour invariant and more stable:
PROF_GATE      = 0.65      # multi-scale profile floor (H/V occupancy) (Do these two logos put their ink in the same places across rows and columns?)
#3)
CHAMFER_GATE   = 0.55      # chamfer-on-outline floor

# Scoring / acceptance
TAU            = 0.92      # blended score threshold (shape-centric)
MASK_VALID_RANGE = (0.05, 0.95)
NEAR_DUP_PH    = 0.95
NEAR_DUP_MPH   = 0.90
KNN            = 12
TRI_M          = 20

# Chamfer similarity: sim = exp(-alpha * distance)
CHAMFER_ALPHA        = 0.15

# Mono detection (foreground saturation/value, masked)
S_MONO_MAX    = 0.22
V_MONO_MAX    = 0.70

# For unions: dark/mono pairs must be mutual-kNN only (no triangle glue)
# ------------------------------------------------

df = pd.read_csv(FEATURE_CSV)
sites = df["site"].values

def take(prefix):
    cols = [c for c in df.columns if c.startswith(prefix)]
    return df[cols].values if cols else None

def take_ph64_only():
    cols = [c for c in df.columns
            if c.startswith("ph") and not c.startswith("ph12_")]
    return df[cols].values if cols else None

PH = take_ph64_only()
if PH is None:
    raise RuntimeError("No 64-bit pHash columns (ph1..ph64) found in features.")
PH = PH.astype(np.int8)
H     = take("h")
E     = take("e")
MPH   = take("mph")                  # ±1 (mask pHash) may be sparse/invalid
HU    = take("hu")
MPH_H = take("mp_h")  # 32-d horizontal profile
MPH_V = take("mp_v")  # 32-d vertical profile

def norm_rows(X):
    if X is None: return None
    X = X.astype(np.float32)
    n = np.linalg.norm(X, axis=1, keepdims=True); n[n==0]=1
    return X/n

def cosine(u, v):
    nu = np.linalg.norm(u); nv = np.linalg.norm(v)
    if nu==0 or nv==0: return 0.0
    return float(np.dot(u, v) / (nu*nv))

Hn = norm_rows(H)
En = norm_rows(E)

# ---------- Weighted pHash (de-bias low-entropy bits) ----------
def ph_bit_weights(PH_bits):
    # PH_bits: (N, 64) in {+1, -1}
    mu = PH_bits.mean(axis=0).astype(np.float32)  # in [-1,1]
    w  = 1.0 - np.abs(mu)                         # high variance -> high weight
    w  = np.clip(w, 1e-4, None)
    return w / w.sum()

WPH = ph_bit_weights(PH)

def ham_sim_pm1_weighted(a_bits, b_bits, weights):
    # raw in [-1,1]
    raw = float(np.sum(weights * (a_bits * b_bits)))
    # map to [0,1]
    return 0.5 * (raw + 1.0)

# ---------- Mask coverage validity ----------
def mask_coverage(path):
    try:
        m = Image.open(path).convert("L")
        arr = np.array(m)
        return float((arr > 0).mean())
    except Exception:
        return 0.0

mask_valid = {}
for s in sites:
    cov = mask_coverage(os.path.join(MASK_DIR, s))
    mask_valid[s] = (MASK_VALID_RANGE[0] <= cov <= MASK_VALID_RANGE[1])

# ---------- Per-logo saturation/value (foreground-only) ----------
S_mean = np.zeros(len(sites), dtype=np.float32)
V_mean = np.zeros(len(sites), dtype=np.float32)
for idx, s in enumerate(sites):
    tp = os.path.join(TIGHT_DIR, s)
    try:
        im = Image.open(tp).convert("RGBA")
        arr = np.array(im)
        a = arr[:, :, 3] > 0
        if a.any():
            rgb = arr[:, :, :3]
            hsv = Image.fromarray(rgb).convert("HSV")
            hsv_arr = np.array(hsv)
            S_mean[idx] = float(np.median(hsv_arr[:, :, 1][a])) / 255.0
            V_mean[idx] = float(np.median(hsv_arr[:, :, 2][a])) / 255.0
    except Exception:
        pass

IS_MONO = (S_mean < S_MONO_MAX) & (V_mean < V_MONO_MAX)

# ---------- 128x128 binary masks / IoU ----------
MASK_BIN = []
for s in sites:
    p = os.path.join(MASK_DIR, s)
    try:
        m = Image.open(p).convert("L").resize((128,128), Image.Resampling.NEAREST)
        MASK_BIN.append((np.array(m) > 0).astype(np.uint8))
    except Exception:
        MASK_BIN.append(np.zeros((128,128), np.uint8))
MASK_BIN = np.stack(MASK_BIN, axis=0)

def mask_iou(i, j):
    a, b = MASK_BIN[i], MASK_BIN[j]
    inter = np.logical_and(a, b).sum()
    uni   = np.logical_or(a, b).sum()
    return 0.0 if uni == 0 else inter / uni

# ---------- Borders + distance transforms (chamfer) ----------
K3 = np.ones((3,3), np.uint8)
BORDER_BIN = []
for i in range(len(sites)):
    m = MASK_BIN[i]
    er = cv2.erode(m, K3, iterations=1)
    border = ((m > 0) & (er == 0)).astype(np.uint8)
    BORDER_BIN.append(border)
BORDER_BIN = np.stack(BORDER_BIN, axis=0)

DT = []
for i in range(len(sites)):
    inv = (BORDER_BIN[i] == 0).astype(np.uint8) * 255
    DT.append(cv2.distanceTransform(inv, cv2.DIST_L2, 3))
DT = np.stack(DT, axis=0).astype(np.float32)

def chamfer_sim(i, j):
    a, b = BORDER_BIN[i], BORDER_BIN[j]
    if a.sum() == 0 or b.sum() == 0: return 0.0
    d1 = DT[i][b > 0].mean() if (b > 0).any() else 1e6
    d2 = DT[j][a > 0].mean() if (a > 0).any() else 1e6
    d  = (d1 + d2) / 2.0
    return float(np.exp(-CHAMFER_ALPHA * d))

# ---------- Multi-scale profile similarity ----------
def pool_rows(X, factor):
    if X is None: return None
    N, D = X.shape
    if D % factor != 0: return None
    return X.reshape(N, D // factor, factor).mean(axis=2)

H32, V32 = MPH_H, MPH_V
H16 = pool_rows(MPH_H, 2) if MPH_H is not None else None
V16 = pool_rows(MPH_V, 2) if MPH_V is not None else None
H8  = pool_rows(MPH_H, 4) if MPH_H is not None else None
V8  = pool_rows(MPH_V, 4) if MPH_V is not None else None

def profile_sim_multi(i, j):
    sims = []
    if H32 is not None and V32 is not None:
        sims.append(min(cosine(H32[i], H32[j]), cosine(V32[i], V32[j])))
    if H16 is not None and V16 is not None:
        sims.append(min(cosine(H16[i], H16[j]), cosine(V16[i], V16[j])))
    if H8  is not None and V8  is not None:
        sims.append(min(cosine(H8[i],  H8[j]),  cosine(V8[i],  V8[j])))
    return min(sims) if sims else 0.0

# =================== DEBUG HELPERS ===================
pair_scores = {}   # (i,j) -> dict
used_edges  = []   # (i,j, rule)
def dbg_reset():
    pair_scores.clear(); used_edges.clear()

def dbg_log_pair(i, j, **scores):
    if i > j: i, j = j, i
    pair_scores[(i, j)] = scores

def union_dbg(parent, i, j, rule=""):
    def find_loc(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    ra, rb = find_loc(i), find_loc(j)
    if ra != rb:
        parent[rb] = ra
        a, b = (i, j) if i < j else (j, i)
        used_edges.append((a, b, rule))

def dbg_dump_group(group_id, components, sites, limit=None):
    if not (1 <= group_id <= len(components)):
        print(f"[DEBUG] group_id {group_id} out of range 1..{len(components)}"); return
    members = components[group_id - 1]
    site2idx = {s: i for i, s in enumerate(sites)}
    idxs = set(site2idx[s] for s in members if s in site2idx)
    print(f"\n[DEBUG] Edges inside group {group_id} ({len(members)} members):")
    cnt = 0
    for (i, j, rule) in used_edges:
        if i in idxs and j in idxs:
            print(f"  {sites[i]}  <->  {sites[j]}  | rule={rule} | scores={pair_scores.get((i,j),{})}")
            cnt += 1
            if limit and cnt >= limit: break
    if cnt == 0: print("  (no logged edges found inside this group)")

def dbg_list_groups(components, top=25):
    sizes = [(gid+1, len(m)) for gid, m in enumerate(components)]
    sizes.sort(key=lambda x: x[1], reverse=True)
    print("\n[DEBUG] Top groups by size:")
    for gid, sz in sizes[:top]:
        print(f"  group {gid:>4}: size={sz}")
    print(f"[DEBUG] Total groups: {len(components)}")
# ====================================================

# =================== PASS 1: candidate edges ===================
dbg_reset()
N = len(sites)
pairs = []                 # (i, j, total, is_mono_pair)
neighbor_scores = [[] for _ in range(N)]  # shape-only score for kNN ranking

for i in range(N):
    for j in range(i+1, N):
        # weighted pHash
        s_phw = ham_sim_pm1_weighted(PH[i], PH[j], WPH) if PH is not None else 0.0

        # shape signals
        s_prof = profile_sim_multi(i, j)
        s_cham = chamfer_sim(i, j)

        # --- Consensus gate (NEW): require all three to pass ---
        if (s_phw < TAU_SHAPE) or (s_prof < PROF_GATE) or (s_cham < CHAMFER_GATE):
            continue

        # other signals
        s_mph = 0.0
        if MPH is not None and mask_valid.get(sites[i], False) and mask_valid.get(sites[j], False):
            # if MPH is also 64-d, reuse WPH; otherwise use a flat weight of correct length
            if MPH.shape[1] == len(WPH):
                w_m = WPH
            else:
                w_m = np.full(MPH.shape[1], 1.0 / MPH.shape[1], dtype=np.float32)
            s_mph = ham_sim_pm1_weighted(MPH[i], MPH[j], w_m)

        s_hist = float(Hn[i] @ Hn[j]) if Hn is not None else 0.0
        s_edge = float(En[i] @ En[j]) if En is not None else 0.0

        mono_pair = bool(IS_MONO[i] and IS_MONO[j])
        # For mono pairs, ignore histogram (degenerate); use edge only
        s_histedge = s_edge if mono_pair else max(s_hist, s_edge)

        s_hu = 0.0
        if HU is not None:
            d_hu = float(np.linalg.norm(HU[i]-HU[j]))
            s_hu = float(np.exp(-0.6*d_hu))

        # shape-centric total (no color hist)
        total = 0.40*s_phw + 0.30*s_prof + 0.20*s_cham + 0.10*s_hu

        votes = 0
        if s_prof >= 0.85: votes += 1
        if s_cham >= 0.75: votes += 1
        if s_hu   >= 0.70: votes += 1

        need = 1 if s_phw >= 0.90 else 2

        # near-dup shortcut
        if mono_pair:
            near_dup = (s_phw >= NEAR_DUP_PH and s_prof >= 0.90 and s_cham >= 0.80)
        else:
            near_dup = (s_phw >= NEAR_DUP_PH) or (s_mph >= NEAR_DUP_MPH)

        ok = near_dup or (total >= TAU and votes >= need)
        if not ok:
            continue

        # record candidate
        pairs.append((i, j, float(total), mono_pair))

        # neighbor ranking uses SHAPE ONLY (prevents color/glue hubs)
        shape_neighbor = 0.6*s_prof + 0.4*s_cham
        neighbor_scores[i].append((j, float(shape_neighbor)))
        neighbor_scores[j].append((i, float(shape_neighbor)))

        # debug log the exact scores that made it in
        dbg_log_pair(
            i, j,
            total=float(total),
            s_ph=float(s_phw),
            s_mph=float(s_mph),
            s_histedge=float(s_histedge),
            s_hu=float(s_hu),
            iou=float(mask_iou(i,j)),
            s_prof=float(s_prof),
            s_cham=float(s_cham),
            mono_pair=mono_pair
        )

# build mutual-kNN sets from shape-only neighbor scores
def topk_set(lst, k):
    if not lst: return set()
    k = min(k, len(lst))
    idx = np.argpartition([-s for _,s in lst], k-1)[:k]
    return {lst[t][0] for t in idx}

topk = [topk_set(neighbor_scores[i], KNN) for i in range(N)]

# triangle support helper
def has_triangle(i, j):
    Ni = sorted(neighbor_scores[i], key=lambda x: -x[1])[:TRI_M]
    Nj = sorted(neighbor_scores[j], key=lambda x: -x[1])[:TRI_M]
    Si = {k for k,_ in Ni}; Sj = {k for k,_ in Nj}
    return len(Si & Sj) > 0

# =================== PASS 2: unions (debug-instrumented) ===================
parent = list(range(N))
def find_root(x):
    while parent[x]!=x:
        parent[x]=parent[parent[x]]
        x=parent[x]
    return x

for i, j, tot, mono_pair in pairs:
    if mono_pair:
        # No triangle glue for mono/dark-ish; require mutual-kNN
        if (j in topk[i] and i in topk[j]):
            union_dbg(parent, i, j, rule="mono_mutual_knn")
    else:
        mk  = (j in topk[i] and i in topk[j])
        tri = has_triangle(i, j)
        if mk:
            union_dbg(parent, i, j, rule="mutual_knn")
        elif tri:
            union_dbg(parent, i, j, rule="triangle")

# =================== Collect groups ===================
groups_dict = defaultdict(list)
for idx in range(N):
    groups_dict[find_root(idx)].append(sites[idx])

roots_sorted = sorted(groups_dict.keys())
components = [sorted(groups_dict[r]) for r in roots_sorted]

# Optional debug:
# dbg_list_groups(components, top=30)
# dbg_dump_group(232, components, sites, limit=50)

# =================== Write CSV + folders ===================
flat_rows = []
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["site","group_id"])
    for gid, members in enumerate(components, 1):
        for s in members:
            w.writerow([s, gid])
            flat_rows.append((gid, s))

# clean/create output dirs
if os.path.exists(CLUSTERS_DIR):  shutil.rmtree(CLUSTERS_DIR)
if os.path.exists(CLUSTERS_FLAT): shutil.rmtree(CLUSTERS_FLAT)
os.makedirs(CLUSTERS_DIR, exist_ok=True)
os.makedirs(CLUSTERS_FLAT, exist_ok=True)

# subfolders (tight)
for gid, members in enumerate(components, 1):
    d = os.path.join(CLUSTERS_DIR, f"group_{gid}")
    os.makedirs(d, exist_ok=True)
    for s in members:
        src = os.path.join(TIGHT_DIR, s)
        if os.path.exists(src):
            shutil.copy(src, os.path.join(d, s))

# flat folder (master if present, else tight)
for gid, s in flat_rows:
    src_master = os.path.join(MASTER_DIR, s)
    src = src_master if os.path.exists(src_master) else os.path.join(TIGHT_DIR, s)
    if os.path.exists(src):
        base, ext = os.path.splitext(s)
        dst_name = f"{gid}_{base}{ext}"
        shutil.copy(src, os.path.join(CLUSTERS_FLAT, dst_name))

print(f"✅ Wrote {len(components)} groups to {OUTPUT_CSV}")
print(f"📁 Subfolders: {CLUSTERS_DIR}")
print(f"📁 Flat folder: {CLUSTERS_FLAT}")
