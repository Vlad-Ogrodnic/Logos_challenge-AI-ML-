# feature_extraction.py
import os
import numpy as np
import pandas as pd
from PIL import Image
import imagehash
import cv2

INPUT_TIGHT = "preprocessed_logos_tight"
INPUT_MASK  = "preprocessed_logos_mask"
OUTPUT_CSV  = "logo_features.csv"

def phash_vec(img_pil, hash_size=8):
    h = imagehash.phash(img_pil, hash_size=hash_size)
    a = h.hash.astype(np.int8) * 2 - 1
    return a.flatten().astype(np.int8)

def hsv_hist(arr_rgb, alpha_mask):
    hsv = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2HSV)
    hist = cv2.calcHist([hsv],[0,1,2],alpha_mask,[3,8,8],[0,180,0,256,0,256])
    hist = cv2.normalize(hist, hist).flatten().astype(np.float32)
    return hist

def edge_vec(arr_rgb, alpha_mask):
    gray = cv2.cvtColor(arr_rgb, cv2.COLOR_RGB2GRAY)
    kernel = np.ones((3,3), np.uint8)
    mask_erode = cv2.erode(alpha_mask, kernel, iterations=1)
    gray_masked = np.where(mask_erode>0, gray, 0).astype(np.uint8)
    edges = cv2.Canny(gray_masked, 100, 200)
    small = cv2.resize(edges, (8,8), interpolation=cv2.INTER_AREA)
    return (small.flatten()/255.0).astype(np.float32), edges

def mask_phash_vec(mask_img_128):
    h = imagehash.phash(mask_img_128, hash_size=8)
    a = h.hash.astype(np.int8) * 2 - 1
    return a.flatten().astype(np.int8)

def hu_moments(mask_arr_128):
    _, bw = cv2.threshold(mask_arr_128, 0, 255, cv2.THRESH_BINARY)
    m = cv2.moments(bw)
    hu = cv2.HuMoments(m).flatten()
    hu = np.sign(hu) * np.log10(np.abs(hu) + 1e-12)
    return hu.astype(np.float32)

def hog128(arr_gray, mask_uint8):
    arr_gray = cv2.resize(arr_gray, (128,128), interpolation=cv2.INTER_AREA)
    mask_u = cv2.resize(mask_uint8, (128,128), interpolation=cv2.INTER_NEAREST)
    gx = cv2.Sobel(arr_gray, cv2.CV_32F, 1, 0, ksize=3)
    gy = cv2.Sobel(arr_gray, cv2.CV_32F, 0, 1, ksize=3)
    mag, ang = cv2.cartToPolar(gx, gy, angleInDegrees=True)
    ang = ang % 180.0
    mag *= (mask_u > 0).astype(np.float32)
    bins = 8
    grid = 4
    hcell = 128 // grid
    wcell = 128 // grid
    feat = []
    for cy in range(grid):
        for cx in range(grid):
            y0, y1 = cy*hcell, (cy+1)*hcell
            x0, x1 = cx*wcell, (cx+1)*wcell
            m = mag[y0:y1, x0:x1]
            a = ang[y0:y1, x0:x1]
            hist = np.zeros(bins, dtype=np.float32)
            bin_idx = np.clip((a/(180.0/bins)).astype(np.int32), 0, bins-1)
            for k in range(bins):
                hist[k] = m[bin_idx==k].sum()
            feat.extend(hist)
    feat = np.array(feat, dtype=np.float32)
    n = np.linalg.norm(feat)
    if n > 0: feat /= n
    return feat  # length 128

def extract_for_file(tight_path, mask_path):
    t = Image.open(tight_path).convert("RGBA")
    arr = np.array(t)
    rgb = arr[...,:3]
    a = arr[...,3]
    mask = np.where(a>0, 255, 0).astype(np.uint8)

    im128_rgb = Image.fromarray(cv2.resize(rgb, (128,128), interpolation=cv2.INTER_AREA))
    ph64 = phash_vec(im128_rgb, hash_size=8)
    ph144 = phash_vec(im128_rgb, hash_size=12)

    hist = hsv_hist(rgb, mask)
    edge64, edges_full = edge_vec(rgb, mask)
    eph64 = phash_vec(Image.fromarray(edges_full).convert("L"), hash_size=8)

    m = Image.open(mask_path).convert("L")
    m128 = m.resize((128,128), Image.Resampling.NEAREST)
    m_arr_128 = np.array(m128)
    mph64 = mask_phash_vec(m128.convert("RGB"))
    hu7 = hu_moments(m_arr_128)

    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    hog = hog128(gray, mask)

    return ph64, ph144, hist, edge64, eph64, mph64, hu7, hog
# after you have m_arr_128 (128x128 uint8 mask) or build it:
def mask_profiles(mask_uint8):
    # normalize to 128x128 if needed
    if mask_uint8.shape != (128,128):
        mask_uint8 = cv2.resize(mask_uint8, (128,128), interpolation=cv2.INTER_NEAREST)
    m = (mask_uint8 > 0).astype(np.float32)
    h = m.mean(axis=1)  # row occupancy
    v = m.mean(axis=0)  # col occupancy
    # downsample to 32 bins each
    h32 = cv2.resize(h[None, :], (32, 1), interpolation=cv2.INTER_AREA).flatten()
    v32 = cv2.resize(v[None, :], (32, 1), interpolation=cv2.INTER_AREA).flatten()
    # L2-normalize
    h32 /= (np.linalg.norm(h32) + 1e-8)
    v32 /= (np.linalg.norm(v32) + 1e-8)
    return h32.astype(np.float32), v32.astype(np.float32)

def main():
    rows = []
    files = sorted([f for f in os.listdir(INPUT_TIGHT) if f.lower().endswith(".png")])
    for fname in files:
        tp = os.path.join(INPUT_TIGHT, fname)
        mp = os.path.join(INPUT_MASK, fname)
        if not os.path.exists(mp):
            continue
        m = Image.open(mp).convert("L")
        m_arr_128 = np.array(m.resize((128, 128), Image.Resampling.NEAREST))
        ph64, ph144, hist, edge64, eph64, mph64, hu7, hog = extract_for_file(tp, mp)
        row = {"site": fname}
        hprof, vprof = mask_profiles(m_arr_128)
        for i, v in enumerate(hprof, 1): row[f"mp_h{i}"] = float(v)
        for i, v in enumerate(vprof, 1): row[f"mp_v{i}"] = float(v)
        for i,v in enumerate(ph64,1):   row[f"ph{i}"] = int(v)
        for i,v in enumerate(ph144,1):  row[f"ph12_{i}"] = int(v)
        for i,v in enumerate(hist,1):   row[f"h{i}"] = float(v)
        for i,v in enumerate(edge64,1): row[f"e{i}"] = float(v)
        for i,v in enumerate(eph64,1):  row[f"eph{i}"] = int(v)
        for i,v in enumerate(mph64,1):  row[f"mph{i}"] = int(v)
        for i,v in enumerate(hu7,1):    row[f"hu{i}"] = float(v)
        for i,v in enumerate(hog,1):    row[f"hog{i}"] = float(v)
        rows.append(row)
    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False)
    print(f"🚀 Saved features for {len(rows)} images to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
