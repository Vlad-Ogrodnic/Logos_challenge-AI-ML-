import os, io, csv, math, glob, shutil, subprocess
import numpy as np
from PIL import Image, UnidentifiedImageError, ImageFilter

try:
    from cairosvg import svg2png as _svg2png
except Exception:
    _svg2png = None

INPUT_DIR = "logos_1000"
OUT_MASTER = "preprocessed_logos"          # square, transparent
OUT_TIGHT  = "preprocessed_logos_tight"    # tight content tile (square)
OUT_MASK   = "preprocessed_logos_mask"     # tight alpha mask (square, L)
SIZE_MASTER = (512, 512)
SIZE_TIGHT  = (256, 256)
SUPPORTED = (".png",".jpg",".jpeg",".bmp",".gif",".svg",".ico",".webp")
MAX_WORKERS = os.cpu_count() or 4
VERSION = "p2-tight-1.0"

def svg_to_png_bytes(path, width, height):
    if _svg2png is not None:
        return _svg2png(url=path, output_width=width, output_height=height, background_color='transparent')
    if shutil.which("rsvg-convert"):
        out = subprocess.check_output(["rsvg-convert","-w",str(width),"-h",str(height),path])
        return out
    if shutil.which("inkscape"):
        tmp = path + ".__tmp_png"
        subprocess.run(["inkscape",path,"--export-type=png",f"--export-filename={tmp}",f"--export-width={width}",f"--export-height={height}"],check=True)
        with open(tmp,"rb") as f: data=f.read()
        os.remove(tmp)
        return data
    raise RuntimeError("No SVG renderer")

def remove_bg_safe(img, tol=6):
    a = np.array(img.convert("RGBA"))
    if (a[...,3] < 255).any(): return Image.fromarray(a, "RGBA")
    h,w = a.shape[:2]
    corners = np.vstack([a[0,0,:3],a[0,-1,:3],a[-1,0,:3],a[-1,-1,:3]])
    bg = np.median(corners, axis=0)
    diff = np.abs(a[...,:3]-bg)
    mask = (diff<=tol).all(axis=2)
    cov = mask.mean()
    if cov>0.9 or cov<0.01: return Image.fromarray(a,"RGBA")
    a[mask,3]=0
    return Image.fromarray(a,"RGBA")

def trim_expand_feather(img):
    b = img.getbbox()
    if not b: return img,(0,0,img.width,img.height)
    x0,y0,x1,y1 = b
    x0=max(0,x0-1); y0=max(0,y0-1); x1=min(img.width,x1+1); y1=min(img.height,y1+1)
    c = img.crop((x0,y0,x1,y1))
    r,g,b,a = c.split()
    a = a.filter(ImageFilter.MaxFilter(3))
    c = Image.merge("RGBA",(r,g,b,a))
    return c,(x0,y0,x1,y1)

def classify_aspect(w,h):
    r = w/h if h else 1
    if r>1.6: return "wide"
    if r<0.625: return "tall"
    return "square"

def pad_targets(aspect):
    if aspect=="wide":  return 0.05,0.14,0.42
    if aspect=="tall":  return 0.14,0.05,0.38
    return 0.12,0.12,0.34

def fit_scale_master(img, aspect):
    px,py,Astar = pad_targets(aspect)
    w,h = img.size
    bw = SIZE_MASTER[0]*(1-2*px)
    bh = SIZE_MASTER[1]*(1-2*py)
    s_fit = min(bw/max(1,w), bh/max(1,h))
    a = np.array(img)[...,3]
    ink = int((a>0).sum())
    if ink==0:
        s = s_fit
    else:
        s_cov = math.sqrt((Astar*SIZE_MASTER[0]*SIZE_MASTER[1])/ink)
        s = min(s_fit, s_cov)
    nw,nh = max(1,int(round(w*s))), max(1,int(round(h*s)))
    rsz = img.resize((nw,nh), Image.LANCZOS)
    canv = Image.new("RGBA", SIZE_MASTER, (0,0,0,0))
    x = (SIZE_MASTER[0]-nw)//2
    y = (SIZE_MASTER[1]-nh)//2
    canv.paste(rsz,(x,y),rsz)
    a2 = np.array(canv)[...,3]
    cov = float((a2>0).mean())
    return canv, s, cov

def tight_tile(img, pad_ratio=0.03):
    w,h = img.size
    a = np.array(img)[...,3]
    ys,xs = np.where(a>0)
    if ys.size==0:
        canv = Image.new("RGBA", SIZE_TIGHT, (0,0,0,0))
        mask = Image.new("L", SIZE_TIGHT, 0)
        return canv, mask, pad_ratio
    y0,y1 = int(ys.min()), int(ys.max()+1)
    x0,x1 = int(xs.min()), int(xs.max()+1)
    crop = img.crop((x0,y0,x1,y1))
    cw,ch = crop.size
    p = int(round(pad_ratio*max(cw,ch)))
    if p>0:
        pad = Image.new("RGBA",(cw+2*p,ch+2*p),(0,0,0,0))
        pad.paste(crop,(p,p),crop)
    else:
        pad = crop
    r = pad.resize(SIZE_TIGHT, Image.LANCZOS)
    m = np.array(r)[...,3]
    mask = Image.fromarray(m, "L")
    return r, mask, pad_ratio

def process_one(path):
    name = os.path.splitext(os.path.basename(path))[0]
    ext = os.path.splitext(path)[1].lower()
    svg_bytes = None
    if ext==".svg":
        svg_bytes = svg_to_png_bytes(path, SIZE_MASTER[0]*4, SIZE_MASTER[1]*4)
        img = Image.open(io.BytesIO(svg_bytes)).convert("RGBA")
        src_fmt = "svg"
    else:
        img = Image.open(path)
        if getattr(img,"is_animated",False):
            try: img.seek(0)
            except Exception: pass
        img = img.convert("RGBA")
        src_fmt = ext.strip(".")
    try:
        img = remove_bg_safe(img, tol=6)
    except Exception:
        pass
    trimmed, bbox = trim_expand_feather(img)
    aspect = classify_aspect(trimmed.width, trimmed.height)
    master, s_final, cov = fit_scale_master(trimmed, aspect)
    tight, mask, pad_ratio = tight_tile(trimmed, pad_ratio=0.03)
    return {
        "name": name,
        "src_fmt": src_fmt,
        "bbox": bbox,
        "aspect": aspect,
        "scale": round(s_final,4),
        "ink_cov_master": round(cov,4),
        "pad_ratio_tight": pad_ratio,
        "master_img": master,
        "tight_img": tight,
        "mask_img": mask
    }

def main():
    os.makedirs(OUT_MASTER, exist_ok=True)
    os.makedirs(OUT_TIGHT,  exist_ok=True)
    os.makedirs(OUT_MASK,   exist_ok=True)
    files = sorted([f for f in glob.glob(os.path.join(INPUT_DIR,"*")) if os.path.splitext(f)[1].lower() in SUPPORTED])
    rows=[]
    ok=0
    for p in files:
        try:
            rec = process_one(p)
            mp = os.path.join(OUT_MASTER, rec["name"]+".png")
            tp = os.path.join(OUT_TIGHT,  rec["name"]+".png")
            sp = os.path.join(OUT_MASK,   rec["name"]+".png")
            rec["master_img"].save(mp,"PNG")
            rec["tight_img"].save(tp,"PNG")
            rec["mask_img"].save(sp,"PNG")
            rows.append({
                "domain": rec["name"],
                "src_path": p,
                "src_format": rec["src_fmt"],
                "master_path": mp,
                "tight_path": tp,
                "mask_path": sp,
                "bbox": f"{rec['bbox'][0]},{rec['bbox'][1]},{rec['bbox'][2]},{rec['bbox'][3]}",
                "aspect_class": rec["aspect"],
                "scale_master": rec["scale"],
                "ink_cov_master": rec["ink_cov_master"],
                "tight_pad_pct": rec["pad_ratio_tight"],
                "version": VERSION
            })
            ok+=1
            print(f"{os.path.basename(p):40s} → OK")
        except UnidentifiedImageError:
            print(f"{os.path.basename(p):40s} → UNIDENTIFIED")
        except Exception as e:
            print(f"{os.path.basename(p):40s} → ERROR: {type(e).__name__}: {e}")
    man = os.path.join(OUT_MASTER, "..", "manifest.csv")
    with open(os.path.abspath(man),"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["domain","src_path","version"])
        w.writeheader(); w.writerows(rows)
    total=len(files)
    print(f"\n✅ Preprocessed {ok}/{total} files ({(ok/total*100 if total else 0):.1f}%)")

if __name__=="__main__":
    main()