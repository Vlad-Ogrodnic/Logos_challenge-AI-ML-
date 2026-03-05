import os
import re
import time
import io
import pandas as pd
import requests
import base64
from urllib.parse import urljoin, unquote
from PIL import Image
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import ParserRejectedMarkup, FeatureNotFound
from selenium.webdriver.support import expected_conditions as EC
import threading
import shutil
import undetected_chromedriver as uc

DRIVER_PATH = ChromeDriverManager().install()   # one download, reused
SELENIUM_SEMAPHORE = threading.Semaphore(2)     # never spawn >2 browsers
#HELPER FUNCTIONS
def make_driver(opts: Options):
    """Return webdriver.Chrome using the shared DRIVER_PATH."""
    SELENIUM_SEMAPHORE.acquire()
    try:
        service = ChromeService(DRIVER_PATH)
        return webdriver.Chrome(service=service, options=opts)
    except Exception:
        SELENIUM_SEMAPHORE.release()
        raise

def safe_soup(markup: str):
    """
    Return a BeautifulSoup object, trying parsers in order:
    1) 'lxml'        (fastest)
    2) 'html.parser' (built-in)
    3) 'html5lib'    (lenient)
    """
    for parser in ("lxml", "html.parser", "html5lib"):
        try:
            return BeautifulSoup(markup, parser)
        except (ParserRejectedMarkup, FeatureNotFound, AssertionError):
            continue
    raise ParserRejectedMarkup("All parsers rejected the markup")

# --- Ensure logos folder exists ---
LOGO_DIR = "logos_6"
shutil.rmtree(LOGO_DIR, ignore_errors=True)
os.makedirs(LOGO_DIR, exist_ok=True)

# --- Setup retryable session ---
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# --- Headers ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com/",
    "DNT": "1"
}

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--disable-gpu")
opts.add_argument("--no-sandbox")
opts.add_argument(f"user-agent={headers['User-Agent']}")
opts.add_argument("--window-size=1920,1080")












#HELPERS---------------------------------------------------------------------------------------------------------
# --- UC driver factory (single non-headless window) ---
def create_uc_driver():
    if uc is None:
        return None
    options = uc.ChromeOptions()
    options.headless = False
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--log-level=3")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
    )
    # Pinning version_main like your other script did
    return uc.Chrome(options=options, version_main=138)
# --- NEW: small helpers for fixes 1,2,3,4,5 ---------------------------------
from urllib.parse import urlparse

def _save_svg_markup(svg_markup: str, domain: str, suffix: str = "") -> str:
    fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + (f"{suffix}" if suffix else "") + ".svg"
    path = os.path.join(LOGO_DIR, fname)
    with open(path, "w", encoding="utf-8") as f:
        f.write(svg_markup)
    return f"local_file:{path}"

def _extract_url_from_style(style: str, base_url: str) -> str | None:
    if not style:
        return None
    # background / background-image
    m = re.search(r'background(?:-image)?:\s*url\(([^)]+)\)', style, re.IGNORECASE)
    if m:
        return urljoin(base_url, m.group(1).strip('\'"'))
    # NEW: CSS mask-based logos
    m = re.search(r'(?:-webkit-)?mask(?:-image)?:\s*url\(([^)]+)\)', style, re.IGNORECASE)
    if m:
        return urljoin(base_url, m.group(1).strip('\'"'))
    return None

def _is_root_link(href: str | None, base_url: str) -> bool:
    if not href:
        return False
    href = href.strip()
    if href in ("", "/"):
        return True
    absu = urljoin(base_url, href)
    try:
        p = urlparse(absu)
        return (p.path == "" or p.path == "/")
    except Exception:
        return False

def _resolve_svg_use(svg_el, soup, base_url: str, domain: str) -> str | None:
    """
    If <svg> contains <use xlink:href="..."> or <use href="...">,
    inline the referenced <symbol> (from same DOM or external sprite) and save.
    """
    if svg_el is None:
        return None
    use = svg_el.find("use")
    if not use:
        return None
    href = use.get("xlink:href") or use.get("href")
    if not href:
        return None

    def _wrap_svg(symbol_markup: str, viewBox: str | None) -> str:
        vb = viewBox or svg_el.get("viewBox") or ""
        vb_attr = f' viewBox="{vb}"' if vb else ""
        return f'<svg xmlns="http://www.w3.org/2000/svg"{vb_attr}>{symbol_markup}</svg>'

    # Case A: internal symbol "#id"
    if href.startswith("#"):
        sym_id = href[1:]
        symbol = soup.find("symbol", id=sym_id)
        if symbol:
            markup = _wrap_svg("".join(str(c) for c in symbol.contents), symbol.get("viewBox"))
            return _save_svg_markup(markup, domain, suffix="_logo")
        return None

    # Case B: external sprite "path.svg#id"
    sprite_url, _, sym_id = href.partition("#")
    if not sprite_url or not sym_id:
        return None
    abs_sprite = urljoin(base_url, sprite_url)
    try:
        r = session.get(abs_sprite, headers=headers, timeout=10)
        r.raise_for_status()
        # Use XML parser for SVGs; fall back to html parser if needed
        sprite_soup = BeautifulSoup(r.text, "xml")
        symbol = sprite_soup.find("symbol", id=sym_id) or sprite_soup.find(id=sym_id)
        if symbol:
            markup = _wrap_svg("".join(str(c) for c in symbol.contents), symbol.get("viewBox"))
            return _save_svg_markup(markup, domain, suffix="_logo")
    except Exception as e:
        print(f"[⚠️] Failed to fetch/inline external sprite {abs_sprite}: {e}")
    return None

def _second_level_label(domain: str) -> str:
    try:
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-2].lower()
    except Exception:
        pass
    return (domain or "").lower()

def _anchor_brand_logo(soup, base_url, domain):
    brand = _second_level_label(domain).replace("-", " ")
    if not brand:
        return None

    # Prefer header anchors that mention the brand
    selectors = [
        f"header a[aria-label*='{brand}'], header a[title*='{brand}']",
        "header a[href='/']"  # common “home” link
    ]
    for sel in selectors:
        a = soup.select_one(sel)
        if not a:
            continue
        # Try <svg> then <img>
        svg = a.find("svg")
        if svg:
            # inline SVG: save locally
            svg_markup = str(svg)
            fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + "_logo.svg"
            path = os.path.join(LOGO_DIR, fname)
            with open(path, "w", encoding="utf-8") as f:
                f.write(svg_markup)
            return f"local_file:{path}"

        img = a.find("img")
        if img:
            u = urljoin(base_url, img.get("src") or "")
            if u:
                return u

    return None
def _svg_has_drawn_content(svg_el) -> bool:
    # If the SVG has its own geometry, it’s likely safe to save.
    return bool(svg_el.find(["path", "rect", "circle", "ellipse", "polygon", "polyline", "line", "text", "image"]))

# ---- Helpers (A) -------------------------------------------------------------

def normalize_url(base_url: str, href: str | None) -> str | None:
    """Resolve relative/protocol-relative URLs; leave data:/local_file: alone."""
    if not href:
        return None
    href = href.strip().strip('"').strip("'")
    if href.startswith(("data:", "local_file:")):
        return href
    if href.startswith("//"):
        # keep base scheme
        scheme = "https:" if base_url.startswith("https:") else "http:"
        return scheme + href
    try:
        # drop pure fragments
        if href.startswith("#"):
            return None
        return urljoin(base_url, href)
    except Exception:
        return href

def pick_largest_from_srcset(srcset_value: str | None) -> str | None:
    """Pick the largest candidate from srcset/data-srcset."""
    if not srcset_value:
        return None
    best_url, best_w = None, -1
    for part in srcset_value.split(","):
        item = part.strip()
        if not item:
            continue
        # forms: "url 640w" or "url 2x" or just "url"
        bits = item.split()
        u = bits[0]
        w = 0
        if len(bits) >= 2:
            desc = bits[1].lower()
            if desc.endswith("w"):
                try:
                    w = int(desc[:-1])
                except Exception:
                    w = 0
            elif desc.endswith("x"):
                # treat 2x ~ 2000w, 3x ~ 3000w (rough, just for ordering)
                try:
                    w = int(float(desc[:-1]) * 1000)
                except Exception:
                    w = 0
        if w >= best_w:
            best_url, best_w = u, w
    return best_url

def extract_noscript_img(tag) -> "bs4.element.Tag | None":
    """If a tag contains a <noscript> with an <img>, return that <img> tag."""
    ns = getattr(tag, "find", lambda *a, **k: None)("noscript")
    if not ns:
        return None
    try:
        ns_soup = BeautifulSoup(ns.decode_contents(), "html.parser")
        return ns_soup.find("img")
    except Exception:
        return None








# --- Your original extractor (unchanged) ---
def extract_logo_from_soup(soup, base_url, domain=None):
    import re, base64, urllib.parse
    from urllib.parse import urljoin

    # ---------- in-function helpers ----------
    def _save_inline_svg(svg_markup: str, domain: str, suffix: str = "_logo.svg") -> str | None:
        if not svg_markup or not domain:
            return None
        os.makedirs(LOGO_DIR, exist_ok=True)
        fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + suffix
        path = os.path.join(LOGO_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg_markup)
        return f"local_file:{path}"

    # ---- get_best_img_url (B) ------------------------------------------------
    def get_best_img_url(img_tag, base_url: str, domain=None) -> str | None:
        """
        Prefer largest srcset/data-srcset; then common lazy attributes; then src.
        Keep ShortPixel/Next.js unwrapping; return a fully-resolved URL.
        """
        def unwrap_shortpixel(u: str) -> str:
            lu = u.lower()
            if "shortpixel" in lu or "/sp-" in lu or "spai" in lu:
                # Try to pull out encoded original URL (https%3A%2F%2F...)
                m = re.search(r"(https?%3A%2F%2F[^&]+)", u)
                if m:
                    try:
                        return unquote(m.group(1))
                    except Exception:
                        pass
                # Or a plain http(s) inside the path/query
                m2 = re.search(r"(https?://[^?&\\s]+)", u)
                if m2 and "shortpixel" not in m2.group(1).lower():
                    return m2.group(1)
            return u

        def unwrap_nextjs(u: str) -> str:
            # /_next/image?url=<...>
            if "/_next/image" in u and "url=" in u:
                try:
                    q = u.split("?", 1)[1]
                    for kv in q.split("&"):
                        if kv.startswith("url="):
                            return unquote(kv[4:])
                except Exception:
                    return u
            return u

        def clean(u: str | None) -> str | None:
            if not u:
                return None
            u = unwrap_shortpixel(u)
            u = unwrap_nextjs(u)
            return normalize_url(base_url, u)
        # donor: handle inline SVG data URIs by saving immediately
        cand0 = (img_tag.get("src") or img_tag.get("data-src") or img_tag.get("data-lazy-src"))
        if cand0 and cand0.startswith("data:image/svg+xml"):
            if domain:
                try:
                    if cand0.startswith("data:image/svg+xml;base64,"):
                        encoded = cand0.split(",", 1)[1]
                        svg_text = base64.b64decode(encoded).decode("utf-8", errors="ignore")
                    else:
                        svg_encoded = cand0.split(",", 1)[1]
                        svg_text = unquote(svg_encoded)
                    os.makedirs(LOGO_DIR, exist_ok=True)
                    filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(svg_text)
                    return f"local_file:{filename}"
                except Exception:
                    pass  # fall through to normal handling

        # 1) srcset / data-srcset (largest)
        srcset = img_tag.get("data-srcset") or img_tag.get("srcset")
        if srcset:
            best = pick_largest_from_srcset(srcset)
            u = clean(best)
            if u:
                return u

        # 2) common lazy attrs
        for attr in (
            "data-src",
            "data-lazy-src",
            "data-original",
            "data-image",
            "data-hires",
            "data-retina",
            "data-src-large",
            "data-large_image",
            "data-zoom-image",
        ):
            val = img_tag.get(attr)
            u = clean(val)
            if u:
                return u

        # 3) fallback to src
        u = clean(img_tag.get("src"))
        if u:
            return u

        return None


    def _resolve_svg_use(svg_el) -> str | None:
        """If <svg> contains <use xlink:href="#id">, pull the referenced symbol
        into a standalone <svg> for a cleaner saved file."""
        try:
            use = svg_el.find("use")
            if not use:
                return None
            href = use.get("href") or use.get("xlink:href")
            if not href or not href.startswith("#"):
                return None
            sym = soup.find(id=href[1:])
            if not sym:
                return None
            # Build a minimal inline SVG
            viewBox = svg_el.get("viewBox") or sym.get("viewBox") or "0 0 512 128"
            inner = "".join(str(c) for c in sym.children)
            return f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{viewBox}">{inner}</svg>'
        except Exception:
            return None
    # ---------- end helpers ----------

    # ── 0) Domain micro-specials kept *inside* soup to stay short ─────────────
    if domain and domain.lower().endswith("berlitz-augsburg.de"):
        svg_el = soup.select_one("a[class*='logo__LogoLink'] svg")
        if svg_el:
            svg_markup = str(svg_el)
            saved = _save_inline_svg(svg_markup, domain, "_logo.svg")
            if saved:
                print(f"[✅ Berlitz-Augsburg] Saved inline SVG")
                return saved

    if domain and "fmlogistic" in domain.lower():
        # Prefer header anchor with brand logo; avoid OG/Twitter noise
        a = soup.select_one("header a.brand, header .brand a, a.brand")
        if a:
            # 1) <img> inside
            img = a.find("img")
            if img:
                u = get_best_img_url(img, base_url, domain)
                if u:
                    return u
            # 2) inline <svg> inside
            svg = a.find("svg")
            if svg:
                # try to resolve <use>, else dump raw svg
                rebuilt = _resolve_svg_use(svg) or str(svg)
                saved = _save_inline_svg(rebuilt, domain, "_logo.svg")
                if saved:
                    return saved
        # if nothing found, continue to generic logic below

    # ── 1) Airbnb-like / explicit anchor logo (generic) ──────────────────────
    anchors = soup.find_all("a", class_=lambda c: c and "logo" in c.lower())
    for a in anchors:
        # prefer an <img> inside
        img = a.find("img")
        if img:
            u = get_best_img_url(img, base_url, domain)
            if u:
                return u
        # else an inline svg
        svg = a.find("svg")
        if svg:
            rebuilt = _resolve_svg_use(svg) or str(svg)
            saved = _save_inline_svg(rebuilt, domain, "_logo.svg")
            if saved:
                return saved

    # aria-label=logo anchors
    anchors_aria = soup.find_all("a", attrs={"aria-label": lambda v: v and "logo" in v.lower()})
    for a in anchors_aria:
        img = a.find("img")
        if img:
            u = get_best_img_url(img, base_url, domain)
            if u:
                return u
        svg = a.find("svg")
        if svg:
            rebuilt = _resolve_svg_use(svg) or str(svg)
            saved = _save_inline_svg(rebuilt, domain, "_logo.svg")
            if saved:
                return saved

    # ── 2) ion-img / generic <i> with mask/background logo ───────────────────
    ion = soup.find("ion-img", src=True)
    if ion:
        return urljoin(base_url, ion.get("src"))

    # generic <i>/<div>/<span> with logo/brand in classes (mask/background)
    for tag in soup.find_all(["i", "div", "span"]):
        class_str = " ".join(tag.get("class", []))
        if "logo" in class_str.lower() or "brand" in class_str.lower():
            style = tag.get("style", "") or ""
            # mask: url(...) or -webkit-mask: url(...)
            m = re.search(r"mask:\s*url\(([^)]+)\)", style) or re.search(r"-webkit-mask:\s*url\(([^)]+)\)", style)
            if m:
                return urljoin(base_url, m.group(1).strip("'\""))
            # background(-image): url(...)
            m = re.search(r'background(?:-image)?:\s*url\(([^)]+)\)', style, re.I)
            if m:
                return urljoin(base_url, m.group(1).strip("'\""))

    # ── 3) <img> with "logo" signal (src/class/alt/filename) ─────────────────
    for img in soup.find_all("img"):
        class_str = " ".join(img.get("class", []))
        alt_str   = (img.get("alt") or "")
        u = get_best_img_url(img, base_url, domain)
        if u and (
            "logo" in (u or "").lower()
            or "logo" in class_str.lower()
            or "logo" in alt_str.lower()
            or "template_photo" in (u or "").lower()
        ):
            print(f"[🔍] Logo candidate found: {u}")
            return u

    # ── 4) <div>/<span> with style background *and* logo-ish classes ─────────
    for tag in soup.find_all(["div", "span"]):
        class_str = " ".join(tag.get("class", []))
        if "logo" in class_str.lower() or "brand" in class_str.lower():
            style = tag.get("style", "") or ""
            m = re.search(r'url\(([^)]+)\)', style)
            if m:
                return urljoin(base_url, m.group(1).strip("'\""))

    # ── 5) Meta OG / Twitter (last-resort before CSS/inline SVG) ─────────────
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return urljoin(base_url, og["content"])

    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return urljoin(base_url, tw["content"])

    # ── 6) Scan linked CSS for .logo/.brand background URLs ──────────────────
    try:
        css_links = [urljoin(base_url, link.get("href")) for link in soup.find_all("link", rel="stylesheet") if link.get("href")]
        for css_url in css_links:
            try:
                css_resp = session.get(css_url, headers=headers, timeout=10)
                if css_resp.status_code == 200:
                    css = css_resp.text
                    # class-qualified first
                    for url in re.findall(r'\.(?:logo|brand)[^{]*\{[^}]*background(?:-image)?:\s*url\(([^)]+)\)', css,
                                          re.I):
                        return urljoin(css_url, url.strip('\'"'))

                    # generic background lines mentioning logo/brand
                    for url in re.findall(r'background(?:-image)?:\s*url\(([^)]+)\)', css, re.I):
                        if "logo" in url.lower() or "brand" in url.lower():
                            return urljoin(css_url, url.strip("'\""))
            except Exception as e:
                print(f"[⚠️] CSS fetch failed: {css_url} - {e}")
    except Exception as e:
        print(f"[⚠️] CSS extraction error: {e}")

    # ── 7) Inline SVGs as candidates (try to identify logo-ish) ──────────────
    svg_candidates = []
    for svg in soup.find_all("svg"):
        svg_classes = " ".join(svg.get("class", []))
        parent_classes = " ".join(svg.parent.get("class", [])) if svg.parent else ""
        aria_label = (svg.get("aria-label") or "") + " " + (svg.parent.get("aria-label") or "" if svg.parent else "")
        if (
            "logo" in svg_classes.lower()
            or "logo" in parent_classes.lower()
            or "logo" in aria_label.lower()
            or svg.get("role", "").lower() == "img"
        ):
            svg_candidates.append(svg)

    if not svg_candidates:
        # take the first svg on the page as a last resort
        all_svgs = soup.find_all("svg")
        if all_svgs:
            svg_candidates.append(all_svgs[0])

    if svg_candidates and domain:
        rebuilt = _resolve_svg_use(svg_candidates[0]) or str(svg_candidates[0])
        saved = _save_inline_svg(rebuilt, domain, "_logo.svg")
        if saved:
            print(f"[✅] Saved inline SVG logo")
            return saved

    # ── 8) Favicon fallback ──────────────────────────────────────────────────
    icon = soup.find("link", rel=lambda r: r and "icon" in r.lower())
    if icon and icon.get("href"):
        return urljoin(base_url, icon["href"])

    return None





# --- Download and save image locally ---
def download_and_save_image(url, domain, session=session):
    if isinstance(url, str) and url.startswith("local_file:"):
        return url.split("local_file:", 1)[1]

        # handle data URI SVG directly (in case one slips through)
    if isinstance(url, str) and url.startswith("data:image/svg+xml"):
        if url.startswith("data:image/svg+xml;base64,"):
            encoded = url.split(",", 1)[1]
            svg_decoded = base64.b64decode(encoded).decode("utf-8", errors="ignore")
        else:
            svg_encoded = url.split(",", 1)[1]
            svg_decoded = unquote(svg_encoded)
        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(svg_decoded)
        print(f"[✅] Saved data URI SVG logo to {filename}")
        return filename
    try:
        headers_image = headers.copy()
        headers_image.pop("Accept-Encoding", None)

        resp = session.get(url, headers=headers_image, timeout=10)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()
        ext = None
        if "png" in content_type:
            ext = "png"
        elif "jpeg" in content_type or "jpg" in content_type:
            ext = "jpg"
        elif "svg" in content_type:
            ext = "svg"
        else:
            if url.lower().endswith(".png"):
                ext = "png"
            elif url.lower().endswith(".jpg") or url.lower().endswith(".jpeg"):
                ext = "jpg"
            elif url.lower().endswith(".svg"):
                ext = "svg"
            else:
                ext = "png"

        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.{ext}")

        if "image" not in content_type and ext != "svg":
            print(f"⚠️ Content at {url} is not an image (Content-Type: {content_type}). Skipping download.")
            return None

        if ext == "svg":
            # accept only if content-type is svg OR body actually starts with <svg
            text_head = resp.content[:1024].lstrip()
            looks_svg = text_head.startswith(b"<svg") or text_head.startswith(b"<?xml")
            if "image/svg" not in content_type and not looks_svg:
                print(f"⚠️ URL claims .svg but response is not SVG (Content-Type: {content_type}).")
                return None  # let caller fall back to selenium/screenshot
            with open(filename, "wb") as f:
                f.write(resp.content)
            return filename

        else:
            try:
                img = Image.open(io.BytesIO(resp.content))
                img.verify()

                img = Image.open(io.BytesIO(resp.content))
                if ext != "png":
                    filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.png")
                    img.save(filename, format="PNG")
                else:
                    img.save(filename)

                print(f"[✅] Image downloaded and saved: {filename}")
            except Exception as e:
                print(f"❌ PIL cannot open/verify image from {url}: {e}")
                with open(filename, "wb") as f:
                    f.write(resp.content)
                return filename

        return filename

    except Exception as e:
        print(f"[❌] Failed to download/save image from {url}: {e}")
        return None

# --- Selenium fallback to find logo URL ---
def fetch_logo_with_selenium(domain):
    print(f"🌐 Selenium fallback for: {domain}")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={headers['User-Agent']}")

    driver = make_driver(options)
    driver.set_page_load_timeout(20)

    try:
        try:
            driver.get(f"https://{domain}")
        except Exception:
            driver.get(f"http://{domain}")

        time.sleep(2)
        imgs = driver.find_elements(By.TAG_NAME, "img")
        for img in imgs:
            src = img.get_attribute("src") or ""
            class_str = img.get_attribute("class") or ""
            if "logo" in src.lower() or "logo" in class_str.lower():
                return src
    except Exception as e:
        return f"Selenium error: {e}"
    finally:
        driver.quit()
        SELENIUM_SEMAPHORE.release()

# --- Selenium fallback to capture inline SVG as PNG screenshot ---
def fetch_svg_logo_as_png(domain, output_png="logo.png"):
    print(f"🎨 Attempting SVG PNG fallback for {domain}")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={headers['User-Agent']}")

    driver = make_driver(options)
    driver.set_page_load_timeout(20)

    try:
        try:
            driver.get(f"https://{domain}")
        except Exception:
            driver.get(f"http://{domain}")

        driver.implicitly_wait(5)

        svg_selectors = [
            'svg[data-testid="zalando-logo"]',
            'svg.logo',
            'svg'
        ]

        svg_element = None
        for selector in svg_selectors:
            try:
                svg_element = driver.find_element(By.CSS_SELECTOR, selector)
                if svg_element:
                    break
            except Exception:
                continue

        if not svg_element:
            print("[⚠️] No SVG element found for PNG fallback.")
            return None

        output_path = os.path.join(LOGO_DIR, output_png)
        png_data = svg_element.screenshot_as_png
        with open(output_path, "wb") as f:
            f.write(png_data)

        print(f"[✅] SVG screenshot saved as {output_path}")
        return output_path

    except Exception as e:
        print(f"[❌] Error in SVG PNG fallback for {domain}: {e}")
        return None
    finally:
        driver.quit()
        SELENIUM_SEMAPHORE.release()

def capture_logo_element_screenshot(domain, output_png=None):
    print(f"📸 Capturing logo element screenshot for {domain}")
    if not output_png:
        output_png = f"{domain.replace('.', '_')}_logo_element.png"
    output_path = os.path.join(LOGO_DIR, output_png)

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=2560,1440")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--force-device-scale-factor=2")
    options.add_argument(f"user-agent={headers['User-Agent']}")

    driver = make_driver(options)
    driver.set_page_load_timeout(20)

    try:
        try:
            driver.get(f"https://{domain}")
        except Exception:
            driver.get(f"http://{domain}")

        time.sleep(3)
        logo_element = None

        selectors = [
            'img[class*="logo"]',
            'img[src*="logo"]',
            'svg[class*="logo"]',
            'svg'
        ]

        for selector in selectors:
            try:
                logo_element = driver.find_element(By.CSS_SELECTOR, selector)
                if logo_element:
                    break
            except Exception:
                continue

        if not logo_element:
            print("[⚠️] No identifiable logo element found.")
            return None

        logo_element.screenshot(output_path)
        print(f"[📸] Logo element screenshot saved to {output_path}")
        return output_path

    except Exception as e:
        print(f"[❌] Logo element screenshot failed for {domain}: {e}")
        return None

    finally:
        driver.quit()
        SELENIUM_SEMAPHORE.release()

# --- Process a single domain ---
def process_domain(domain, driver=None):
    """
    If driver is None -> run your original stable flow (unchanged).
    If driver is provided (UC pass) -> ONLY try rendered HTML extraction with that driver,
    without raster/screenshot fallbacks (to avoid duplicates and keep risk low).
    """
    print(f"🔍 Processing: {domain}")
    # --- UC MODE (second pass only): use provided driver, no raster fallbacks ---
    if driver is not None:
        try:
            url = f"https://{domain}"
            print(f"[UC] Fetching DOM for: {domain}")
            try:
                driver.get(url)
            except Exception:
                driver.get(f"http://{domain}")
            WebDriverWait(driver, 8).until(
                lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
            )
            page_source = driver.page_source
            base_url = driver.current_url
            soup = safe_soup(page_source.replace("\x00", " "))
            picked = extract_logo_from_soup(soup, base_url, domain)
            if picked:
                if isinstance(picked, str) and picked.startswith("http"):
                    lf = download_and_save_image(picked, domain)
                    if lf:
                        picked = f"local_file:{lf}"
                print(f"➡️ Final logo_url for {domain} [UC]: {picked or ''}")
                return {"domain": domain, "logo_url": picked or ""}
        except Exception as e:
            print(f"[UC] failed for {domain}: {e}")
        # UC pass does not do raster fallbacks
        return {"domain": domain, "logo_url": ""}

    # --- ORIGINAL STABLE FLOW (unchanged) ---
    url = f"https://{domain}"
    logo_url = None

    try:
        resp = session.get(url, headers=headers, timeout=10)
        if resp.status_code == 503:
            print(f"⚠️ Skipping {domain}: 503 Service Unavailable")
            return {'domain': domain, 'logo_url': '503 Service Unavailable'}

        resp.raise_for_status()
        soup = safe_soup(resp.text)

        logo_url = extract_logo_from_soup(soup, url, domain)
        # If extractor already saved a local file, accept it and stop.
        if isinstance(logo_url, str) and logo_url.startswith("local_file:"):
            print(f"➡️ Final logo_url for {domain}: {logo_url}")
            return {"domain": domain, "logo_url": logo_url}



        if logo_url:
            local_file = download_and_save_image(logo_url, domain)
            if local_file:
                logo_url = f"local_file:{local_file}"
            else:
                logo_url = ""

        if not logo_url:
            selenium_logo_url = fetch_logo_with_selenium(domain)
            if selenium_logo_url and selenium_logo_url.startswith("http"):
                local_file = download_and_save_image(selenium_logo_url, domain)
                if local_file:
                    logo_url = f"local_file:{local_file}"
                else:
                    logo_url = ""
            else:
                logo_url = selenium_logo_url

        if not logo_url:
            output_png = f"{domain.replace('.', '_')}_logo.png"
            png_path = fetch_svg_logo_as_png(domain, output_png)
            if png_path:
                logo_url = f"local_file:{png_path}"
            else:
                screenshot_path = capture_logo_element_screenshot(domain)
                if screenshot_path:
                    logo_url = f"local_file:{screenshot_path}"
                else:
                    logo_url = ""

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error for {domain}: {e}")
        logo_url = ""

        selenium_logo_url = fetch_logo_with_selenium(domain)
        if selenium_logo_url and selenium_logo_url.startswith("http"):
            local_file = download_and_save_image(selenium_logo_url, domain)
            if local_file:
                logo_url = f"local_file:{local_file}"

        if not logo_url:
            output_png = f"{domain.replace('.', '_')}_logo.png"
            png_path = fetch_svg_logo_as_png(domain, output_png)
            if png_path:
                logo_url = f"local_file:{png_path}"

        if not logo_url:
            screenshot_path = capture_logo_element_screenshot(domain)
            if screenshot_path:
                logo_url = f"local_file:{screenshot_path}"

    print(f"➡️ Final logo_url for {domain}: {logo_url}")
    return {
        'domain': domain,
        'logo_url': logo_url or ''
    }

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    df = pd.read_parquet("logos.snappy.parquet")
    col = df.columns[0]
    #df = df[df[col].str.contains("kalyan", case=False, na=False)]
    blacklist = {"cafelasmargaritas.es","berlitz.com.py"}
    values = ( #drops duplicates
        df[col]
        .dropna()
        .astype("string")  # avoid using the built-in str
        .str.strip()
        .str.replace(r"^https?://", "", regex=True)
        .str.replace(r"^www\.", "", regex=True)
    )
    values = values[~values.isin(blacklist)].drop_duplicates(ignore_index=True)
    domains = values.head(100).tolist()

    # domains = list(dict.fromkeys([
    #     "worldvision.cl",
    #     "vans.es",
    #     "vans.com.ar",
    #     "toyota-bornholm.dk",
    #     "sto-ukraine.com",
    #     "sto-sea.com",
    #     "sto.hr",
    #     "tevamexico.com.mx",
    #     "stanbicbank.co.zm",
    #     "spitex-flawil-degersheim.ch",
    #     "schaumann.it",
    #     "sans-sans.com.my",
    #     "renault-argenteuil.fr",
    #     "renaultsprings.co.za",
    #     "pingusenglish.ps",
    #     "medef-essonne.org",
    #     "linexofwilliston.com",
    #     "kia-asc.ru",
    #     "kia-chita.ru",
    #     "kia-crimea.ru",
    #     "kia-kmv.ru",
    #     "intersport.be",
    #     "fordmediacenter.nl",
    #     "fmlogistic.cz",
    #     "fmlogistic.com.ua",
    #     "fmlogistic.es",
    #     "fmlogistic.ro",
    #     "fmlogistic.sk",
    #     "fmlogistic.pl",
    #     "dulux.co.zw",
    #     "decathlon.ro",
    #     "cvjm-muensingen.de",
    #     "crocssa.co.za",
    #     "bowlero.mx",
    #     "axa.dz",
    #     "atalian.com.tr",
    #     "amway.at",
    #     "allianzlifechanger.com",
    #     "aamcovinelandnj.com",
    #     "demetercs.eu",
    #     "dysartsservicecenter.com",
    #     "toyota-bauer.at",
    #     "atalian.hu",
    #     "toysrus.com.ph",
    #     "repsol.in.ua",
    #     "atalian.pl",
    #     "bosch-industry-consulting.com",
    #     "jungheinrich.com.sg",
    #     "mcdonalds.md",
    #     "bioderma-sk.com",
    #     "bonprix.nl",
    #     "crocs.ch",
    #     "enterprise.dk",
    #     "daikin.com.vn",
    #     "nobleprog.ro",
    #     "nobleprog.co.th",
    #     "despec.eu",
    #     "medef-bearnetsoule.com",
    #     "zktecoma.com",
    #     "chip.pl",
    #     "sephora.pl",
    #     "medef-artois.fr",
    #     "tbwa.co.za",
    #     "deheus.com",
    #     "worldvision.ca",
    #     "berlitz-augsburg.de",
    #     "wurth.co",
    #     "linexofschererville.com",
    # ]))
    results = []

    # Pass 1: your original threaded flow, unchanged
    with ThreadPoolExecutor(max_workers=20) as pool:
        for result in pool.map(process_domain, domains):
            site = result["domain"]
            logo = result["logo_url"]
            if logo:
                if logo.startswith("http"):
                    lf = download_and_save_image(logo, site)
                    logo = f"local_file:{lf}" if lf else ""
            results.append((site, logo))
            print(f"{site} => {logo}")

    # Collect failures
    failed = [site for site, logo in results if not logo]

    # Pass 2: single UC window, sequential, HTML extraction only
    if failed:
        uc_driver = create_uc_driver()
        if uc_driver:
            try:
                for site in failed:
                    res = process_domain(site, driver=uc_driver)
                    logo = res.get("logo_url") or ""
                    if isinstance(logo, str) and logo.startswith("http"):
                        lf = download_and_save_image(logo, site)
                        logo = f"local_file:{lf}" if lf else ""
                    if logo:
                        # update the original results
                        for i, (d, old_logo) in enumerate(results):
                            if d == site and not old_logo:
                                results[i] = (d, logo)
                                print(f"[UC✅] {d} => {logo}")
                                break
            finally:
                try:
                    uc_driver.quit()
                except Exception:
                    pass

    out = pd.DataFrame(results, columns=["website", "logo_url"])
    out.to_csv("logo_results.csv", index=False)

    found  = sum(1 for _, logo in results if logo)
    total  = len(results)
    print(f"\n✅ Found logos for {found}/{total} sites "
          f"({found/total*100:.2f}%)")

if __name__ == "__main__":
    main()