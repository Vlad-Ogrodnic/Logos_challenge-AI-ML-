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
LOGO_DIR = "logos_5"
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









# --- Brand-specific helpers --------------------------------------------------------------
def fetch_airbnb_logo(domain: str) -> str | None:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"user-agent={headers['User-Agent']}")
    opts.add_argument("--window-size=1920,1080")

    driver = make_driver(opts)
    url = domain if domain.startswith(("http://", "https://")) else f"https://{domain}"

    try:
        driver.set_page_load_timeout(15)
        driver.get(url)

        wait = WebDriverWait(driver, 10)

        try:
            anchor = wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//a[contains(translate(@aria-label,"
                        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),"
                        "'airbnb')]"
                    )
                )
            )
        except Exception:
            anchor = driver.find_element(By.CSS_SELECTOR, "a[href='/']")

        svg = anchor.find_element(By.TAG_NAME, "svg")
        svg_markup = svg.get_attribute("outerHTML")

    except Exception:
        svg_markup = None

    finally:
        driver.quit()
        SELENIUM_SEMAPHORE.release()

    if not svg_markup:
        return None

    path = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
    with open(path, "w", encoding="utf-8") as f:
        f.write(svg_markup)

    return f"local_file:{path}"

import re


def fetch_atalian_logo(domain: str) -> str | None:
    url = domain if domain.startswith(("http://","https://")) else f"https://{domain}"
    try:
        r = session.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = safe_soup(r.text)

        img = soup.select_one("header img.attachment-large.size-large")
        if img and img.get("src"):
            return urljoin(r.url, img["src"])

        logo_anchor = soup.find("a", class_="header_logo")
        if logo_anchor:
            svg_el = logo_anchor.find("svg")
            if svg_el:
                svg_markup = str(svg_el)
                fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + ".svg"
                path = os.path.join(LOGO_DIR, fname)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(svg_markup)
                return f"local_file:{path}"

        if domain.lower().endswith(".nl"):
            svg_el = soup.find("svg")
            if svg_el:
                svg_markup = str(svg_el)
                fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + ".svg"
                path = os.path.join(LOGO_DIR, fname)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(svg_markup)
                return f"local_file:{path}"

    except Exception as e:
        print(f"[⚠️ Atalian] failed to fetch logo for {domain}: {e}")

    return None

# --- Your original extractor (unchanged) ---
def extract_logo_from_soup(soup, base_url, domain=None):



    def get_best_img_url(img_tag, base_url, domain=None):
        # unchanged: handles data: SVGs, ShortPixel unwrap, etc.
        for attr in ['data-src', 'data-lazy-src', 'src']:
            url = img_tag.get(attr)
            if url:
                if url.startswith("data:image/svg+xml"):
                    if domain:
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
                        return f"local_file:{filename}"
                    return url

                if "shortpixel.ai" in url:
                    match = re.search(r"https?://[^/]+/(https?://.+)", url)
                    if match:
                        url = match.group(1)

                return urljoin(base_url, url)
        return None
    # ── Special-case: inline SVG for Berlitz-Augsburg (kept) ───────────
    if domain and domain.lower().endswith("berlitz-augsburg.de"):
        svg_el = soup.select_one("a[class*='logo__LogoLink'] svg")
        if svg_el:
            svg_markup = str(svg_el)
            fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + "_logo.svg"
            path = os.path.join(LOGO_DIR, fname)
            with open(path, "w", encoding="utf-8") as f:
                f.write(svg_markup)
            print(f"[✅ Berlitz-Augsburg] Saved inline SVG logo to {path}")
            return f"local_file:{path}"
        # ── Special-case: FM Logistic — prefer header logo only; avoid OG/Twitter ──
    if domain and "fmlogistic" in domain.lower():
        # Look for the brand anchor in the header
        a = soup.select_one("header a.brand, a.brand")
        if a:
            # Prefer an <img> inside the anchor
            img = a.find("img")
            if img:
                u = get_best_img_url(img, base_url, domain)
                if u:
                    return u

            # Else, take the inline SVG used for the logo
            svg = (a.select_one("svg.brand--color")
                   or a.select_one("svg.brand--white")
                   or a.find("svg"))
            if svg:
                svg_markup = str(svg)
                filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(svg_markup)
                print(f"[✅ FM Logistic] Saved inline SVG logo to {filename}")
                return f"local_file:{filename}"
    # ───────────────────────────────────────────────────────────────────
    # NEW #4: Header/root-link heuristic (look in header/nav/#logo)
    # ───────────────────────────────────────────────────────────────────
    if domain:
        header_candidates = soup.select("header, #header, .site-header, nav, #logo")
        for container in header_candidates:
            a = container.find("a", href=True)
            if a and _is_root_link(a.get("href"), base_url):
                el = a.find(["img", "svg"]) or a
                # Try <img> first
                img = el if el.name == "img" else el.find("img")
                if img:
                    url = get_best_img_url(img, base_url, domain)
                    if url:
                        return url
                # Or inline SVG
                svg = el if el.name == "svg" else el.find("svg")
                if svg:
                    # NEW #3: resolve <use> sprites if present
                    local = _resolve_svg_use(svg, soup, base_url, domain)
                    if local:
                        return local
                    return _save_svg_markup(str(svg), domain, suffix="_logo")

    # ───────────────────────────────────────────────────────────────────
    # NEW #5: Containers named like brandLogo
    # ───────────────────────────────────────────────────────────────────
    brand_cont = soup.find(lambda t: t.name in ("div", "a", "span", "figure", "header")
                           and "class" in t.attrs
                           and any(s in " ".join(t.get("class", [])).lower()
                                   for s in ("brandlogo", "brand-logo", "brand_logo")))
    if brand_cont:
        img = brand_cont.find("img")
        if img:
            url = get_best_img_url(img, base_url, domain)
            if url:
                return url
        svg = brand_cont.find("svg")
        if svg:
            local = _resolve_svg_use(svg, soup, base_url, domain)
            if local:
                return local
            return _save_svg_markup(str(svg), domain, suffix="_logo")
        # style-based?
        style_url = _extract_url_from_style(brand_cont.get("style", ""), base_url)
        if style_url:
            return style_url

    # 1) <a class="logo"> / aria-label ~logo → <img>
    anchors = soup.find_all("a", class_=lambda c: c and "logo" in c.lower())
    for a in anchors:
        img = a.find("img")
        if img:
            img_url = get_best_img_url(img, base_url, domain)
            if img_url:
                return img_url

    anchors_aria = soup.find_all("a", attrs={"aria-label": lambda v: v and "logo" in v.lower()})
    for a in anchors_aria:
        img = a.find("img")
        if img:
            img_url = get_best_img_url(img, base_url, domain)
            if img_url:
                return img_url

    # NEW #2: treat <ion-img> and generic <i> with logo classes as candidates
    ion = soup.find("ion-img", src=True)
    if ion:
        return urljoin(base_url, ion.get("src"))

    # 2) <img> with "logo" signal (extend: alt includes logo)
    for img in soup.find_all("img"):
        class_str = " ".join(img.get("class", []))
        alt_str = (img.get("alt") or "")
        img_url = get_best_img_url(img, base_url, domain)
        if img_url and (
            "logo" in (img_url or "").lower() or
            "logo" in class_str.lower() or
            "logo" in alt_str.lower() or
            "template_photo" in (img_url or "").lower()
        ):
            print(f"[🔍] Logo candidate found: {img_url}")
            return img_url

    # NEW #1 & #2: any element with logo-ish class and a style url() (background or mask)
    for tag in soup.find_all(["div", "span", "i", "a"]):
        class_str = " ".join(tag.get("class", [])).lower()
        if any(k in class_str for k in ("logo", "brandlogo", "brand-logo", "brand_logo")):
            style_url = _extract_url_from_style(tag.get("style", ""), base_url)
            if style_url:
                return style_url

    # 3) <div>/<span> with background-image in inline style (kept)
    for tag in soup.find_all(["div", "span"]):
        class_str = " ".join(tag.get("class", []))
        if "logo" in class_str.lower() or "brand" in class_str.lower():
            style = tag.get("style", "")
            m = re.search(r'url\(([^)]+)\)', style)
            if m:
                return urljoin(base_url, m.group(1).strip("'\""))

    # 4) Meta tags (unchanged; keep after DOM heuristics)
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return urljoin(base_url, og["content"])
    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return urljoin(base_url, tw["content"])

    # 5) Favicon fallback (unchanged)
    icon = soup.find("link", rel=lambda r: r and "icon" in r.lower())
    icon_url = urljoin(base_url, icon["href"]) if icon and icon.get("href") else None

    # 6) CSS files: extend to search mask(-image) too
    try:
        css_links = [link.get("href") for link in soup.find_all("link", rel="stylesheet")]
        css_links = [urljoin(base_url, href) for href in css_links if href]

        for css_url in css_links:
            try:
                css_resp = session.get(css_url, headers=headers, timeout=10)
                if css_resp.status_code == 200:
                    css_text = css_resp.text

                    # existing background-image scan
                    pattern = r'\.(logo|brand)[^{]*\{[^}]*background(?:-image)?:\s*url\(([^)]+)\)'
                    matches = re.findall(pattern, css_text, re.IGNORECASE)
                    for _, url in matches:
                        return urljoin(css_url, url.strip("'\""))

                    generic_match = re.findall(r'background(?:-image)?:\s*url\(([^)]+)\)', css_text)
                    for bg_url in generic_match:
                        if "logo" in bg_url.lower() or "brand" in bg_url.lower():
                            return urljoin(css_url, bg_url.strip("'\""))

                    # NEW: mask(-image) scan
                    mask_matches = re.findall(r'(?:-webkit-)?mask(?:-image)?:\s*url\(([^)]+)\)', css_text, re.IGNORECASE)
                    for mk in mask_matches:
                        if "logo" in mk.lower() or "brand" in mk.lower():
                            return urljoin(css_url, mk.strip("'\""))
            except Exception as e:
                print(f"[⚠️] CSS fetch failed: {css_url} - {e}")
    except Exception as e:
        print(f"[⚠️] CSS extraction error: {e}")

    # 7) Inline <svg> (extend: resolve <use> sprite)
    svg_candidates = []
    for svg in soup.find_all("svg"):
        svg_classes = " ".join(svg.get("class", []))
        parent_classes = " ".join(svg.parent.get("class", [])) if svg.parent else ""
        aria_label = svg.get("aria-label", "") or ""
        role = svg.get("role", "") or ""

        if (
            "logo" in svg_classes.lower() or
            "logo" in parent_classes.lower() or
            "logo" in aria_label.lower() or
            role.lower() == "img"
        ):
            svg_candidates.append(svg)

    if not svg_candidates:
        all_svgs = soup.find_all("svg")
        if all_svgs:
            svg_candidates.append(all_svgs[0])

    if svg_candidates and domain:
        # NEW #3: try to inline <use> first
        local = _resolve_svg_use(svg_candidates[0], soup, base_url, domain)
        if local:
            return local
        svg_str = str(svg_candidates[0])
        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(svg_str)
        print(f"[✅] Saved inline SVG logo to {filename}")
        return f"local_file:{filename}"

    # 8) Fallback to favicon (unchanged)
    return icon_url


# --- Download and save image locally ---
def download_and_save_image(url, domain, session=session):
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
            with open(filename, "wb") as f:
                f.write(resp.content)
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

    # brand-specific shortcuts – keep them in both modes
    if "airbnb" in domain.lower():
        logo_url = fetch_airbnb_logo(domain) or ""
        print(f"➡️ Final logo_url for {domain}: {logo_url}")
        return {"domain": domain, "logo_url": logo_url}

    if "atalian" in domain.lower():
        logo_url = fetch_atalian_logo(domain) or ""
        print(f"➡️ Final logo_url for {domain}: {logo_url}")
        return {"domain": domain, "logo_url": logo_url}
    # ── ibc-solar.jp: asset route blocks direct SVG fetch → screenshot the <img class="logo">
    if "ibc-solar.jp" in domain.lower():
        shot = capture_logo_element_screenshot(domain, f"{domain.replace('.', '_')}_logo.png")
        if shot:
            print(f"[📸 ibc-solar.jp] saved logo screenshot -> {shot}")
            return {"domain": domain, "logo_url": f"local_file:{shot}"}
        # fall through if something odd happens

    if "berlitz-augsburg.de" in domain.lower():
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument(f"user-agent={headers['User-Agent']}")
        drv = make_driver(opts)
        try:
            drv.set_page_load_timeout(15)
            drv.get(f"https://{domain}")
            svg = drv.find_element(
                By.CSS_SELECTOR,
                "a.logo__LogoLink-sc-14ehgdt-0 svg"
            )
            svg_markup = svg.get_attribute("outerHTML")
        finally:
            drv.quit()
            SELENIUM_SEMAPHORE.release()
        if svg_markup:
            fname = re.sub(r"[^A-Za-z0-9_-]", "_", domain) + "_logo.svg"
            path = os.path.join(LOGO_DIR, fname)
            with open(path, "w", encoding="utf-8") as f:
                f.write(svg_markup)
            print(f"[✅ Berlitz-Augsburg] Saved inline SVG logo to {path}")
            return {"domain": domain, "logo_url": f"local_file:{path}"}

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
    df = df[df[col].str.contains("kalyan", case=False, na=False)]
    blacklist = {"cafelasmargaritas.es","berlitz.com.py"}
    domains = [
        s for s in df[col].dropna().astype(str).tolist()
        if s not in blacklist
    ][:100]
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