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
from bs4 import ParserRejectedMarkup, FeatureNotFound   # ← add
from selenium.webdriver.support import expected_conditions as EC
import threading

DRIVER_PATH = ChromeDriverManager().install()   # one download, reused
SELENIUM_SEMAPHORE = threading.Semaphore(2)     # never spawn >2 browsers
#useful in the airbnb function since otherwise it hangs, and this way it manges to get 29/29 logos

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

    Any ParserRejectedMarkup / AssertionError will automatically
    fall through to the next parser.
    """
    for parser in ("lxml", "html.parser", "html5lib"):
        try:
            return BeautifulSoup(markup, parser)
        except (ParserRejectedMarkup, FeatureNotFound, AssertionError):
            continue
    # If every parser fails, re-raise the last exception
    raise ParserRejectedMarkup("All parsers rejected the markup")








# --- Ensure logos folder exists ---
LOGO_DIR = "logos_stable91"
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


# --- Airbnb-specific Selenium SVG grab (from Code 1) -------------------------
def fetch_airbnb_logo(domain: str) -> str | None:
    """
    Return local_file:<path> for the SVG logo on any *.airbnb.* site.
    Uses one shared chromedriver binary and never spawns more than
    two concurrent browsers (handled by make_driver / semaphore).
    """
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument(f"user-agent={headers['User-Agent']}")
    opts.add_argument("--window-size=1920,1080")

    driver = make_driver(opts)                     # ← shared driver helper
    url = domain if domain.startswith(("http://", "https://")) else f"https://{domain}"

    try:
        driver.set_page_load_timeout(15)           # hard browser timeout
        driver.get(url)

        wait = WebDriverWait(driver, 10)

        try:                                       # primary selector
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
            # fallback selector
            anchor = driver.find_element(By.CSS_SELECTOR, "a[href='/']")

        svg = anchor.find_element(By.TAG_NAME, "svg")
        svg_markup = svg.get_attribute("outerHTML")

    except Exception:
        svg_markup = None

    finally:
        driver.quit()
        SELENIUM_SEMAPHORE.release()               # ← must release!

    if not svg_markup:
        return None

    # save SVG locally
    path = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
    with open(path, "w", encoding="utf-8") as f:
        f.write(svg_markup)

    return f"local_file:{path}"

import re

def fetch_atalian_logo(domain: str) -> str | None:
    """
    Grab the Atalian logo for any *.atalian.* domain:
      1) the <img class="attachment-large size-large"> inside the header
      2) the inline <svg> inside <a class="header_logo">
      3) FALLBACK for .nl: take the very first <svg> on the page
    """
    url = domain if domain.startswith(("http://","https://")) else f"https://{domain}"
    try:
        r = session.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = safe_soup(r.text)

        # 1) PNG logo (other locales)
        img = soup.select_one("header img.attachment-large.size-large")
        if img and img.get("src"):
            return urljoin(r.url, img["src"])

        # 2) Inline SVG in the header_logo link
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

        # 3) NL special-case: grab the very first <svg> on the page
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



























def extract_logo_from_soup(soup, base_url, domain=None):
    # ── Special-case: inline SVG for Berlitz-Augsburg ────────────────
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
    def get_best_img_url(img_tag, base_url, domain=None):
        for attr in ['data-src', 'data-lazy-src', 'src']:
            url = img_tag.get(attr)
            if url:
                # --- Handle inline data URI SVG (plain or base64) ---
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
                    return url  # return data URI if no domain to save

                # Unwrap ShortPixel or similar CDN wrappers
                if "shortpixel.ai" in url:
                    match = re.search(r"https?://[^/]+/(https?://.+)", url)
                    if match:
                        url = match.group(1)

                return urljoin(base_url, url)
        return None

    # 1) <a class="logo"> or <a aria-label="...logo..."> → <img>
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

    # 2) <img> with "logo" in src, class, or filename
    for img in soup.find_all("img"):
        class_str = " ".join(img.get("class", []))
        img_url = get_best_img_url(img, base_url, domain)
        if img_url and (
            "logo" in (img_url or "").lower() or
            "logo" in class_str.lower() or
            "template_photo" in (img_url or "").lower()
        ):
            print(f"[🔍] Logo candidate found: {img_url}")
            return img_url

    # 3) <div>/<span> with background-image in inline style
    for tag in soup.find_all(["div", "span"]):
        class_str = " ".join(tag.get("class", []))
        if "logo" in class_str.lower() or "brand" in class_str.lower():
            style = tag.get("style", "")
            m = re.search(r'url\(([^)]+)\)', style)
            if m:
                return urljoin(base_url, m.group(1).strip("'\""))

    # 4) Meta tags: Open Graph, Twitter
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return urljoin(base_url, og["content"])

    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return urljoin(base_url, tw["content"])

    # 5) Favicon fallback (prepare it early for final fallback)
    icon = soup.find("link", rel=lambda r: r and "icon" in r.lower())
    icon_url = urljoin(base_url, icon["href"]) if icon and icon.get("href") else None

    # 6) Check CSS stylesheets for `.logo` or `.brand` background-image
    try:
        css_links = [link.get("href") for link in soup.find_all("link", rel="stylesheet")]
        css_links = [urljoin(base_url, href) for href in css_links if href]

        for css_url in css_links:
            try:
                css_resp = session.get(css_url, headers=headers, timeout=10)
                if css_resp.status_code == 200:
                    css_text = css_resp.text

                    pattern = r'\.(logo|brand)[^{]*\{[^}]*background(?:-image)?:\s*url\(([^)]+)\)'
                    matches = re.findall(pattern, css_text, re.IGNORECASE)
                    for _, url in matches:
                        return urljoin(css_url, url.strip("'\""))

                    generic_match = re.findall(r'background(?:-image)?:\s*url\(([^)]+)\)', css_text)
                    for bg_url in generic_match:
                        if "logo" in bg_url.lower() or "brand" in bg_url.lower():
                            return urljoin(css_url, bg_url.strip("'\""))
            except Exception as e:
                print(f"[⚠️] CSS fetch failed: {css_url} - {e}")
    except Exception as e:
        print(f"[⚠️] CSS extraction error: {e}")

    # 7) Inline <svg> elements as logo candidates
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
        svg_str = str(svg_candidates[0])
        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
        with open(filename, "w", encoding="utf-8") as f:
            f.write(svg_str)
        print(f"[✅] Saved inline SVG logo to {filename}")
        return f"local_file:{filename}"

    # 8) Fallback to favicon
    return icon_url





# --- Download and save image locally ---
def download_and_save_image(url, domain, session=session):
    try:
        headers_image = headers.copy()
        headers_image.pop("Accept-Encoding", None)  # avoid compressed content issues

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
                img.verify()  # verify image

                img = Image.open(io.BytesIO(resp.content))  # reopen to save
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

    return None


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

        # Try to locate common logo candidates
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
def process_domain(domain):
    print(f"🔍 Processing: {domain}")

    # ── Airbnb gets its own Selenium SVG path with a 25-s hard timeout ──
    if "airbnb" in domain.lower():
        logo_url = fetch_airbnb_logo(domain) or ""
        print(f"➡️ Final logo_url for {domain}: {logo_url}")
        return {"domain": domain, "logo_url": logo_url}
    # ── Atalian gets its own direct extractor ──
    if "atalian" in domain.lower():
        logo_url = fetch_atalian_logo(domain) or ""
        print(f"➡️ Final logo_url for {domain}: {logo_url}")
        return {"domain": domain, "logo_url": logo_url}
    #berlitz
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
    # ── Non-Airbnb flow (unchanged) ──────────────────────────────────────
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
                # Final fallback: take full-page screenshot
                screenshot_path = capture_logo_element_screenshot(domain)
                if screenshot_path:
                    logo_url = f"local_file:{screenshot_path}"
                else:
                    logo_url = ""

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Request error for {domain}: {e}")
        logo_url = ""

        # Try selenium image grab
        selenium_logo_url = fetch_logo_with_selenium(domain)
        if selenium_logo_url and selenium_logo_url.startswith("http"):
            local_file = download_and_save_image(selenium_logo_url, domain)
            if local_file:
                logo_url = f"local_file:{local_file}"

        # Try SVG screenshot
        if not logo_url:
            output_png = f"{domain.replace('.', '_')}_logo.png"
            png_path = fetch_svg_logo_as_png(domain, output_png)
            if png_path:
                logo_url = f"local_file:{png_path}"

        # Final fallback: full-page screenshot
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
#  MAIN  – same structure as Code 1, but still calls Code 2’s process_domain()
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # Load the first column of the Parquet  and keep only the
    # first 100 rows for a quick run.
    df = pd.read_parquet("logos.snappy.parquet")
    col = df.columns[0]
    #df = df[df[col].str.contains("berlitz", case=False, na=False)]

    blacklist = {"cafelasmargaritas.es","berlitz.com.py"}  # ← new
    domains = [
                  s for s in df[col].dropna().astype(str).tolist()
                  if s not in blacklist  # ← skip blacklisted site
              ][:100]
    results = []

    # Exactly Code 1’s ThreadPool pattern (20 workers, executor.map keeps order)
    with ThreadPoolExecutor(max_workers=20) as pool:
        for result in pool.map(process_domain, domains):
            # process_domain() already returns a dict {"domain":…, "logo_url":…}
            site  = result["domain"]
            logo  = result["logo_url"]
            if logo:
                # Code 2’s download_and_save_image() handles local_file / URL
                if logo.startswith("http"):
                    download_and_save_image(logo, site)
            results.append((site, logo))
            print(f"{site} => {logo}")

    # Write CSV exactly like Code 1
    out = pd.DataFrame(results, columns=["website", "logo_url"])
    out.to_csv("logo_results.csv", index=False)

    found  = sum(1 for _, logo in results if logo)
    total  = len(results)
    print(f"\n✅ Found logos for {found}/{total} sites "
          f"({found/total*100:.2f}%)")

if __name__ == "__main__":
    main()