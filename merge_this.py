import os
import re
import time
import io
import pandas as pd
import requests
import base64
import html
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

# --- Ensure logos folder exists ---
LOGO_DIR = "logos"
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


def extract_logo_from_soup(soup, base_url, domain=None):
    def log(step, msg):
        print(f"[{domain}] {step} {msg}")

    def get_best_img_url(img_tag, base_url, domain=None):
        for attr in ['data-src', 'data-lazy-src', 'src', 'data-srcset']:
            url = img_tag.get(attr)
            if not url:
                continue

            # Handle data-srcset: pick largest
            if attr == "data-srcset":
                urls = [u.strip().split(" ")[0] for u in url.split(",") if u.strip()]
                if urls:
                    url = urls[-1]

            if url.startswith("//"):
                url = "https:" + url

            # Inline SVG (data URI)
            if url.startswith("data:image/svg+xml"):
                log("DATA-URI", f"Found inline SVG in {attr}")
                if domain:
                    if url.startswith("data:image/svg+xml;base64,"):
                        encoded = url.split(",", 1)[1]
                        svg_decoded = base64.b64decode(encoded).decode("utf-8", errors="ignore")
                    else:
                        svg_encoded = url.split(",", 1)[1]
                        svg_decoded = unquote(svg_encoded)
                    os.makedirs(LOGO_DIR, exist_ok=True)
                    filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
                    with open(filename, "w", encoding="utf-8") as f:
                        f.write(svg_decoded)
                    log("SAVE", f"Saved inline SVG to {filename}")
                    return f"local_file:{filename}"
                return url

            # Handle ShortPixel
            if "shortpixel.ai" in url:
                match = re.search(r"https?://[^/]+/(https?://.+)", url)
                if match:
                    url = match.group(1)
                    log("CDN", f"Unwrapped shortpixel URL → {url}")

            return urljoin(base_url, url)
        return None

    candidates = []

    # STEP 1 & 2: Anchors (.logo or aria-label)
    for step, selector, score in [
        ("STEP 1", lambda a: a.find("img"), 10),
        ("STEP 2", lambda a: a.find("img"), 8),
    ]:
        if step == "STEP 1":
            anchors = soup.find_all("a", class_=lambda c: c and "logo" in c.lower())
        else:
            anchors = soup.find_all("a", attrs={"aria-label": lambda v: v and "logo" in v.lower()})
        for a in anchors:
            img = selector(a)
            if img:
                url = get_best_img_url(img, base_url, domain)
                if url:
                    candidates.append((url, score))
                    log(step, f"Candidate → {url} (score {score})")
            # noscript fallback
            noscript = a.find("noscript")
            if noscript:
                img_tag = BeautifulSoup(noscript.decode_contents(), "html.parser").find("img")
                if img_tag:
                    url = get_best_img_url(img_tag, base_url, domain)
                    if url:
                        candidates.append((url, score))
                        log(step + " NOSCRIPT", f"Candidate → {url} (score {score})")

    # STEP 3: All img tags (includes template_photo from second version)
    for img in soup.find_all(["img", "ion-img"]):
        class_str = " ".join(img.get("class", []))
        parent_classes = " ".join(img.parent.get("class", [])) if img.parent else ""
        url = get_best_img_url(img, base_url, domain)
        if url:
            url_lower = url.lower()
            score = 2
            if any(x in url_lower for x in ["logo", "emblem", "brand", "template_photo"]):
                score = 6
            if img.get("title") and any(x in img["title"].lower() for x in ["logo", "emblem", "brand"]):
                score = max(score, 8)
            if img.parent.name == "a" and (img.parent.get("href") in ["/", base_url, f"https://{domain}/"]):
                score = max(score, 10)
            candidates.append((url, score))
            log("STEP 3", f"Candidate → {url} (score {score})")

    # STEP 4: Inline style background-image (.logo/.brand)
    for tag in soup.find_all(["div", "span"]):
        style = tag.get("style", "")
        m = re.search(r'url\(([^)]+)\)', style)
        if m:
            url = m.group(1).strip("'\"")
            if url.startswith("//"):
                url = "https:" + url
            url = urljoin(base_url, url)
            class_str = " ".join(tag.get("class", []))
            score = 5 if "logo" in class_str.lower() or "brand" in class_str.lower() else 2
            candidates.append((url, score))
            log("STEP 4", f"Candidate → {url} (score {score})")

    # STEP 5: Meta tags (og:image, twitter:image, etc.)
    for attr, name, score in [
        ("property", "og:image", 4),
        ("name", "twitter:image", 4),
        ("itemprop", "logo", 6),
        ("property", "og:logo", 6)
    ]:
        tag = soup.find("meta", attrs={attr: name})
        if tag and tag.get("content"):
            url = tag["content"]
            if url.startswith("//"):
                url = "https:" + url
            url = urljoin(base_url, url)
            candidates.append((url, score))
            log("STEP 5", f"Candidate → {url} (score {score})")

    # STEP 6: Favicon fallback
    icon = soup.find("link", rel=lambda r: r and "icon" in r.lower())
    if icon and icon.get("href"):
        url = icon["href"]
        if url.startswith("//"):
            url = "https:" + url
        url = urljoin(base_url, url)
        candidates.append((url, 1))
        log("STEP 6", f"Candidate → {url} (score 1)")

    # STEP 7: SVG candidates (extended + <use>/<symbol>)
    for svg in soup.find_all("svg"):
        svg_text = " ".join([
            svg.get("aria-label", ""),
            svg.get("role", ""),
            " ".join(svg.get("class", [])),
            (svg.title.string if svg.title else ""),
            (svg.desc.string if svg.desc else "")
        ])
        parent_classes = " ".join(svg.parent.get("class", [])) if svg.parent else ""

        if "logo" in svg_text.lower() or "logo" in parent_classes.lower():
            log("STEP 7", f"Candidate <svg> → {svg_text}")
            if domain:
                os.makedirs(LOGO_DIR, exist_ok=True)
                filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.svg")
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(str(svg))
                url = f"local_file:{filename}"
            else:
                url = str(svg)
            candidates.append((url, 10))

        # Check for <use> reference
        for u in svg.find_all('use', attrs={'xlink:href': True}):
            href = u['xlink:href']
            if 'logo' in href.lower():
                symbol_id = href.lstrip('#')
                symbol = soup.find('symbol', attrs={'id': symbol_id})
                if symbol:
                    svg_el = soup.new_tag('svg', xmlns="http://www.w3.org/2000/svg")
                    for attr in ['viewBox', 'width', 'height']:
                        if symbol.has_attr(attr):
                            svg_el[attr] = symbol[attr]
                    svg_el.append(BeautifulSoup(symbol.decode_contents(), "html.parser"))
                    if domain:
                        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_symbol_logo.svg")
                        with open(filename, "w", encoding="utf-8") as f:
                            f.write(str(svg_el))
                        url = f"local_file:{filename}"
                        candidates.append((url, 10))

    # STEP 8: CSS logo detection (from second version)
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
                        candidates.append((urljoin(css_url, url.strip("'\"")), 6))
                    generic_match = re.findall(r'background(?:-image)?:\s*url\(([^)]+)\)', css_text)
                    for bg_url in generic_match:
                        if "logo" in bg_url.lower() or "brand" in bg_url.lower():
                            candidates.append((urljoin(css_url, bg_url.strip("'\"")), 6))
            except Exception as e:
                log("CSS", f"Failed to fetch {css_url} → {e}")
    except Exception as e:
        log("CSS", f"Error extracting CSS → {e}")

    if not candidates:
        log("FALLBACK", "No candidates found")
        return None, 0

    # Pick best candidate
    best_url, best_score = max(candidates, key=lambda x: x[1])
    log("RESULT", f"Selected logo → {best_url} (score {best_score})")

    # Download if HTTP(S)
    if best_url.startswith("http"):
        os.makedirs(LOGO_DIR, exist_ok=True)
        ext = os.path.splitext(best_url)[1].split("?")[0] or ".png"
        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_') if domain else 'logo'}{ext}")
        try:
            resp = requests.get(best_url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                with open(filename, "wb") as f:
                    f.write(resp.content)
                log("DOWNLOAD", f"Logo downloaded → {filename}")
            else:
                log("DOWNLOAD", f"Failed (status {resp.status_code})")
        except Exception as e:
            log("DOWNLOAD", f"Error: {e}")

    return best_url, best_score


# --- Download and save image locally ---
def download_with_selenium(url, filename):
    """Fallback using Selenium if requests fails"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get(url)
        # Execute JS to get image as base64
        img_data = driver.execute_script("""
            const img = document.querySelector('img') || document.images[0];
            if(img){
                const canvas = document.createElement('canvas');
                canvas.width = img.naturalWidth;
                canvas.height = img.naturalHeight;
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0);
                return canvas.toDataURL('image/png').split(',')[1];
            }
            return null;
        """)
        if img_data:
            with open(filename, "wb") as f:
                f.write(base64.b64decode(img_data))
            print(f"[✅] Image downloaded via Selenium: {filename}")
            return filename
        else:
            print(f"[❌] Selenium could not extract image from {url}")
            return None
    finally:
        driver.quit()


def download_and_save_image(url, domain, session=requests.Session()):
    try:
        if url.startswith("local_file:"):
            return url.replace("local_file:", "")

        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()
        if "svg" in content_type or url.lower().endswith(".svg"):
            ext = "svg"
        elif "png" in content_type or url.lower().endswith(".png"):
            ext = "png"
        elif "jpeg" in content_type or "jpg" in content_type or url.lower().endswith((".jpg", ".jpeg")):
            ext = "jpg"
        else:
            ext = "png"

        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.{ext}")
        with open(filename, "wb") as f:
            f.write(resp.content)

        print(f"[✅] Image downloaded and saved: {filename}")
        return filename

    except Exception as e:
        print(f"[⚠️] Requests failed, falling back to Selenium for {url}: {e}")
        # fallback with Selenium
        ext = "png" if not url.lower().endswith((".png", ".jpg", ".jpeg", ".svg")) else url.split(".")[-1]
        filename = os.path.join(LOGO_DIR, f"{domain.replace('.', '_')}_logo.{ext}")
        return download_with_selenium(url, filename)


# --- Selenium fallback to find logo URL ---
def fetch_page_source_with_selenium(domain):
    """Return page source using Selenium (headless Chrome)."""
    print(f"🌐 Selenium page source for: {domain}")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-agent={headers['User-Agent']}")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(20)

    page_source = None
    try:
        try:
            driver.get(f"https://{domain}")
        except Exception:
            driver.get(f"http://{domain}")

        time.sleep(2)  # let JS load
        page_source = driver.page_source

    except Exception as e:
        print(f"⚠️ Selenium error for {domain}: {e}")
    finally:
        driver.quit()

    return page_source


# --- Selenium fallback to capture inline SVG as PNG screenshot ---
def fetch_svg_logo_as_png(domain, output_png="logo.png"):
    print(f"🎨 [START] Attempting SVG PNG fallback for {domain}")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={headers['User-Agent']}")

    print("[INFO] Chrome options set. Initializing WebDriver...")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(20)

    try:
        print(f"[INFO] Trying HTTPS for {domain}...")
        try:
            driver.get(f"https://{domain}")
            print("[INFO] HTTPS loaded successfully.")
        except Exception as e_https:
            print(f"[WARN] HTTPS failed: {e_https}. Trying HTTP...")
            driver.get(f"http://{domain}")
            print("[INFO] HTTP loaded successfully.")

        driver.implicitly_wait(5)
        print("[INFO] Implicit wait of 5 seconds done. Searching for SVG elements...")

        svg_selectors = [
            'svg[data-testid="zalando-logo"]',
            'svg.logo',
            'svg'
        ]

        svg_element = None
        for selector in svg_selectors:
            print(f"[INFO] Trying selector: '{selector}'")
            try:
                svg_element = driver.find_element(By.CSS_SELECTOR, selector)
                if svg_element:
                    print(f"[INFO] SVG found using selector: '{selector}'")
                    break
            except Exception as e_sel:
                print(f"[WARN] Selector '{selector}' did not match any element: {e_sel}")
                continue

        if not svg_element:
            print("[⚠️] No SVG element found for PNG fallback.")
            return None

        output_path = os.path.join(LOGO_DIR, output_png)
        print(f"[INFO] Capturing screenshot to save as {output_path}...")
        png_data = svg_element.screenshot_as_png
        with open(output_path, "wb") as f:
            f.write(png_data)

        print(f"[✅] SVG screenshot saved successfully as {output_path}")
        return output_path

    except Exception as e:
        print(f"[❌] Error in SVG PNG fallback for {domain}: {e}")
        return None
    finally:
        print("[INFO] Quitting WebDriver...")
        driver.quit()
        print("[INFO] WebDriver quit successfully.")


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

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
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


def find_logo(domain, page_source=None, driver=None, selenium=False):
    base_url = f"https://{domain}"
    logo_url = None

    # Extract logo from HTML
    if page_source:
        soup = BeautifulSoup(page_source, 'html.parser')
        logo_url, score = extract_logo_from_soup(soup, base_url, domain)
        # Check if we have a strong candidate
        if logo_url and score > 1:
            # If it's a remote URL, download it
            if logo_url.startswith("http"):
                local_file = download_and_save_image(logo_url, domain)
                if local_file:
                    return f"local_file:{local_file}"
                else:
                    print(f"[{domain}] WARNING Failed to download strong candidate logo")
                    return None
            # If it's already a local file
            elif logo_url.startswith("local_file:"):
                return logo_url
        else:
            print(f"[{domain}] FALLBACK: No strong logo candidates found (score {score})")
            if not selenium:
                return "FALLBACK No candidates"

    # SVG fallback
    output_png = f"{domain.replace('.', '_')}_logo.png"
    png_path = fetch_svg_logo_as_png(domain, output_png)
    if png_path:
        print(f"[{domain}] FALLBACK Found logo via SVG")
        return f"local_file:{png_path}"
    else:
        print(f"[{domain}] FALLBACK SVG not found")

    # Screenshot fallback
    screenshot_path = capture_logo_element_screenshot(domain)
    if screenshot_path:
        print(f"[{domain}] FALLBACK Found logo via screenshot")
        return f"local_file:{screenshot_path}"
    else:
        print(f"[{domain}] FALLBACK Screenshot not found")

    # Nothing found
    print(f"[{domain}] FALLBACK No logo found at all")
    return None


# --- Process a single domain ---
# --- Process a single domain using find_logo ---
def process_domain(domain):
    print(f"🔍 Processing: {domain}")
    logo_url = None

    try:
        # First, try fetching page source using Selenium
        print(f"[INFO] Trying Selenium first for {domain}...")
        selenium_source = fetch_page_source_with_selenium(domain)
        if selenium_source:
            logo_url = find_logo(domain, page_source=selenium_source, selenium=True)
            if logo_url and logo_url != "FALLBACK No candidates":
                print(f"[✅] Logo found via Selenium for {domain}")
            else:
                print(f"[⚠️] Selenium did not find a logo, will try requests fallback")
                selenium_source = None  # treat as failed

        # If Selenium failed, try using requests
        if not selenium_source:
            print(f"[INFO] Trying requests fallback for {domain}...")
            try:
                resp = session.get(f"https://{domain}", headers=headers, timeout=10)
                if resp.status_code == 503:
                    print(f"⚠️ Skipping {domain}: 503 Service Unavailable")
                    return {'domain': domain, 'logo_url': '503 Service Unavailable'}

                resp.raise_for_status()
                page_source = resp.text
                logo_url = find_logo(domain, page_source=page_source)
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Requests failed for {domain}: {e}")
                logo_url = find_logo(domain)  # last-resort attempt

    except Exception as e:
        print(f"[❌] Unexpected error for {domain}: {e}")
        logo_url = find_logo(domain)  # last-resort attempt

    print(f"➡️ Final logo_url for {domain}: {logo_url}")
    return {
        'domain': domain,
        'logo_url': logo_url or ''
    }


def main():
    # Your specific domains only
    df = pd.read_parquet("logos.snappy.parquet")
    col = df.columns[0]
    #df = df[df[col].str.contains("kalyan", case=False, na=False)]
    blacklist = {"cafelasmargaritas.es", "berlitz.com.py"}
    domains = [
                  s for s in df[col].dropna().astype(str).tolist()
                  if s not in blacklist
              ][:100]

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_domain, domain): domain for domain in domains}
        for future in as_completed(futures):
            domain = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Error processing {domain}: {e}")
                results.append({
                    'domain': domain,
                    'logo_url': f"Unexpected error: {e}"
                })

    df = pd.DataFrame(results)
    df.to_csv("logos_found.csv", index=False)
    print("✅ Saved to 'logos_found.csv'")

    bad_links = df[~df["logo_url"].str.startswith("local_file:")]
    if not bad_links.empty:
        bad_links.to_csv("bad_logos.csv", index=False)
        print(f"⚠️ Saved {len(bad_links)} bad links to 'bad_logos.csv'")

    print("\n📊 Summary:")
    print(f"Total domains processed: {len(domains)}")
    print(f"✅ Logos found: {len(df) - len(bad_links)}")
    print(f"❌ Logos missing: {len(bad_links)}")


if __name__ == "__main__":
    main()