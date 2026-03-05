# count_domain_roots.py
import sys
import pandas as pd
from urllib.parse import urlparse, unquote

try:
    import tldextract
except ImportError:
    raise SystemExit("Please install tldextract:  pip install tldextract")

# Use baked-in PSL snapshot (no network fetch). Remove suffix_list_urls=None to allow updates.
_extract = tldextract.TLDExtract(suffix_list_urls=None)

def to_host(s: str) -> str | None:
    s = unquote(str(s)).strip()
    if not s:
        return None
    if not (s.startswith("http://") or s.startswith("https://") or s.startswith("//")):
        s = "http://" + s
    host = urlparse(s).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None

def to_root(host: str) -> str | None:
    ext = _extract(host)
    return ext.domain or None   # “example” from example.co.uk

def main(path: str = "logos.snappy.parquet"):
    df = pd.read_parquet(path)
    col = df.columns[0]
    roots = (
        df[col]
        .dropna()
        .map(to_host)
        .dropna()
        .map(to_root)
        .dropna()
    )
    counts = roots.value_counts().rename_axis("root").reset_index(name="count")
    counts.to_csv("domain_root_counts.csv", index=False)
    print(counts.head(50).to_string(index=False))
    print("\nSaved full results to domain_root_counts.csv")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "logos.snappy.parquet")
