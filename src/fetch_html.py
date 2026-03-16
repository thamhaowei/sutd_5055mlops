# src/fetch_html.py
from __future__ import annotations

import argparse
import csv
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import requests

DEFAULT_USER_AGENT = "SUTD-Student-Project-MLops"

@dataclass
class FetchResult:
    url: str
    raw_path: str
    status: str
    retrieved_at: str
    bytes: int
    error: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(url: str) -> str:
    """
    Convert URL to a stable filename-ish slug.
    - ignores fragment (#...)
    - keeps path + query (paged=1 etc.)
    """
    parsed = urlparse(url)
    path = parsed.path.strip("/").replace("/", "_") or "root"
    query = parsed.query.replace("=", "_").replace("&", "_")
    return f"{path}_{query}" if query else path


def read_seed_urls(seed_file: str) -> List[str]:
    with open(seed_file, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]


def fetch_html(
    url: str,
    headers: dict,
    timeout: int = 20,
    retries: int = 2,
    backoff_sec: float = 1.5,
) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Returns: (html_text or None, status_or_error, error_message_or_None)
    """
    last_err: Optional[str] = None

    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            status = str(resp.status_code)

            if resp.status_code != 200:
                last_err = f"HTTP_{resp.status_code}"
            else:
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text, status, None

        except Exception as e:
            last_err = repr(e)
            status = "EXCEPTION"

        # retry with backoff
        if attempt < retries:
            time.sleep(backoff_sec * (attempt + 1))

    return None, status, last_err


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def append_metadata_csv(meta_path: str, rows: List[FetchResult]) -> None:
    """
    Appends rows to CSV; writes header if file doesn't exist.
    """
    ensure_dir(os.path.dirname(meta_path))

    file_exists = os.path.exists(meta_path)
    with open(meta_path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["url", "raw_path", "status", "retrieved_at", "bytes", "error"],
        )
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(
                {
                    "url": r.url,
                    "raw_path": r.raw_path,
                    "status": r.status,
                    "retrieved_at": r.retrieved_at,
                    "bytes": r.bytes,
                    "error": r.error,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch raw HTML pages from seed URLs.")
    parser.add_argument("--seed", default="data/seed_urls.txt", help="Path to seed_urls.txt")
    parser.add_argument("--out", default="data/raw", help="Output directory for raw HTML")
    parser.add_argument("--meta", default="data/raw/metadata.csv", help="Metadata CSV path")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay (seconds) between requests")
    parser.add_argument("--timeout", type=int, default=20, help="Request timeout (seconds)")
    parser.add_argument("--retries", type=int, default=2, help="Retries after failure")
    parser.add_argument("--user_agent", default=DEFAULT_USER_AGENT, help="User-Agent string")
    args = parser.parse_args()

    ensure_dir(args.out)

    headers = {"User-Agent": args.user_agent}

    urls = read_seed_urls(args.seed)
    if not urls:
        raise RuntimeError(f"No URLs found in {args.seed}")

    results: List[FetchResult] = []

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Fetching: {url}")

        retrieved_at = utc_now_iso()
        html, status, err = fetch_html(
            url=url,
            headers=headers,
            timeout=args.timeout,
            retries=args.retries,
        )

        if html is not None:
            filename = slugify(url) + ".html"
            raw_path = os.path.join(args.out, filename)

            with open(raw_path, "w", encoding="utf-8") as f:
                f.write(html)

            results.append(
                FetchResult(
                    url=url,
                    raw_path=raw_path,
                    status=status,
                    retrieved_at=retrieved_at,
                    bytes=len(html.encode("utf-8")),
                    error="",
                )
            )
        else:
            results.append(
                FetchResult(
                    url=url,
                    raw_path="",
                    status=status,
                    retrieved_at=retrieved_at,
                    bytes=0,
                    error=err or "UNKNOWN_ERROR",
                )
            )

        # be polite
        if i < len(urls):
            time.sleep(args.delay)

    append_metadata_csv(args.meta, results)
    print(f"Done. HTML saved to {args.out} and metadata written to {args.meta}")


if __name__ == "__main__":
    main()

# to run: python -m src.fetch_html --seed data/seed_urls.txt --out data/raw --meta data/raw/metadata.csv --delay 2
# to run 2: python -m src.fetch_html --seed data/seed_urls_pages.txt --out data/raw/pages --meta data/raw/pages/metadata.csv --delay 2

# ass2:
""" 
python -m src.fetch_html \
  --seed data/seed_urls_pages.txt \
  --out data/raw/pages \
  --meta data/raw/pages/metadata.csv \
  --delay 2
  """