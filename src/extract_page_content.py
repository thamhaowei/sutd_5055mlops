# src/extract_page_content.py
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

REMOVE_SELECTORS = [
    "header", "nav", "footer",
    "script", "style", "noscript",
    "form", "svg",
]


def slugify(url: str) -> str:
    p = urlparse(url)
    path = p.path.strip("/").replace("/", "_") or "root"
    query = p.query.replace("=", "_").replace("&", "_")
    return f"{path}_{query}" if query else path


def clean(text: str) -> str:
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def best_text_container(soup: BeautifulSoup):
    # all the info from main body is in <main class>
    main = soup.find("main")
    return main if main else (soup.body or soup)



def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Remove boilerplate blocks
    for sel in REMOVE_SELECTORS:
        for el in soup.select(sel):
            el.decompose()

    container = best_text_container(soup)

    # Get text while preserving paragraph breaks
    text = container.get_text("\n", strip=True)
    return clean(text)


def append_csv(meta_path: Path, rows: list[dict]) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = meta_path.exists()
    with meta_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["source_url", "raw_file", "out_file", "chars"],
        )
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract main content text from raw HTML pages.")
    ap.add_argument("--raw", default="data/raw/pages", help="Directory of raw .html files")
    ap.add_argument("--out", default="data/processed/pages", help="Directory to write cleaned .txt files")
    ap.add_argument("--meta", default="data/processed/pages/metadata.csv", help="Metadata CSV output path")
    ap.add_argument(
        "--url_map",
        default="data/raw/pages/metadata.csv",
        help="Fetch metadata CSV (from fetch_html.py) to map raw_file -> source_url",
    )
    args = ap.parse_args()

    raw_dir = Path(args.raw)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build filename -> url mapping from fetch metadata (if available)
    file_to_url = {}
    url_map_path = Path(args.url_map)
    if url_map_path.exists():
        import pandas as pd
        df = pd.read_csv(url_map_path)
        # raw_path is full path; we just want filename
        for _, row in df.iterrows():
            raw_path = str(row.get("raw_path", "") or "")
            url = str(row.get("url", "") or "")
            if raw_path and url:
                file_to_url[Path(raw_path).name] = url

    rows = []
    html_files = sorted(raw_dir.glob("*.html"))
    if not html_files:
        raise RuntimeError(f"No .html files found in {raw_dir}. Did you run fetch_html.py with --out {raw_dir}?")

    for fp in html_files:
        html = fp.read_text(encoding="utf-8", errors="ignore")
        text = extract_main_text(html)

        src_url = file_to_url.get(fp.name, "")
        out_name = fp.stem + "_content.txt"
        out_path = out_dir / out_name
        out_path.write_text(text, encoding="utf-8")

        rows.append(
            {
                "source_url": src_url,
                "raw_file": fp.name,
                "out_file": out_name,
                "chars": len(text),
            }
        )

        print(f"{fp.name} → {out_name} | chars={len(text)}")

    append_csv(Path(args.meta), rows)
    print(f"Done. Cleaned content written to {out_dir} and metadata to {args.meta}")


if __name__ == "__main__":
    main()

# to run: python -m src.extract_page_content --raw data/raw/pages --out data/processed/pages

# for ass 2
"""
python -m src.extract_page_content \
  --raw data/raw/pages \
  --out data/processed/docs \
  --meta data/processed/docs/metadata.csv
"""