from __future__ import annotations

import math
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://arxiv.org/search/?"
PAGE_SIZE = 50
HEADERS = {"User-Agent": "get-arXiv/0.1 (+https://arxiv.org)"}
TOTAL_RE = re.compile(r"of\s+([0-9,]+)\s+results", re.IGNORECASE)


def build_search_url(last_name: str, first_name: str, start: int) -> str:
    query = f"{last_name}, {first_name}"
    params = {
        "searchtype": "all",
        "query": query,
        "abstracts": "show",
        "size": str(PAGE_SIZE),
        "order": "-announced_date_first",
        "start": str(start),
    }
    return f"{BASE_URL}{urlencode(params)}"


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def parse_total_results(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    for text in soup.find_all(string=re.compile(r"Showing", re.IGNORECASE)):
        match = TOTAL_RE.search(text)
        if match:
            return int(match.group(1).replace(",", ""))
    match = TOTAL_RE.search(soup.get_text(" ", strip=True))
    if match:
        return int(match.group(1).replace(",", ""))
    return 0


def ensure_directories(root: Path, last_name: str, first_name: str) -> Path:
    author_dir = root / "AUTHORS" / f"{last_name}-{first_name}"
    html_dir = author_dir / "HTML"
    html_dir.mkdir(parents=True, exist_ok=True)
    return html_dir


def save_html(html_dir: Path, page_number: int, html: str) -> None:
    output_path = html_dir / f"page-{page_number}.html"
    output_path.write_text(html, encoding="utf-8")


def parse_args(argv: list[str]) -> tuple[str, str]:
    if len(argv) != 3:
        print("Usage: python main.py <last-name> <first-name>", file=sys.stderr)
        raise SystemExit(2)
    last_name = argv[1].strip().strip(",")
    first_name = argv[2].strip().strip(",")
    if not last_name or not first_name:
        print("Both last-name and first-name are required.", file=sys.stderr)
        raise SystemExit(2)
    return last_name, first_name


def main() -> None:
    last_name, first_name = parse_args(sys.argv)
    root = Path(__file__).resolve().parent
    html_dir = ensure_directories(root, last_name, first_name)

    with requests.Session() as session:
        first_url = build_search_url(last_name, first_name, start=0)
        first_html = fetch_html(session, first_url)
        save_html(html_dir, 1, first_html)

        total_results = parse_total_results(first_html)
        total_pages = max(1, math.ceil(total_results / PAGE_SIZE))

        for page_number in range(2, total_pages + 1):
            start = (page_number - 1) * PAGE_SIZE
            url = build_search_url(last_name, first_name, start=start)
            html = fetch_html(session, url)
            save_html(html_dir, page_number, html)
            time.sleep(1)

    print(
        f"Saved {total_pages} page(s) for {last_name}, {first_name} to {html_dir}"
    )


if __name__ == "__main__":
    main()
