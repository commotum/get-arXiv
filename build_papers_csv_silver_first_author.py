from __future__ import annotations

import argparse
import csv
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

NAMESPACES = {"atom": "http://www.w3.org/2005/Atom"}
YEAR_RE = re.compile(r"^(\d{4})")
VERSION_RE = re.compile(r"v\d+$")


@dataclass(frozen=True)
class Paper:
    year: int
    title: str
    url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a David Silver first-author papers CSV from cached arXiv API XML files."
    )
    parser.add_argument(
        "--author-dir",
        default="AUTHORS/Silver-David",
        help="Author directory containing API/page-*.xml files (default: AUTHORS/Silver-David).",
    )
    parser.add_argument(
        "--output",
        default="papers-silver-david-first-author.csv",
        help="Output CSV path (default: papers-silver-david-first-author.csv).",
    )
    return parser.parse_args()


def normalize_space(text: str) -> str:
    return " ".join(text.split())


def normalize_tokens(text: str) -> list[str]:
    cleaned = re.sub(r"[^a-zA-Z]+", " ", text).strip().lower()
    if not cleaned:
        return []
    return [token for token in cleaned.split() if token]


def is_david_silver(name: str) -> bool:
    tokens = normalize_tokens(name)
    if len(tokens) < 2:
        return False

    first = "david"
    first_initial = "d"
    last_tokens = ["silver"]

    if tokens[-len(last_tokens) :] == last_tokens:
        given = tokens[0]
        return given == first or given.startswith(first) or given == first_initial

    if tokens[: len(last_tokens)] == last_tokens and len(tokens) > len(last_tokens):
        given = tokens[len(last_tokens)]
        return given == first or given.startswith(first) or given == first_initial

    return False


def first_author_is_david_silver(entry: ET.Element) -> bool:
    first_author = entry.find("atom:author", NAMESPACES)
    if first_author is None:
        return False
    name = first_author.findtext("atom:name", default="", namespaces=NAMESPACES).strip()
    if not name:
        return False
    return is_david_silver(name)


def canonical_pdf_url_from_entry(entry: ET.Element) -> str | None:
    entry_id = entry.findtext("atom:id", default="", namespaces=NAMESPACES).strip()
    if not entry_id:
        return None

    if "/abs/" in entry_id:
        arxiv_id = entry_id.split("/abs/", 1)[1]
    else:
        arxiv_id = entry_id

    arxiv_id = arxiv_id.strip()
    if not arxiv_id:
        return None

    arxiv_id = VERSION_RE.sub("", arxiv_id)
    return f"https://arxiv.org/pdf/{arxiv_id}.pdf"


def parse_year(entry: ET.Element) -> int | None:
    published = entry.findtext("atom:published", default="", namespaces=NAMESPACES).strip()
    if not published:
        return None
    match = YEAR_RE.match(published)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def iter_api_files(author_dir: Path) -> list[Path]:
    return sorted((author_dir / "API").glob("page-*.xml"))


def collect_papers(author_dir: Path) -> tuple[list[Paper], int]:
    papers_by_url: dict[str, Paper] = {}
    parse_errors = 0

    for xml_path in iter_api_files(author_dir):
        try:
            root = ET.fromstring(xml_path.read_text(encoding="utf-8"))
        except (OSError, ET.ParseError):
            parse_errors += 1
            print(f"Skipping unreadable XML: {xml_path}", file=sys.stderr)
            continue

        for entry in root.findall("atom:entry", NAMESPACES):
            if not first_author_is_david_silver(entry):
                continue

            title = normalize_space(
                entry.findtext("atom:title", default="", namespaces=NAMESPACES)
            )
            year = parse_year(entry)
            url = canonical_pdf_url_from_entry(entry)

            if not title or year is None or not url:
                continue

            paper = Paper(year=year, title=title, url=url)
            existing = papers_by_url.get(url)
            if existing is None:
                papers_by_url[url] = paper
                continue

            if paper.year < existing.year:
                papers_by_url[url] = paper

    rows = sorted(
        papers_by_url.values(),
        key=lambda row: (-row.year, row.title.lower(), row.url),
    )
    return rows, parse_errors


def write_csv(path: Path, rows: list[Paper]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["year", "title", "url"])
        for row in rows:
            writer.writerow([row.year, row.title, row.url])


def main() -> int:
    args = parse_args()
    author_dir = Path(args.author_dir).resolve()
    output_path = Path(args.output).resolve()

    if not author_dir.exists():
        print(f"Missing author directory: {author_dir}", file=sys.stderr)
        return 2

    rows, parse_errors = collect_papers(author_dir)
    write_csv(output_path, rows)

    print(f"Wrote {len(rows)} rows to {output_path}")
    if parse_errors:
        print(f"Skipped {parse_errors} unreadable XML file(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
