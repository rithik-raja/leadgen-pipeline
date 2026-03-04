import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

TMP_DIR = Path("data/tmp")
FINAL_DIR = Path("data/scraped/gmaps")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps places from a search query."
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Ignore scrape params and enrich all data/tmp/gmap_*.json files.",
    )
    parser.add_argument(
        "--query",
        required=False,
        help="Search query for Google Maps, e.g. 'restaurants in New York'",
    )
    parser.add_argument(
        "--max-places",
        type=int,
        default=None,
        help="Maximum number of places to scrape (default: scrape all found).",
    )
    parser.add_argument(
        "--lang",
        default="en",
        help="Google Maps language code (default: en).",
    )
    parser.add_argument(
        "--headless",
        dest="headless",
        action="store_true",
        help="Run browser in headless mode (default).",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser with UI visible.",
    )
    parser.set_defaults(headless=True)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of concurrent tabs for detail scraping (default: 5).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional final output file path. Ignored when --enrich is used.",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print JSON output to stdout in addition to writing to file.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.enrich:
        return
    if not args.query:
        raise ValueError("--query is required unless --enrich is provided.")
    if args.max_places is not None and args.max_places <= 0:
        raise ValueError("--max-places must be a positive integer.")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be a positive integer.")


def format_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, indent=2, ensure_ascii=False)
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def load_scraper():
    try:
        from .scraper import scrape_google_maps as scrape_fn
        return scrape_fn
    except ImportError:
        package_root = Path(__file__).resolve().parent.parent
        if str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
        from gmaps_scraper.scraper import scrape_google_maps as scrape_fn  # type: ignore
        return scrape_fn


def load_website_enricher():
    try:
        from .website_enrich import scrape_website as enrich_fn
        return enrich_fn
    except ImportError:
        package_root = Path(__file__).resolve().parent.parent
        if str(package_root) not in sys.path:
            sys.path.insert(0, str(package_root))
        from gmaps_scraper.website_enrich import scrape_website as enrich_fn  # type: ignore
        return enrich_fn


def timestamped_gmap_filename() -> str:
    return datetime.now().strftime("gmap_%Y-%m-%dT%H:%M:%S.json")


def enrich_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scrape_website = load_website_enricher()
    enriched: list[dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        record = dict(item)
        emails: list[str] = []
        website = record.get("website")
        if isinstance(website, str) and website.strip():
            try:
                emails = sorted(scrape_website(website))
            except Exception as exc:
                logging.warning("Email enrichment failed for item %d (%s): %s", idx, website, exc)
        record["emails"] = emails
        enriched.append(record)
    return enriched


def save_json(path: Path, data: Any, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_json(data, pretty) + "\n", encoding="utf-8")


def enrich_file(tmp_path: Path, pretty: bool, final_output: Path | None = None, print_output: bool = False) -> int:
    raw = json.loads(tmp_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Expected JSON array in {tmp_path}")
    if any(not isinstance(x, dict) for x in raw):
        raise ValueError(f"Expected array of objects in {tmp_path}")
    items = raw
    enriched = enrich_items(items)

    final_path = final_output if final_output is not None else FINAL_DIR / tmp_path.name
    save_json(final_path, enriched, pretty)
    tmp_path.unlink()
    logging.info("Enriched %d records -> %s (removed %s)", len(enriched), final_path, tmp_path)

    if print_output:
        print(format_json(enriched, pretty))
    return len(enriched)


async def run(args: argparse.Namespace) -> int:
    if args.enrich:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        pending = sorted(TMP_DIR.glob("gmap_*.json"))
        if not pending:
            logging.info("No pending files found in %s", TMP_DIR)
            return 0

        total = 0
        for tmp_file in pending:
            total += enrich_file(tmp_file, pretty=args.pretty)
        logging.info("Enriched %d file(s), %d record(s) total.", len(pending), total)
        return 0

    scrape_google_maps = load_scraper()
    results = await scrape_google_maps(
        query=args.query,
        max_places=args.max_places,
        lang=args.lang,
        headless=args.headless,
        concurrency=args.concurrency,
    )

    filename = timestamped_gmap_filename()
    tmp_path = TMP_DIR / filename
    save_json(tmp_path, results, args.pretty)
    logging.info("Saved raw scrape to %s", tmp_path)

    final_path = args.output if args.output is not None else FINAL_DIR / filename
    enrich_file(tmp_path, pretty=args.pretty, final_output=final_path, print_output=args.print)
    return 0


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        validate_args(args)
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        return 130
    except Exception as exc:
        logging.error("Scrape failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
