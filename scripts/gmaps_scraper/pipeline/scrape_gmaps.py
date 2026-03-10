import argparse
import asyncio
import logging

from ..utils.pipeline_common import (
    configure_logging,
    create_run_id,
    format_json,
    raw_stage_path,
    save_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps places into a staging file."
    )
    parser.add_argument(
        "--query",
        required=True,
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
        "--run-id",
        default=None,
        help="Optional run identifier used for staging filenames.",
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
    if args.max_places is not None and args.max_places <= 0:
        raise ValueError("--max-places must be a positive integer.")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be a positive integer.")


async def run(args: argparse.Namespace) -> int:
    from ..utils.scraper import scrape_google_maps

    results = await scrape_google_maps(
        query=args.query,
        max_places=args.max_places,
        lang=args.lang,
        headless=args.headless,
        concurrency=args.concurrency,
    )

    run_id = args.run_id or create_run_id()
    output_path = raw_stage_path(run_id)
    save_json(output_path, results, args.pretty)
    logging.info("Saved raw scrape stage with %d records -> %s", len(results), output_path)

    if args.print:
        print(format_json(results, args.pretty))
    return 0


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

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
