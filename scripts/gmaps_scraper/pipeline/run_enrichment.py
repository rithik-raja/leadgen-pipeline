import argparse
import logging
from pathlib import Path

from ..utils.pipeline_common import (
    configure_logging,
    enrich_stage_path,
    extract_run_id,
    list_raw_stage_files,
    load_mapping,
    load_items,
    raw_stage_path,
    save_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich staged Google Maps scrape files into resumable staging outputs."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier to enrich. Defaults to all raw stage files.",
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


def resolve_raw_stage_files(run_id: str | None) -> list[Path]:
    if run_id is None:
        return list_raw_stage_files()

    raw_path = raw_stage_path(run_id)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw stage file not found: {raw_path}")
    return [raw_path]


def extract_enrichment_payload(raw_item: dict[str, object], enriched_item: dict[str, object]) -> dict[str, object]:
    return {
        key: value
        for key, value in enriched_item.items()
        if key not in raw_item
    }


def enrich_items_incrementally(raw_path: Path, pretty: bool) -> int:
    from ..utils.website_enrich import enrich_website

    run_id = extract_run_id(raw_path)
    output_path = enrich_stage_path(run_id)
    items = load_items(raw_path)
    links: list[str] = []
    seen_links: set[str] = set()

    for item in items:
        link = item.get("link")
        if not isinstance(link, str) or not link.strip():
            raise ValueError(f"Every raw item must have a non-empty 'link' in {raw_path}")
        if link in seen_links:
            raise ValueError(f"Duplicate 'link' value in {raw_path}: {link}")
        seen_links.add(link)
        links.append(link)

    if output_path.exists():
        enrichments = load_mapping(output_path)
        unknown_links = set(enrichments) - seen_links
        if unknown_links:
            raise ValueError(
                f"Enrichment stage contains links not found in raw stage for run {run_id}: "
                f"{sorted(unknown_links)}"
            )
    else:
        enrichments = {}

    start_index = 0
    while start_index < len(links) and links[start_index] in enrichments:
        start_index += 1

    logging.info(
        "Enriching run %s starting at item %d of %d",
        run_id,
        start_index + 1 if start_index < len(items) else len(items),
        len(items),
    )

    for index in range(start_index, len(items)):
        item = items[index]
        link = links[index]
        working_item = dict(item)
        website = item.get("website")
        if isinstance(website, str) and website.strip():
            enrich_website(working_item)

        enrichments[link] = extract_enrichment_payload(item, working_item)
        save_json(output_path, enrichments, pretty)
        logging.info(
            "Enriched item %d/%d for run %s -> %s",
            index + 1,
            len(items),
            run_id,
            output_path,
        )

    return len(items)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    try:
        raw_files = resolve_raw_stage_files(args.run_id)
        if not raw_files:
            logging.info("No raw stage files found.")
            return 0

        total_records = 0
        for raw_file in raw_files:
            total_records += enrich_items_incrementally(raw_file, args.pretty)

        logging.info(
            "Completed enrichment for %d run(s), %d record(s) total.",
            len(raw_files),
            total_records,
        )
        return 0
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        return 130
    except Exception as exc:
        logging.error("Enrichment failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
