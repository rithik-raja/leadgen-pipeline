import argparse
import logging
from pathlib import Path

from ..utils.pipeline_common import (
    FINAL_DIR,
    configure_logging,
    enrich_stage_path,
    extract_run_id,
    final_output_path,
    list_enrich_stage_files,
    load_mapping,
    load_items,
    raw_stage_path,
    save_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Finalize enriched Google Maps staging files into final outputs."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run identifier to finalize. Defaults to all completed runs.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args()


def resolve_enriched_stage_files(run_id: str | None) -> list[Path]:
    if run_id is None:
        return list_enrich_stage_files()

    enrich_path = enrich_stage_path(run_id)
    if not enrich_path.exists():
        raise FileNotFoundError(f"Enrichment stage file not found: {enrich_path}")
    return [enrich_path]


def finalize_run(enriched_path: Path) -> Path:
    run_id = extract_run_id(enriched_path)
    raw_path = raw_stage_path(run_id)
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw stage file for run {run_id}: {raw_path}")

    raw_items = load_items(raw_path)
    enrichments = load_mapping(enriched_path)
    finalized_items: list[dict[str, object]] = []

    for item in raw_items:
        link = item.get("link")
        if not isinstance(link, str) or not link.strip():
            raise ValueError(f"Every raw item must have a non-empty 'link' in {raw_path}")
        if link not in enrichments:
            raise ValueError(f"Run {run_id} is incomplete: missing enrichment entry for {link}")

        finalized_items.append({**item, **enrichments[link]})

    unknown_links = set(enrichments) - {
        item["link"] for item in raw_items if isinstance(item.get("link"), str)
    }
    if unknown_links:
        raise ValueError(
            f"Enrichment stage contains links not found in raw stage for run {run_id}: "
            f"{sorted(unknown_links)}"
        )

    output_path = final_output_path(run_id)
    save_json(output_path, finalized_items, pretty=False)
    enriched_path.unlink()
    raw_path.unlink()
    logging.info(
        "Finalized run %s -> %s (removed %s and %s)",
        run_id,
        output_path,
        raw_path,
        enriched_path,
    )
    return output_path


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    try:
        enriched_files = resolve_enriched_stage_files(args.run_id)
        if not enriched_files:
            logging.info("No enrichment stage files found.")
            return 0

        finalized = 0
        for enriched_file in enriched_files:
            finalize_run(enriched_file)
            finalized += 1

        logging.info("Finalized %d run(s) into %s", finalized, FINAL_DIR)
        return 0
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        return 130
    except Exception as exc:
        logging.error("Finalize failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
