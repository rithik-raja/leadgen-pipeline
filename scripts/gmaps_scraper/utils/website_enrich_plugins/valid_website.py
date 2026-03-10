from typing import Any
from scripts.gmaps_scraper.utils.website_enrich_types import PageResponse


def valid_website_plugin(item: dict[str, Any], response: PageResponse) -> None:
    item["valid_website"] = True