from collections.abc import Callable, Iterable
from typing import Any

import requests
import requests.exceptions as request_exception

from .website_enrich_plugins import cms_detect_plugin, email_scrape_plugin, llm_extract_plugin

EnrichmentPlugin = Callable[[dict[str, Any], requests.Response], None]


DEFAULT_PLUGINS: tuple[EnrichmentPlugin, ...] = (
    email_scrape_plugin,
    cms_detect_plugin,
    llm_extract_plugin,
)


def enrich_website(
    item: dict[str, Any],
    plugins: Iterable[EnrichmentPlugin] | None = None,
) -> None:
    """
    Fetches the item's website and applies enrichment plugins to the response.

    :param item: The item to enrich in place. Must include a website URL.
    :param plugins: Response plugins to run on every fetched page.
    """
    website = item.get("website")
    if not isinstance(website, str) or not website.strip():
        return

    selected_plugins = tuple(plugins or DEFAULT_PLUGINS)
    if not selected_plugins:
        return

    try:
        response = requests.get(website)
        response.raise_for_status()
    except (request_exception.RequestException, request_exception.MissingSchema, request_exception.ConnectionError):
        print('There was a request error')
        return

    for plugin in selected_plugins:
        plugin(item, response)
