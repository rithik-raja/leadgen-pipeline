from __future__ import annotations

import logging
import random
import urllib.request
from collections.abc import Callable, Iterable
from typing import Any

from playwright.sync_api import sync_playwright

from .website_enrich_types import PageResponse
from .website_enrich_plugins import (
    valid_website_plugin,
    cms_detect_plugin,
    email_scrape_plugin,
    llm_extract_plugin,
)

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


EnrichmentPlugin = Callable[[dict[str, Any], PageResponse], None]

DEFAULT_PLUGINS: tuple[EnrichmentPlugin, ...] = (
    #valid_website_plugin,
    #email_scrape_plugin,
    #cms_detect_plugin,
    llm_extract_plugin,
)


_CONNECTIVITY_PROBE = "https://www.google.com"
_CONNECTIVITY_TIMEOUT = 5


def _assert_network_reachable(original_exc: Exception) -> None:
    """Raise a RuntimeError if the network appears to be down, otherwise return silently."""
    try:
        urllib.request.urlopen(_CONNECTIVITY_PROBE, timeout=_CONNECTIVITY_TIMEOUT)
    except Exception:
        raise RuntimeError("Network appears to be down — aborting enrichment.") from original_exc


def fetch_page(url: str) -> PageResponse:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = context.new_page()
        nav_response = page.goto(url, wait_until="domcontentloaded")
        html = page.content()
        final_url = page.url
        headers = dict(nav_response.headers) if nav_response else {}
        browser.close()
    return PageResponse(text=html, url=final_url, headers=headers)


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

    logger.info("Fetching website: %s", website)
    try:
        response = fetch_page(website)
    except Exception as exc:
        _assert_network_reachable(exc)
        logger.warning("Failed to fetch website %s: %s", website, exc)
        return

    for plugin in selected_plugins:
        plugin_name = plugin.__name__
        logger.debug("Running plugin %s on %s", plugin_name, website)
        plugin(item, response)
        logger.debug("Plugin %s finished for %s", plugin_name, website)
