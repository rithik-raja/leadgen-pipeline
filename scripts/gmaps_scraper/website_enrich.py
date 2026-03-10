from collections.abc import Callable, Iterable
from typing import Any

import requests
import requests.exceptions as request_exception

from .website_enrich_plugins import cms_detect_plugin, email_scrape_plugin, llm_extract_plugin

EnrichmentPlugin = Callable[[dict[str, Any], requests.Response], None]

CONNECTIVITY_CHECK_URLS: tuple[str, ...] = (
    "https://1.1.1.1/cdn-cgi/trace",
    "https://www.google.com/generate_204",
)


class InternetConnectionError(RuntimeError):
    pass


DEFAULT_PLUGINS: tuple[EnrichmentPlugin, ...] = (
    #email_scrape_plugin,
    cms_detect_plugin,
    llm_extract_plugin,
)


def internet_connection_available() -> bool:
    for url in CONNECTIVITY_CHECK_URLS:
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return True
        except request_exception.RequestException:
            continue
    return False


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
        response = requests.get(website, timeout=15)
        response.raise_for_status()
    except request_exception.MissingSchema:
        item["website_broken"] = True
        return
    except request_exception.HTTPError:
        item["website_broken"] = True
        return
    except (
        request_exception.ConnectionError,
        request_exception.Timeout,
        request_exception.TooManyRedirects,
        request_exception.InvalidURL,
        request_exception.InvalidSchema,
        request_exception.SSLError,
    ) as exc:
        if not internet_connection_available():
            raise InternetConnectionError("Internet connection appears unavailable.") from exc
        item["website_broken"] = True
        return

    for plugin in selected_plugins:
        plugin(item, response)
