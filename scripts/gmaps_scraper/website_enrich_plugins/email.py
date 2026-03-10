import re
import urllib.parse
from collections import deque
from typing import Any

from bs4 import BeautifulSoup
import requests
import requests.exceptions as request_exception


def get_base_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return "{0.scheme}://{0.netloc}".format(parts)


def get_page_path(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return url[: url.rfind("/") + 1] if "/" in parts.path else url


def extract_emails(response_text: str) -> set[str]:
    email_pattern = r"[a-z0-9\.\-+]+@[a-z0-9\.\-+]+\.[a-z]+"
    return set(re.findall(email_pattern, response_text, re.I))


def normalize_link(link: str, base_url: str, page_path: str) -> str:
    if link.startswith("/"):
        return base_url + link
    if not link.startswith("http"):
        return page_path + link
    return link


def normalized_domain(url: str) -> str:
    netloc = urllib.parse.urlsplit(url).netloc.lower()
    if netloc.startswith("www."):
        return netloc[4:]
    return netloc


def email_scrape_plugin(
    item: dict[str, Any],
    response: requests.Response,
    max_count: int = 30,
    max_depth: int = 2,
) -> None:
    start_url = response.url
    urls_to_process = deque([(start_url, 0, response)])
    scraped_urls: set[str] = set()
    collected_emails: set[str] = set()
    count = 0
    start_domain = normalized_domain(start_url)

    while urls_to_process:
        count += 1
        if count > max_count:
            break

        url, depth, current_response = urls_to_process.popleft()
        if url in scraped_urls:
            continue

        scraped_urls.add(url)
        base_url = get_base_url(url)
        page_path = get_page_path(url)
        collected_emails.update(extract_emails(current_response.text))

        soup = BeautifulSoup(current_response.text, "lxml")

        for anchor in soup.find_all("a"):
            link = anchor.get("href", "")
            normalized_link = normalize_link(link, base_url, page_path)  # type: ignore[arg-type]
            if normalized_domain(normalized_link) != start_domain:
                continue
            if depth >= max_depth:
                continue
            if normalized_link in scraped_urls:
                continue
            if any(normalized_link == queued_url for queued_url, _, _ in urls_to_process):
                continue

            try:
                next_response = requests.get(normalized_link)
                next_response.raise_for_status()
            except (
                request_exception.RequestException,
                request_exception.MissingSchema,
                request_exception.ConnectionError,
            ):
                continue

            urls_to_process.append((normalized_link, depth + 1, next_response))

    if collected_emails:
        item["emails"] = sorted(collected_emails)
