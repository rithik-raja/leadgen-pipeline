from collections import deque
import urllib.parse
import re
from bs4 import BeautifulSoup
import requests
import requests.exceptions as request_exception


def get_base_url(url: str) -> str:
    """
    Extracts the base URL (scheme and netloc) from a given URL.

    :param url: The full URL from which to extract the base.
    :return: The base URL in the form 'scheme://netloc'.
    """

    parts = urllib.parse.urlsplit(url)
    return '{0.scheme}://{0.netloc}'.format(parts)


def get_page_path(url: str) -> str:
    """
    Extracts the page path from the given URL, used to normalize relative links.

    :param url: The full URL from which to extract the page path.
    :return: The page path (URL up to the last '/').
    """

    parts = urllib.parse.urlsplit(url)
    return url[:url.rfind('/') + 1] if '/' in parts.path else url


def extract_emails(response_text: str) -> set[str]:
    """
    Extracts all email addresses from the provided HTML text.

    :param response_text: The raw HTML content of a webpage.
    :return: A set of email addresses found within the content.
    """

    email_pattern = r'[a-z0-9\.\-+]+@[a-z0-9\.\-+]+\.[a-z]+'
    return set(re.findall(email_pattern, response_text, re.I))


def normalize_link(link: str, base_url: str, page_path: str) -> str:
    """
    Normalizes relative links into absolute URLs.

    :param link: The link to normalize (could be relative or absolute).
    :param base_url: The base URL for relative links starting with '/'.
    :param page_path: The page path for relative links not starting with '/'.
    :return: The normalized link as an absolute URL.
    """

    if link.startswith('/'):
        return base_url + link
    elif not link.startswith('http'):
        return page_path + link
    return link


def scrape_website(start_url: str, max_count: int = 100) -> set[str]:
    """
    Scrapes a website starting from the given URL, follows links, and collects email addresses.

    :param start_url: The initial URL to start scraping.
    :param max_count: The maximum number of pages to scrape. Defaults to 100.
    :return: A set of email addresses found during the scraping process.
    """

    urls_to_process = deque([start_url])
    scraped_urls = set()
    collected_emails = set()
    count = 0

    while urls_to_process:
        count += 1
        if count > max_count:
            break

        url = urls_to_process.popleft()
        if url in scraped_urls:
            continue

        scraped_urls.add(url)
        base_url = get_base_url(url)
        page_path = get_page_path(url)

        print(f'[{count}] Processing {url}')

        try:
            response = requests.get(url)
            response.raise_for_status()
        except (request_exception.RequestException, request_exception.MissingSchema, request_exception.ConnectionError):
            print('There was a request error')
            continue

        collected_emails.update(extract_emails(response.text))

        soup = BeautifulSoup(response.text, 'lxml')

        for anchor in soup.find_all('a'):
            link = anchor.get('href', '')
            normalized_link = normalize_link(link, base_url, page_path) # type: ignore
            if normalized_link not in urls_to_process and normalized_link not in scraped_urls:
                urls_to_process.append(normalized_link)

    return collected_emails