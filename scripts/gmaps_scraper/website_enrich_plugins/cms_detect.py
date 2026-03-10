from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
import requests


CMS_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "wordpress",
        (
            "wp-content",
            "wp-includes",
            "wp-json",
            "wordpress",
            "/xmlrpc.php",
            "wordpress.org",
        ),
    ),
    (
        "wix",
        (
            "wixstatic.com",
            "wix.com",
            "_wixcss",
            "_wixbi",
            "wix-image",
            "x-wix-",
        ),
    ),
    (
        "webflow",
        (
            "webflow",
            "webflow.js",
            "webflow.css",
            "data-wf-page",
            "data-wf-site",
        ),
    ),
    (
        "shopify",
        (
            "cdn.shopify.com",
            "shopify.theme",
            "shopify.com/s/",
            "x-shopid",
            "shopify-payment-button",
        ),
    ),
    (
        "squarespace",
        (
            "static.squarespace.com",
            "squarespace.com",
            "squarespace-cdn.com",
            "sqs-",
        ),
    ),
    (
        "joomla",
        (
            "/media/system/js/",
            "/components/com_",
            "joomla!",
            "joomla",
            "option=com_",
        ),
    ),
    (
        "drupal",
        (
            "/sites/default/files/",
            "/sites/all/",
            "drupal-settings-json",
            "drupal.js",
            "drupal",
        ),
    ),
    (
        "ghost",
        (
            "ghost/content",
            "ghost.io",
            "data-ghost",
            "ghost-sdk",
        ),
    ),
    (
        "weebly",
        (
            "weebly.com",
            "weeblycdn.com",
            "weeblycloud.com",
        ),
    ),
    (
        "duda",
        (
            "duda.co",
            "dudamobile.com",
            "dmcdn.net",
            "siteapi.io",
        ),
    ),
    (
        "hubspot cms",
        (
            "hubspot",
            "hs-scripts.com",
            "hsforms.net",
            "hubspotusercontent",
        ),
    ),
    (
        "godaddy",
        (
            "img1.wsimg.com",
            "img6.wsimg.com",
            "secureserver.net",
            "websitebuilder",
            "godaddy",
            "wsimg.com",
        ),
    ),
    (
        "argo",
        (
            "argo",
            "argo-cdn",
            "builtwithargo",
        ),
    ),
)


GENERATOR_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("wordpress", re.compile(r"\bwordpress\b", re.I)),
    ("joomla", re.compile(r"\bjoomla!?(\s|$)", re.I)),
    ("drupal", re.compile(r"\bdrupal\b", re.I)),
    ("squarespace", re.compile(r"\bsquarespace\b", re.I)),
    ("wix", re.compile(r"\bwix\b", re.I)),
    ("webflow", re.compile(r"\bwebflow\b", re.I)),
    ("shopify", re.compile(r"\bshopify\b", re.I)),
    ("ghost", re.compile(r"\bghost\b", re.I)),
    ("weebly", re.compile(r"\bweebly\b", re.I)),
    ("hubspot cms", re.compile(r"\bhubspot\b", re.I)),
    ("godaddy", re.compile(r"\bgodaddy\b", re.I)),
    ("argo", re.compile(r"\bargo\b", re.I)),
)


def _header_values(response: requests.Response) -> str:
    return " ".join(f"{key} {value}" for key, value in response.headers.items()).lower()


def _get_meta_content(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("meta", attrs={"name": name})
    if not tag:
        return ""
    content = tag.get("content")
    return content if isinstance(content, str) else ""


def _iter_asset_urls(soup: BeautifulSoup) -> list[str]:
    asset_urls: list[str] = []
    for tag_name, attr_name in (("script", "src"), ("link", "href"), ("img", "src")):
        for tag in soup.find_all(tag_name):
            attr_value = tag.get(attr_name)
            if isinstance(attr_value, str) and attr_value:
                asset_urls.append(attr_value.lower())
    return asset_urls


def _is_wix(response: requests.Response, soup: BeautifulSoup, haystack: str) -> bool:
    if any(pattern in haystack for pattern in ("_wixcss", "_wixbi", "wixstatic.com", "x-wix-")):
        return True
    html_tag = soup.find("html")
    if html_tag and html_tag.get("id") == "wix":
        return True
    return urlsplit(response.url).netloc.endswith(".wixsite.com")


def _is_webflow(soup: BeautifulSoup, haystack: str) -> bool:
    if "webflow" in haystack and ("data-wf-page" in haystack or "data-wf-site" in haystack):
        return True
    html_tag = soup.find("html")
    if not html_tag:
        return False
    return html_tag.has_attr("data-wf-page") or html_tag.has_attr("data-wf-site")


def _is_shopify(response: requests.Response, haystack: str) -> bool:
    if any(pattern in haystack for pattern in ("cdn.shopify.com", "shopify.theme", "x-shopid")):
        return True
    return ".myshopify.com" in response.text.lower()


def detect_cms(response: requests.Response) -> str | None:
    text_lower = response.text.lower()
    soup = BeautifulSoup(response.text, "lxml")
    generator = _get_meta_content(soup, "generator")
    generator_lower = generator.lower()
    asset_urls = _iter_asset_urls(soup)
    header_values = _header_values(response)
    haystack = " ".join((text_lower, generator_lower, header_values, " ".join(asset_urls)))

    for cms_name, generator_pattern in GENERATOR_PATTERNS:
        if generator_pattern.search(generator):
            return cms_name

    if _is_wix(response, soup, haystack):
        return "wix"
    if _is_webflow(soup, haystack):
        return "webflow"
    if _is_shopify(response, haystack):
        return "shopify"

    for cms_name, patterns in CMS_PATTERNS:
        if any(pattern in haystack for pattern in patterns):
            return cms_name

    return None


def cms_detect_plugin(item: dict[str, Any], response: requests.Response) -> None:
    cms_name = detect_cms(response)
    if cms_name:
        item["cms"] = cms_name
