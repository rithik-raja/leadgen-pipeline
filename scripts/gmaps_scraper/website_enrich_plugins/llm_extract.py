from __future__ import annotations

import subprocess
import time
import urllib.parse
from typing import Any

from bs4 import BeautifulSoup
import ollama
from pydantic import BaseModel, ValidationError
import requests
import requests.exceptions as request_exception


OLLAMA_MODEL = "lfm2:24b"
OLLAMA_STARTUP_TIMEOUT_SECONDS = 15.0
FOLLOW_LINK_LIMIT = 5
REQUEST_TIMEOUT_SECONDS = 20
MAX_PAGE_TEXT_CHARS = 16000
LLM_VALIDATION_RETRIES = 3


class RootExtraction(BaseModel):
    iceBreakerInfo: list[str]
    needMoreInfo: bool
    linksToFollow: list[str]


class FollowUpExtraction(BaseModel):
    iceBreakerInfo: list[str]


ROOT_SYSTEM_PROMPT = """You are a senior sales engineer reviewing a business website homepage for cold-email personalization.

Your job is to extract only outreach-relevant facts that can turn into strong ice breakers and decide whether this page alone is enough. Look for founder/operator tenure, local roots, awards, certifications, community work, notable specialties, unusual proof points, and specific differentiators that signal credibility or uniqueness.

Rules:
- Use only facts directly supported by the provided page text.
- Write `iceBreakerInfo` as short, factual, quote-ready strings.
- Set `needMoreInfo` to true only when this page lacks enough high-signal personalization for a strong cold email.
- Populate `linksToFollow` only with promising same-site URLs or paths likely to reveal better personalization, such as about, team, founder, story, awards, testimonials, community, or services pages.
- Ignore vague claims, broad slogans, and generic SEO copy.
- Return valid JSON matching the schema exactly."""

FOLLOW_UP_SYSTEM_PROMPT = """You are a senior sales engineer reviewing a business website homepage for cold-email personalization.

Your job is to extract only outreach-relevant facts that can turn into strong ice breakers. Look for founder/operator tenure, local roots, awards, certifications, community work, notable specialties, unusual proof points, and specific differentiators that signal credibility or uniqueness.

Rules:
- Use only facts directly supported by the provided page text.
- Write `iceBreakerInfo` as short, factual, quote-ready strings.
- Ignore vague claims, broad slogans, and generic SEO copy.
- Return valid JSON matching the schema exactly."""


def get_base_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return "{0.scheme}://{0.netloc}".format(parts)


def get_page_path(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return url[: url.rfind("/") + 1] if "/" in parts.path else url


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


def extract_page_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(soup.stripped_strings)
    return text[:MAX_PAGE_TEXT_CHARS]


def extract_same_site_links(response: requests.Response) -> set[str]:
    soup = BeautifulSoup(response.text, "lxml")
    base_url = get_base_url(response.url)
    page_path = get_page_path(response.url)
    start_domain = normalized_domain(response.url)
    discovered_links: set[str] = set()

    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if not isinstance(href, str) or not href.strip():
            continue

        normalized = normalize_link(href.strip(), base_url, page_path)
        if normalized_domain(normalized) != start_domain:
            continue

        parsed = urllib.parse.urlsplit(normalized)
        if parsed.scheme not in {"http", "https"}:
            continue

        cleaned = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))
        discovered_links.add(cleaned)

    return discovered_links


def normalize_same_site_candidate(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    cleaned_items: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = " ".join(item.split()).strip()
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned_items.append(cleaned)
    return cleaned_items


def wait_for_ollama_server() -> None:
    deadline = time.monotonic() + OLLAMA_STARTUP_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": "Respond with {}"}],
                format="json",
                options={"num_predict": 1},
            )
            return
        except Exception as exc:  # pragma: no cover - depends on local ollama runtime.
            last_error = exc
            time.sleep(0.5)
    if last_error:
        raise last_error
    raise RuntimeError("Timed out waiting for Ollama server to start.")


def run_structured_llm(
    user_content: str,
    system_prompt: str,
    output_model: type[BaseModel],
) -> BaseModel:
    last_error: Exception | None = None
    schema = output_model.model_json_schema()

    for _ in range(LLM_VALIDATION_RETRIES):
        try:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                format=schema,
                options={"temperature": 0},
            )
            content = response.message.content if response.message else ""
            if not isinstance(content, str) or not content.strip():
                raise ValueError("Empty structured output from model.")
            return output_model.model_validate_json(content)
        except (ValidationError, ValueError) as exc:
            last_error = exc

    assert last_error is not None
    raise last_error


def fetch_follow_up_page(url: str) -> requests.Response | None:
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except (
        request_exception.RequestException,
        request_exception.MissingSchema,
        request_exception.ConnectionError,
    ):
        return None
    return response


def llm_extract_plugin(item: dict[str, Any], response: requests.Response) -> None:
    try:
        server_process = subprocess.Popen(
            ["ollama", "serve"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return

    try:
        wait_for_ollama_server()

        root_text = extract_page_text(response.text)
        if not root_text:
            return
        available_links = extract_same_site_links(response)
        available_links_text = "\n".join(sorted(available_links))

        root_result = run_structured_llm(
            user_content=f"""Page text:
{root_text}

Available links:
{available_links_text}""",
            system_prompt=ROOT_SYSTEM_PROMPT,
            output_model=RootExtraction,
        )

        merged_ice_breakers = coerce_string_list(root_result.iceBreakerInfo) # type: ignore
        need_more_info = root_result.needMoreInfo # type: ignore
        suggested_links = coerce_string_list(root_result.linksToFollow) # type: ignore

        item["needMoreInfo"] = need_more_info
        item["linksToFollow"] = suggested_links

        normalized_candidates: list[str] = []
        seen_links: set[str] = set()
        base_url = get_base_url(response.url)
        page_path = get_page_path(response.url)
        start_domain = normalized_domain(response.url)

        for suggested_link in suggested_links:
            candidate = normalize_same_site_candidate(normalize_link(suggested_link, base_url, page_path))
            if normalized_domain(candidate) != start_domain:
                continue
            if candidate not in available_links:
                continue
            if candidate in seen_links:
                continue
            seen_links.add(candidate)
            normalized_candidates.append(candidate)

        if need_more_info:
            for candidate_url in normalized_candidates[:FOLLOW_LINK_LIMIT]:
                candidate_response = fetch_follow_up_page(candidate_url)
                if candidate_response is None:
                    continue
                candidate_text = extract_page_text(candidate_response.text)
                if not candidate_text:
                    continue
                follow_up_result = run_structured_llm(
                    user_content=f"""Page text:
{candidate_text}""",
                    system_prompt=FOLLOW_UP_SYSTEM_PROMPT,
                    output_model=FollowUpExtraction,
                )
                merged_ice_breakers.extend(coerce_string_list(follow_up_result.iceBreakerInfo)) # type: ignore

        item["iceBreakerInfo"] = coerce_string_list(merged_ice_breakers)
    finally:
        server_process.terminate()
        try:
            server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_process.kill()
            server_process.wait(timeout=5)
