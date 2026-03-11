from __future__ import annotations

import logging
import os
from typing import Any

from bs4 import BeautifulSoup
from google import genai
from google.genai import types
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv

from ..website_enrich_types import PageResponse

logger = logging.getLogger(__name__)


GEMINI_MODEL = "gemini-3.1-flash-lite-preview"
MAX_PAGE_TEXT_CHARS = 16000
LLM_VALIDATION_RETRIES = 3

_client: genai.Client | None = None

load_dotenv()


def get_client() -> genai.Client:
    global _client
    if _client is None:
        _client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    return _client


class Extraction(BaseModel):
    iceBreakerInfo: list[str]


ROOT_SYSTEM_PROMPT = """You are a senior sales engineer reviewing a business website homepage for cold-email personalization.

Your job is to extract outreach-relevant facts that can turn into strong icebreakers.

Look for:
- unusually long tenure (20+ years)
- named awards or recognitions
- non-obvious certifications
- specific community partnerships
- quantified proof points (e.g. "500+ installs", "40% waste reduction")
- rare specialties that set the company apart

Differentiation Test:
Only include facts that would NOT appear on the majority of competitor websites in the same industry. If the statement could plausibly appear on most competitor websites, treat it as generic marketing copy and exclude it.

Icebreaker Framing:
Each icebreaker must start with one of the following prefixes and match the correct context.

"I noticed that"
Use for a unique differentiator such as an uncommon specialty, partnership, or positioning.

"I was impressed to see that"
Use for distinct achievements such as awards, recognitions, scale, or strong quantified metrics.

"I was curious when I saw that"
Use for interesting differentiators or unusual focus areas that stand out.

"I see you're ... — ever tried ...?"
Use only when the text explicitly mentions hobbies, personal interests, or lifestyle activities. Do not invent hobbies.

Rules:
- ONLY include facts with a direct or near-verbatim basis in the provided page text.
- If you are not certain the text says it, do not include it.
- Do NOT infer or calculate information.
- Return an EMPTY list if the page contains only generic marketing copy.
- Return at most 3 icebreakers ranked by strength.
- Each entry must read as a complete natural sentence.
- NEVER include: licensing or insurance, free quotes, guarantees, service area lists, customer review mentions, or experience claims under 15 years.

Weak Signal Rejection:
If the page contains only generic marketing language, service descriptions, location lists, licensing statements, or vague quality claims without numbers, return an EMPTY list.

GOOD examples:
- I noticed that the team specializes in historic home restorations.
- I was impressed to see that the company has completed over 1,200 installations across four states.
- I was curious when I saw that the team focuses exclusively on energy-efficient retrofit projects.

BAD examples:
- I noticed that the company is licensed and insured.
- I noticed that the team serves the local area.
- I noticed that the company offers free quotes.
- I noticed that the company is committed to customer satisfaction.
"""


def extract_page_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = " ".join(soup.stripped_strings)
    return text[:MAX_PAGE_TEXT_CHARS]


def run_structured_llm(
    user_content: str,
    system_prompt: str,
    output_model: type[BaseModel],
) -> BaseModel:
    last_error: Exception | None = None
    client = get_client()

    for attempt in range(LLM_VALIDATION_RETRIES):
        try:
            logger.debug("LLM extract: attempt %d/%d with model %s", attempt + 1, LLM_VALIDATION_RETRIES, GEMINI_MODEL)
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=user_content,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    response_mime_type="application/json",
                    response_schema=output_model,
                    temperature=0,
                ),
            )
            content = response.text
            if not isinstance(content, str) or not content.strip():
                raise ValueError("Empty structured output from model.")
            return output_model.model_validate_json(content)
        except (ValidationError, ValueError) as exc:
            logger.warning("LLM extract: attempt %d failed: %s", attempt + 1, exc)
            last_error = exc

    assert last_error is not None
    raise last_error


def llm_extract_plugin(item: dict[str, Any], response: PageResponse) -> None:
    logger.info("LLM extract: extracting ice breakers from %s", response.url)
    root_text = extract_page_text(response.text)
    if not root_text:
        logger.warning("LLM extract: no page text found for %s, skipping", response.url)
        return

    logger.debug("LLM extract: extracted %d chars of page text", len(root_text))
    root_result = run_structured_llm(
        user_content=root_text,
        system_prompt=ROOT_SYSTEM_PROMPT,
        output_model=Extraction,
    )

    ice_breakers = root_result.iceBreakerInfo  # type: ignore
    logger.info("LLM extract: found %d ice breaker(s) for %s", len(ice_breakers), response.url)
    item["iceBreakerInfo"] = ice_breakers
