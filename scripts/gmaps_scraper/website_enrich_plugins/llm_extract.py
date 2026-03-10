from __future__ import annotations
from typing import Any

from bs4 import BeautifulSoup
import ollama
from pydantic import BaseModel, ValidationError
import requests


OLLAMA_MODEL = "lfm2:24b"
MAX_PAGE_TEXT_CHARS = 16000
LLM_VALIDATION_RETRIES = 3


class Extraction(BaseModel):
    iceBreakerInfo: list[str]


ROOT_SYSTEM_PROMPT = """You are a senior sales engineer reviewing a business website homepage for cold-email personalization.

Your job is to extract outreach-relevant facts that can turn into strong ice breakers. Look for founder/operator tenure, local roots, awards, certifications, community work, notable specialties, unusual proof points, and specific differentiators that signal credibility or uniqueness.

Rules:
- Use only facts directly supported by the provided page text.
- Write `iceBreakerInfo` as short, factual, quote-ready strings.
- Ignore vague claims, broad slogans, and generic SEO copy.
- Avoid generic points that signal lazy research, such as "Has high customer ratings" or "Offers customized solutions"
- Rather, focus on unique identifiers that stand out, such as "Has been serving the Tampa area for 20 years"
- Return valid JSON matching the schema exactly."""


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


def llm_extract_plugin(item: dict[str, Any], response: requests.Response) -> None:
    root_text = extract_page_text(response.text)
    if not root_text:
        return

    root_result = run_structured_llm(
        user_content=root_text,
        system_prompt=ROOT_SYSTEM_PROMPT,
        output_model=Extraction,
    )

    item["iceBreakerInfo"] = root_result.iceBreakerInfo # type: ignore
