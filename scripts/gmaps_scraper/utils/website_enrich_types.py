from dataclasses import dataclass


@dataclass
class PageResponse:
    text: str
    url: str
    headers: dict[str, str]
