import json
import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

TMP_DIR = Path("data/tmp")
FINAL_DIR = Path("data/scraped/gmaps")
RAW_STAGE_SUFFIX = ".raw.json"
ENRICH_STAGE_SUFFIX = ".enriched.json"
OLLAMA_MODEL = "lfm2:24b"
OLLAMA_STARTUP_TIMEOUT_SECONDS = 15.0


def format_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, indent=2, ensure_ascii=False)
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def save_json(path: Path, data: Any, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_json(data, pretty) + "\n", encoding="utf-8")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_items(path: Path) -> list[dict[str, Any]]:
    raw = load_json(path)
    if not isinstance(raw, list):
        raise ValueError(f"Expected JSON array in {path}")
    if any(not isinstance(item, dict) for item in raw):
        raise ValueError(f"Expected array of objects in {path}")
    return raw


def load_mapping(path: Path) -> dict[str, dict[str, Any]]:
    raw = load_json(path)
    if not isinstance(raw, dict):
        raise ValueError(f"Expected JSON object in {path}")
    if any(not isinstance(key, str) for key in raw):
        raise ValueError(f"Expected string keys in {path}")
    if any(not isinstance(value, dict) for value in raw.values()):
        raise ValueError(f"Expected object values in {path}")
    return raw


def create_run_id() -> str:
    return datetime.now().strftime("gmap_%Y-%m-%dT%H:%M:%S")


def raw_stage_path(run_id: str) -> Path:
    return TMP_DIR / f"{run_id}{RAW_STAGE_SUFFIX}"


def enrich_stage_path(run_id: str) -> Path:
    return TMP_DIR / f"{run_id}{ENRICH_STAGE_SUFFIX}"


def final_output_path(run_id: str) -> Path:
    return FINAL_DIR / f"{run_id}.json"


def extract_run_id(path: Path) -> str:
    name = path.name
    if name.endswith(RAW_STAGE_SUFFIX):
        return name[: -len(RAW_STAGE_SUFFIX)]
    if name.endswith(ENRICH_STAGE_SUFFIX):
        return name[: -len(ENRICH_STAGE_SUFFIX)]
    if name.endswith(".json"):
        return name[:-5]
    raise ValueError(f"Unrecognized gmaps staging file name: {path.name}")


def list_raw_stage_files() -> list[Path]:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(TMP_DIR.glob(f"*{RAW_STAGE_SUFFIX}"))


def list_enrich_stage_files() -> list[Path]:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(TMP_DIR.glob(f"*{ENRICH_STAGE_SUFFIX}"))


def start_ollama_server() -> subprocess.Popen[bytes]:
    import ollama

    server_process = subprocess.Popen(
        ["ollama", "serve"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

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
            return server_process
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)

    server_process.terminate()
    try:
        server_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_process.kill()
        server_process.wait(timeout=5)

    if last_error is not None:
        raise last_error
    raise RuntimeError("Timed out waiting for Ollama server to start.")


def stop_ollama_server(server_process: subprocess.Popen[bytes]) -> None:
    server_process.terminate()
    try:
        server_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_process.kill()
        server_process.wait(timeout=5)


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
