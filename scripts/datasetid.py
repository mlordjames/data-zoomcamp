import re
import json
from pathlib import Path
from datetime import datetime, timedelta
import requests

URL = "https://openpaymentsdata.cms.gov/frontend/build/static/js/index.js?ta9low"

HEADERS = {
    "user-agent": "Mozilla/5.0"
}

CACHE_FILE = "dataset_ids_cache.json"
MAX_CACHE_DAYS = 7


def fetch_js() -> str:
    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def extract_general_payment_ids(js_text: str) -> dict[str, str]:
    results = {}

    year_blocks = re.findall(r'PGYR(\d{4}):\[(.*?)\]', js_text, re.DOTALL)

    for year, block in year_blocks:
        match = re.search(r'type:"generalPayments",id:"([^"]+)"', block)
        if match:
            results[year] = match.group(1)

    return results


def _is_cache_fresh(cache_path: Path) -> bool:
    if not cache_path.exists():
        return False

    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=MAX_CACHE_DAYS)


def getdatasetids(cache_dir: Path | str = ".") -> dict[str, str]:
    """
    Returns YEAR -> dataset_id mapping.
    Uses local cache if fresh, otherwise fetches and updates cache.
    """
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / CACHE_FILE

    # Use cache if fresh
    if _is_cache_fresh(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)

    # Fetch fresh
    js_text = fetch_js()
    ids = extract_general_payment_ids(js_text)

    # Save cache
    with open(cache_path, "w") as f:
        json.dump(ids, f, indent=2)

    return ids


if __name__ == "__main__":
    print(getdatasetids())
