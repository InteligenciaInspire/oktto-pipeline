from __future__ import annotations

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_retry_adapter(max_retries: int, backoff_factor: float) -> HTTPAdapter:
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST", "PATCH", "DELETE"),
        raise_on_status=False,
    )
    return HTTPAdapter(max_retries=retry)
