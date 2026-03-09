from __future__ import annotations

from typing import Any, Dict, Generator, Iterable, Optional

import requests

from src.config import OkttoSettings
from src.utils.logger import get_logger
from src.utils.retry import build_retry_adapter


class OkttoClientError(RuntimeError):
    pass


class OkttoClient:
    def __init__(self, settings: OkttoSettings) -> None:
        self.settings = settings
        self.logger = get_logger(self.__class__.__name__)
        self.session = requests.Session()
        adapter = build_retry_adapter(settings.max_retries, settings.backoff_factor)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.headers = {
            "Authorization": f"Bearer {settings.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        return f"{self.settings.base_url.rstrip('/')}/{path.lstrip('/')}"

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = self._url(path)
        self.logger.debug("Request %s %s params=%s", method, url, params)

        response = self.session.request(
            method=method.upper(),
            url=url,
            headers=self.headers,
            params=params,
            json=json,
            timeout=self.settings.timeout_seconds,
        )

        if response.status_code >= 400:
            message = (
                f"Oktto API error {response.status_code} in {method.upper()} {path}: "
                f"{response.text[:500]}"
            )
            self.logger.error(message)
            raise OkttoClientError(message)

        return response

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("GET", path, params=params).json()

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", path, json=payload).json()

    def patch(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("PATCH", path, json=payload).json()

    def delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("DELETE", path, params=params).json()

    @staticmethod
    def _extract_items(payload: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("data", "items", "results"):
                if key in payload and isinstance(payload[key], list):
                    return payload[key]
        return []

    def get_paginated(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        page_size_param: str = "per_page",
    ) -> Generator[Dict[str, Any], None, None]:
        current_page = 1
        query = dict(params or {})
        query.setdefault(page_size_param, self.settings.page_size)

        while True:
            query[page_param] = current_page
            payload = self.get(path, params=query)
            items = list(self._extract_items(payload))

            if not items:
                break

            for item in items:
                yield item

            if len(items) < int(query[page_size_param]):
                break

            current_page += 1
