from __future__ import annotations

from unittest.mock import Mock

from src.clients.oktto_client import OkttoClient
from src.config import OkttoSettings


def test_get_paginated_reads_two_pages() -> None:
    settings = OkttoSettings(token="abc", page_size=2)
    client = OkttoClient(settings)

    client.get = Mock(
        side_effect=[
            {"data": [{"id": 1}, {"id": 2}]},
            {"data": [{"id": 3}]},
        ]
    )

    items = list(client.get_paginated("/leads"))

    assert [x["id"] for x in items] == [1, 2, 3]
