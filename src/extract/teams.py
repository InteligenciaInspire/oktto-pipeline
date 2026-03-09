from __future__ import annotations

from typing import Dict, List

from src.clients.oktto_client import OkttoClient


def fetch_teams(client: OkttoClient) -> List[Dict]:
    return list(client.get_paginated("/teams"))
