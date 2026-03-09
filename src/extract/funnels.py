from __future__ import annotations

from typing import Dict, List

from src.clients.oktto_client import OkttoClient


def fetch_funnels(client: OkttoClient) -> List[Dict]:
    return list(client.get_paginated("/funnels"))


def fetch_stages_by_funnel(client: OkttoClient, funnel_id: str) -> List[Dict]:
    return list(client.get_paginated(f"/funnels/{funnel_id}/stages"))
