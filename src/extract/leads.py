from __future__ import annotations

from typing import Dict, List

from src.clients.oktto_client import OkttoClient


def fetch_leads(client: OkttoClient) -> List[Dict]:
    return list(client.get_paginated("/leads"))


def fetch_lead_sales(client: OkttoClient, lead_id_or_external_id: str) -> List[Dict]:
    path = f"/leads/{lead_id_or_external_id}/sales"
    return list(client.get_paginated(path))


def fetch_lead_tasks(client: OkttoClient, lead_id_or_external_id: str) -> List[Dict]:
    path = f"/leads/{lead_id_or_external_id}/tasks"
    return list(client.get_paginated(path))
