from __future__ import annotations

import pandas as pd

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient
from src.extract.additional_fields import fetch_additional_fields
from src.extract.funnels import fetch_funnels, fetch_stages_by_funnel
from src.extract.teams import fetch_teams
from src.extract.users import fetch_users
from src.load.sheets_loader import load_dataframe_to_sheet
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run(oktto_client: OkttoClient, sheets_client: SheetsClient) -> None:
    logger.info("Sync dimensions started")

    additional_fields_df = pd.json_normalize(fetch_additional_fields(oktto_client), sep="__")
    users_df = pd.json_normalize(fetch_users(oktto_client), sep="__")
    teams_df = pd.json_normalize(fetch_teams(oktto_client), sep="__")

    funnels = fetch_funnels(oktto_client)
    funnels_df = pd.json_normalize(funnels, sep="__")

    all_stages = []
    for funnel in funnels:
        funnel_id = str(funnel.get("id", ""))
        if not funnel_id:
            continue
        stages = fetch_stages_by_funnel(oktto_client, funnel_id)
        for stage in stages:
            stage["funnel_id"] = funnel_id
        all_stages.extend(stages)

    stages_df = pd.json_normalize(all_stages, sep="__")

    load_dataframe_to_sheet(sheets_client, "raw_additional_fields", additional_fields_df)
    load_dataframe_to_sheet(sheets_client, "raw_users", users_df)
    load_dataframe_to_sheet(sheets_client, "raw_teams", teams_df)
    load_dataframe_to_sheet(sheets_client, "raw_funnels", funnels_df)
    load_dataframe_to_sheet(sheets_client, "raw_stages", stages_df)

    logger.info("Sync dimensions finished")
