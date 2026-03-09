from __future__ import annotations

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient
from src.jobs import sync_dimensions, sync_leads, sync_sales
from src.transform.business_views import build_vw_comercial_resumo
from src.load.sheets_loader import load_dataframe_to_sheet
from src.extract.leads import fetch_leads
from src.extract.sales import fetch_sales
from src.transform.normalize_leads import normalize_leads
from src.transform.normalize_sales import normalize_sales
from src.utils.logger import get_logger


logger = get_logger(__name__)


def run(oktto_client: OkttoClient, sheets_client: SheetsClient) -> None:
    logger.info("Sync full started")

    sync_dimensions.run(oktto_client, sheets_client)
    sync_leads.run(oktto_client, sheets_client)
    sync_sales.run(oktto_client, sheets_client)

    leads_df = normalize_leads(fetch_leads(oktto_client))
    sales_df = normalize_sales(fetch_sales(oktto_client))
    vw_comercial = build_vw_comercial_resumo(leads_df, sales_df)

    load_dataframe_to_sheet(sheets_client, "painel_comercial", vw_comercial)

    logger.info("Sync full finished")
