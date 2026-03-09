from __future__ import annotations

import argparse

from src.clients.oktto_client import OkttoClient
from src.clients.sheets_client import SheetsClient
from src.config import get_settings
from src.jobs import sync_dimensions, sync_full, sync_leads, sync_sales
from src.utils.logger import setup_logger


JOBS = {
    "sync_dimensions": sync_dimensions.run,
    "sync_leads": sync_leads.run,
    "sync_sales": sync_sales.run,
    "sync_full": sync_full.run,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Oktto pipeline jobs")
    parser.add_argument(
        "--job",
        choices=["sync_dimensions", "sync_leads", "sync_sales", "sync_full"],
        required=True,
        help="Job to run",
    )
    return parser.parse_args()


def run_job(job_name: str) -> None:
    if job_name not in JOBS:
        raise ValueError(f"Invalid job: {job_name}")

    settings = get_settings()
    setup_logger(settings.log_level)

    oktto_client = OkttoClient(settings.oktto)
    sheets_client = SheetsClient(settings.sheets)
    run_job_with_clients(job_name, oktto_client, sheets_client)


def run_job_with_clients(job_name: str, oktto_client: OkttoClient, sheets_client: SheetsClient) -> None:
    if job_name not in JOBS:
        raise ValueError(f"Invalid job: {job_name}")

    JOBS[job_name](oktto_client, sheets_client)


def main() -> None:
    args = parse_args()
    run_job(args.job)


if __name__ == "__main__":
    main()
