from __future__ import annotations

from typing import Dict, List

import pandas as pd


def normalize_leads(leads: List[Dict]) -> pd.DataFrame:
    if not leads:
        return pd.DataFrame()
    return pd.json_normalize(leads, sep="__")
