from __future__ import annotations

from typing import Dict, List

import pandas as pd


def normalize_sales(sales: List[Dict]) -> pd.DataFrame:
    if not sales:
        return pd.DataFrame()
    return pd.json_normalize(sales, sep="__")
