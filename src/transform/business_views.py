from __future__ import annotations

import pandas as pd


def build_vw_comercial_resumo(leads_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
    if leads_df.empty and sales_df.empty:
        return pd.DataFrame()

    leads_total = len(leads_df)
    sales_total = len(sales_df)

    total_sales_value = 0.0
    candidate_columns = ["value", "amount", "total", "sale_value"]
    for col in candidate_columns:
        if col in sales_df.columns:
            total_sales_value = pd.to_numeric(sales_df[col], errors="coerce").fillna(0).sum()
            break

    return pd.DataFrame(
        [
            {
                "leads_total": leads_total,
                "sales_total": sales_total,
                "sales_value_total": float(total_sales_value),
            }
        ]
    )
