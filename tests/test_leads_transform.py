from __future__ import annotations

from src.transform.normalize_leads import normalize_leads


def test_normalize_leads_flattens_nested_objects() -> None:
    leads = [{"id": 1, "user": {"id": 10, "name": "Ana"}}]

    df = normalize_leads(leads)

    assert "user__id" in df.columns
    assert "user__name" in df.columns
    assert int(df.iloc[0]["user__id"]) == 10
