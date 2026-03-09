from __future__ import annotations

from src.transform.normalize_sales import normalize_sales


def test_normalize_sales_flattens_nested_objects() -> None:
    sales = [{"id": 1, "product": {"id": 99, "name": "Curso"}}]

    df = normalize_sales(sales)

    assert "product__id" in df.columns
    assert "product__name" in df.columns
    assert int(df.iloc[0]["product__id"]) == 99
