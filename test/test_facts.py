import pandas as pd

from etl.dimensions import build_dimensions
from etl.facts import build_facts
from .test_dimensions import _sample_bronze_df


def test_build_facts_basic():
    df_bronze = _sample_bronze_df()

    # Primero construimos dimensiones y mapeos
    dims, maps = build_dimensions(df_bronze)

    # Luego construimos hechos
    fact_orders, fact_order_items = build_facts(df_bronze, maps)

    # ========== fact_orders ==========
    # Debe haber 2 pedidos únicos: O-001, O-002
    assert fact_orders["order_id"].nunique() == 2

    # order_key debe comenzar en 1 y ser secuencial
    assert fact_orders["order_key"].min() == 1
    assert fact_orders["order_key"].max() == len(fact_orders)

    # FKs no deben ser nulos
    for col in ["customer_key", "order_date_key", "ship_date_key",
                "ship_mode_key", "geography_key"]:
        assert fact_orders[col].isna().sum() == 0

    # ========== fact_order_items ==========
    # Debe haber tantas filas como lineas en bronce (3 en el ejemplo)
    assert len(fact_order_items) == len(df_bronze)

    # order_item_key debe ser secuencial
    assert fact_order_items["order_item_key"].min() == 1
    assert fact_order_items["order_item_key"].max() == len(fact_order_items)

    # FKs order_key y product_key no nulos
    assert fact_order_items["order_key"].isna().sum() == 0
    assert fact_order_items["product_key"].isna().sum() == 0

    # Métricas coherentes
    assert fact_order_items["quantity"].sum() == df_bronze["quantity"].sum()
    assert fact_order_items["sales"].sum() == df_bronze["sales"].sum()
