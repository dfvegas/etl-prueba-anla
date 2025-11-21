import logging
import pandas as pd

# Contruye dataframes de las tablas de hechos (fact_orders y fact_order_items) a partir del dataframe de bronce y las dimesiones
def build_facts(df_bronze: pd.DataFrame, maps):
    geo_cols = ["country", "state", "city", "postal_code", "region"]

    # ------------------------------
    # fact_orders (nivel pedido)
    # ------------------------------
    orders = (
        df_bronze[
            ["order_id", "customer_id", "order_date", "ship_date", "ship_mode"]
            + geo_cols
        ]
        .drop_duplicates(subset=["order_id"])
        .reset_index(drop=True)
    )

    orders = orders.merge(
        maps["customer"],
        on="customer_id",
        how="left",
    )

    orders = orders.merge(
        maps["ship_mode"],
        on="ship_mode",
        how="left",
    )

    orders = orders.merge(
        maps["geography"],
        on=geo_cols,
        how="left",
    )

    orders = orders.merge(
        maps["date"].rename(columns={"full_date": "order_date", "date_key": "order_date_key"}),
        on="order_date",
        how="left",
    )

    orders = orders.merge(
        maps["date"].rename(columns={"full_date": "ship_date", "date_key": "ship_date_key"}),
        on="ship_date",
        how="left",
    )

    orders.insert(0, "order_key", range(1, len(orders) + 1))

    fact_orders = orders[
        [
            "order_key",
            "order_id",
            "customer_key",
            "order_date_key",
            "ship_date_key",
            "ship_mode_key",
            "geography_key",
        ]
    ]

    # ------------------------------
    # fact_order_items (nivel ítem)
    # ------------------------------
    items = df_bronze[
        [
            "row_id",
            "order_id",
            "product_id",
            "quantity",
            "sales",
            "discount",
            "profit",
        ]
    ].copy()

    items = items.merge(
        fact_orders[["order_key", "order_id"]],
        on="order_id",
        how="left",
    )

    items = items.merge(
        maps["product"],
        on="product_id",
        how="left",
    )

    items.insert(0, "order_item_key", range(1, len(items) + 1))

    fact_order_items = items[
        [
            "order_item_key",
            "order_key",
            "product_key",
            "quantity",
            "sales",
            "discount",
            "profit",
            "row_id",
        ]
    ]

    logging.info(
        "Hechos construidos: fact_orders=%d, fact_order_items=%d",
        len(fact_orders),
        len(fact_order_items),
    )

    return fact_orders, fact_order_items
