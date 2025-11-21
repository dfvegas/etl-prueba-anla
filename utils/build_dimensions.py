import logging
import pandas as pd

# Contruye dataframes de las tablas de dimensiones a partir del dataframe de bronce
def build_dimensions(df_bronze: pd.DataFrame):
    dims = {}
    maps = {}

    # ------------------------------
    # Dim Customer
    # ------------------------------
    dim_customer = (
        df_bronze[["customer_id", "customer_name", "segment"]]
        .drop_duplicates()
        .sort_values("customer_id")
        .reset_index(drop=True)
    )
    dim_customer.insert(0, "customer_key", range(1, len(dim_customer) + 1))
    dims["dim_customer"] = dim_customer
    maps["customer"] = dim_customer[["customer_id", "customer_key"]]

    # ------------------------------
    # Dim Geography
    # ------------------------------
    geo_cols = ["country", "state", "city", "postal_code", "region"]
    dim_geography = (
        df_bronze[geo_cols]
        .drop_duplicates()
        .sort_values(geo_cols)
        .reset_index(drop=True)
    )
    dim_geography.insert(0, "geography_key", range(1, len(dim_geography) + 1))
    dims["dim_geography"] = dim_geography
    maps["geography"] = dim_geography[geo_cols + ["geography_key"]]

    # ------------------------------
    # Dim Product
    # ------------------------------
    prod_cols = ["product_id", "product_name", "category", "subcategory"]

    raw_products = (
        df_bronze[prod_cols]
        .dropna(subset=["product_id"])
        .copy()
    )

    raw_products = raw_products.sort_values(prod_cols).reset_index(drop=True)

    dim_product = (
        raw_products
        .drop_duplicates(subset=["product_id"])
        .reset_index(drop=True)
    )

    n_distinct_ids = raw_products["product_id"].nunique()
    n_rows_full = raw_products.drop_duplicates().shape[0]

    if n_rows_full > n_distinct_ids:
        logging.warning(
            "Se detectaron productos con el mismo product_id pero distinta descripción: "
            "%d combinaciones vs %d product_id únicos. "
            "Se tomó la primera ocurrencia por product_id.",
            n_rows_full,
            n_distinct_ids,
        )

    dim_product.insert(0, "product_key", range(1, len(dim_product) + 1))
    dims["dim_product"] = dim_product
    maps["product"] = dim_product[["product_id", "product_key"]]


    # ------------------------------
    # Dim Ship Mode
    # ------------------------------
    dim_ship_mode = (
        df_bronze[["ship_mode"]]
        .drop_duplicates()
        .sort_values("ship_mode")
        .reset_index(drop=True)
    )
    dim_ship_mode.insert(0, "ship_mode_key", range(1, len(dim_ship_mode) + 1))
    dims["dim_ship_mode"] = dim_ship_mode
    maps["ship_mode"] = dim_ship_mode[["ship_mode", "ship_mode_key"]]

    # ------------------------------
    # Dim Date (Order + Ship dates)
    # ------------------------------
    date_series = pd.concat(
        [df_bronze["order_date"], df_bronze["ship_date"]]
    ).dropna().drop_duplicates()

    dim_date = (
        pd.DataFrame({"full_date": date_series})
        .drop_duplicates()
        .sort_values("full_date")
        .reset_index(drop=True)
    )

    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["quarter"] = dim_date["full_date"].dt.quarter

    dim_date = dim_date[
        ["date_key", "full_date", "year", "month", "day", "quarter"]
    ]

    dims["dim_date"] = dim_date
    maps["date"] = dim_date[["full_date", "date_key"]]

    logging.info(
        "Dimensiones construidas: customers=%d, geography=%d, products=%d, "
        "ship_modes=%d, dates=%d",
        len(dims["dim_customer"]),
        len(dims["dim_geography"]),
        len(dims["dim_product"]),
        len(dims["dim_ship_mode"]),
        len(dims["dim_date"]),
    )

    return dims, maps