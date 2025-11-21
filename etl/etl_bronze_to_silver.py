import logging

import pandas as pd
from sqlalchemy import text

from .db import get_bronze_engine
from .db import get_silver_engine
from .dimensions import build_dimensions
from .facts import build_facts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Leer todos los registros de la capa bronce (tabla orders_bronze)
def extract_bronze_orders() -> pd.DataFrame:
    engine = get_bronze_engine()
    query = "SELECT * FROM orders_bronze;"
    logging.info("Extrayendo datos de orders_bronze...")
    df = pd.read_sql(query, con=engine, parse_dates=["order_date", "ship_date"])
    logging.info(f"Se extrajeron {len(df)} filas de Bronce.")
    return df

# Borrar datos de las tablas de DB silver antes de recargar
def truncate_silver_tables(engine):
    logging.info("Truncando tablas de Silver...")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    fact_order_items,
                    fact_orders,
                    dim_product,
                    dim_customer,
                    dim_geography,
                    dim_ship_mode,
                    dim_date
                RESTART IDENTITY CASCADE;
                """
            )
        )
    logging.info("Truncate completado.")

# Cargar dataframe en tabla de DB silver
def load_dataframe(df: pd.DataFrame, table_name: str, engine):
    logging.info("Cargando %d filas en %s...", len(df), table_name)
    with engine.begin() as conn:
        df.to_sql(
            table_name,
            con=conn,
            if_exists="append",
            index=False,
        )
    logging.info("Carga en %s completada.", table_name)


def main():
    try:
        df_bronze = extract_bronze_orders()
        if df_bronze.empty:
            logging.warning("No hay datos en Bronce. Abortando ETL Silver.")
            return

        dims, maps = build_dimensions(df_bronze)
        fact_orders, fact_order_items = build_facts(df_bronze, maps)

        silver_engine = get_silver_engine()
        truncate_silver_tables(silver_engine)

        # Cargar dimensiones
        load_dataframe(dims["dim_customer"], "dim_customer", silver_engine)
        load_dataframe(dims["dim_geography"], "dim_geography", silver_engine)
        load_dataframe(dims["dim_product"], "dim_product", silver_engine)
        load_dataframe(dims["dim_ship_mode"], "dim_ship_mode", silver_engine)
        load_dataframe(dims["dim_date"], "dim_date", silver_engine)

        # Cargar hechos
        load_dataframe(fact_orders, "fact_orders", silver_engine)
        load_dataframe(fact_order_items, "fact_order_items", silver_engine)

        logging.info("ETL Bronce -> Silver completada exitosamente.")

    except Exception as e:
        logging.exception("Error ejecutando ETL Bronce -> Silver: %s", e)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
