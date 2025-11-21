import argparse
import logging
import os

import pandas as pd

from config.settings import get_bronze_engine
from utils.validators import validate_and_clean
from utils.constants import COLUMN_MAPPING, TABLE_NAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def read_csv_to_dataframe(csv_path: str) -> pd.DataFrame:
    logging.info(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path, encoding="latin1")
    return df


def transform_for_bronze(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    logging.info("Aplicando validaciones y limpieza mínima...")
    df = validate_and_clean(df)

    df = df.rename(columns=COLUMN_MAPPING)

    df["source_file"] = os.path.basename(source_file)

    ordered_cols = list(COLUMN_MAPPING.values()) + ["source_file"]
    df = df[ordered_cols]

    return df


def load_to_bronze(df: pd.DataFrame):
    engine = get_bronze_engine()

    logging.info(f"Insertando {len(df)} filas en {TABLE_NAME}...")
    
    # Insert bulk con to_sql (append)
    with engine.begin() as conn:
        df.to_sql(
            TABLE_NAME,
            con=conn,
            if_exists="append",
            index=False,
        )
    logging.info("Carga completada.")


def main():
    parser = argparse.ArgumentParser(description="ETL CSV -> Bronze")
    parser.add_argument(
        "csv_path",
        help="Ruta del archivo CSV de origen",
    )
    args = parser.parse_args()

    csv_path = args.csv_path

    if not os.path.isfile(csv_path):
        logging.error(f"El archivo {csv_path} no existe.")
        raise SystemExit(1)

    df_raw = read_csv_to_dataframe(csv_path)
    df_bronze = transform_for_bronze(df_raw, source_file=csv_path)

    if df_bronze.empty:
        logging.warning("No hay filas para cargar después de la limpieza.")
        return

    load_to_bronze(df_bronze)


if __name__ == "__main__":
    main()
