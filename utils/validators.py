import pandas as pd

# Elimina filas donde TODAS las columnas estén vacías
def drop_fully_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="all")

# Convierte las columnas de fecha al tipo datatime
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Order Date", "Ship Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

# Convierte columnas numéricas básicas a tipo numérico
def cast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = ["Sales", "Quantity", "Discount", "Profit"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

# Aplica validaciones mínimas definidas para capa bronce
def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_fully_empty_rows(df)
    df = parse_dates(df)
    df = cast_numeric(df)
    return df
