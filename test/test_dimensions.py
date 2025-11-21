import pandas as pd

from utils.build_dimensions import build_dimensions

# Crea un DataFrame de ejemplo similar a lo que llega de DB bronze
def _sample_bronze_df():
    data = {
        "row_id": [1, 2, 3],
        "order_id": ["O-001", "O-001", "O-002"],
        "order_date": pd.to_datetime(["2020-01-01", "2020-01-01", "2020-01-05"]),
        "ship_date": pd.to_datetime(["2020-01-03", "2020-01-03", "2020-01-06"]),
        "ship_mode": ["Standard Class", "Standard Class", "Second Class"],
        "customer_id": ["C-001", "C-001", "C-002"],
        "customer_name": ["Ana", "Ana", "David"],
        "segment": ["Consumer", "Consumer", "Corporate"],
        "country": ["USA", "USA", "USA"],
        "state": ["NY", "NY", "MA"],
        "city": ["New York", "New York", "Boston"],
        "postal_code": [10001, 10001, 2110],
        "region": ["East", "East", "East"],
        "product_id": ["P-001", "P-002", "P-001"],
        "product_name": ["Silla", "Mesa", "Silla"],
        "category": ["Furniture", "Furniture", "Furniture"],
        "subcategory": ["Chairs", "Tables", "Chairs"],
        "sales": [100.0, 200.0, 400.0],
        "quantity": [1, 2, 4],
        "discount": [0.0, 0.1, 0.0],
        "profit": [10.0, 20.0, 40.0],
        "source_file": ["test.csv", "test.csv", "test.csv"],
    }
    return pd.DataFrame(data)


def test_build_dimensions_basic():
    df_bronze = _sample_bronze_df()

    dims, maps = build_dimensions(df_bronze)

    # Dimensiones creadas
    assert "dim_customer" in dims
    assert "dim_geography" in dims
    assert "dim_product" in dims
    assert "dim_ship_mode" in dims
    assert "dim_date" in dims

    # Clientes: debe haber 2 (C-001, C-002)
    dim_customer = dims["dim_customer"]
    assert dim_customer["customer_id"].nunique() == 2
    assert "customer_key" in dim_customer.columns

    # Productos: debe haber 2 (P-001, P-002)
    dim_product = dims["dim_product"]
    assert dim_product["product_id"].nunique() == 2
    assert "product_key" in dim_product.columns

    # Ship modes únicos
    dim_ship_mode = dims["dim_ship_mode"]
    assert set(dim_ship_mode["ship_mode"]) == {"Standard Class", "Second Class"}

    # Fechas: deben incluir 4 fechas distintas (2 order + 2 ship)
    dim_date = dims["dim_date"]
    assert len(dim_date) == 4
    assert "date_key" in dim_date.columns

    # Maps devueltos
    assert "customer" in maps
    assert "product" in maps
    assert "geography" in maps
    assert "ship_mode" in maps
    assert "date" in maps
