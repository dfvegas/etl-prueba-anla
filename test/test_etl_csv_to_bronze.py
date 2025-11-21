import os
import pandas as pd

from etl.etl_csv_to_bronze import transform_for_bronze, COLUMN_MAPPING


# Verifica que transform_for_bronze:
#   - renombre columnas según COLUMN_MAPPING
#   - agregue la columna source_file
#   - respete el orden de columnas esperado

def test_transform_for_bronze_basic(tmp_path):

    # Simulamos un DataFrame como si viniera del CSV original (Kaggle Superstore)
    data = {
        "Row ID": [1, 2],
        "Order ID": ["CA-2016-152156", "CA-2016-152156"],
        "Order Date": ["11/8/2016", "11/8/2016"],
        "Ship Date": ["11/11/2016", "11/11/2016"],
        "Ship Mode": ["Second Class", "Second Class"],
        "Customer ID": ["CG-12520", "CG-12520"],
        "Customer Name": ["Claire Gute", "Claire Gute"],
        "Segment": ["Consumer", "Consumer"],
        "Country": ["United States", "United States"],
        "City": ["Henderson", "Henderson"],
        "State": ["Kentucky", "Kentucky"],
        "Postal Code": [42420, 42420],
        "Region": ["South", "South"],
        "Product ID": ["FUR-BO-10001798", "FUR-CH-10000454"],
        "Category": ["Furniture", "Furniture"],
        "Sub-Category": ["Bookcases", "Chairs"],
        "Product Name": ["Bush Somerset Collection Bookcase",
                         "Hon Deluxe Fabric Upholstered Stacking Chairs"],
        "Sales": [261.96, 731.94],
        "Quantity": [2, 3],
        "Discount": [0.0, 0.0],
        "Profit": [41.9136, 219.582],
    }
    df_raw = pd.DataFrame(data)

    fake_csv_path = os.path.join(str(tmp_path), "Superstore.csv")

    df_bronze = transform_for_bronze(df_raw, source_file=fake_csv_path)

    # Columnas esperadas: las de COLUMN_MAPPING + source_file
    expected_cols = list(COLUMN_MAPPING.values()) + ["source_file"]
    assert list(df_bronze.columns) == expected_cols

    # La columna source_file debe contener solo el nombre del archivo
    assert set(df_bronze["source_file"].unique()) == {"Superstore.csv"}

    # Algunas comprobaciones de valores renombrados
    assert df_bronze.loc[0, "row_id"] == 1
    assert df_bronze.loc[0, "order_id"] == "CA-2016-152156"
    assert df_bronze.loc[0, "customer_id"] == "CG-12520"
    assert df_bronze.loc[0, "sales"] == 261.96
    assert df_bronze.loc[1, "subcategory"] == "Chairs"
