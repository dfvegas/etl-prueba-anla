# 🧩 Proyecto ETL: Kaggle → Bronze → Silver (PostgreSQL)

Este proyecto implementa un pipeline ETL completo utilizando Python, Pandas, SQLAlchemy y PostgreSQL. Se trabaja con el dataset de tipo **Superstore** de Kaggle , que primero entra a una capa Bronze (datos crudos) y luego se transforma a una capa Silver (modelo relacional normalizado).

Incluye:

- Descarga automática desde Kaggle.
- Limpieza y estandarización de datos (Bronze).
- Normalización completa a un modelo dimensional (Silver).
- Scripts SQL para la creación del modelo.
- Tests automatizados con pytest.

---

# 1. Descripción de la solución

El pipeline ETL sigue una arquitectura clásica de **Bronze → Silver**.

## Capa Bronze
Contiene los datos crudos provenientes del CSV descargado desde Kaggle.  
Se aplica limpieza mínima para asegurar consistencia:

- Tipos de datos correctos.
- Normalización de nombres de columnas.
- Conversión de fechas.
- Marcado del archivo fuente y timestamp de ingesta.

## Capa Silver
Modelo dimensional normalizado compuesto por:

- `dim_customer`
- `dim_geography`
- `dim_product`
- `dim_ship_mode`
- `dim_date`
- `fact_orders`
- `fact_order_items`

Características principales:

- Llaves surrogate en todas las dimensiones.
- Relaciones por claves foráneas.
- Separación clara entre entidades del negocio.
- Fechas normalizadas vía tabla `dim_date`.
- Hechos con granularidad adecuada:
  - Nivel pedido (`fact_orders`)
  - Nivel ítem (`fact_order_items`)

---

# 2. Supuestos tomados

1. El dataset Superstore no tiene duplicados críticos en sus llaves naturales.
2. `product_id` identifica un producto, incluso si hay pequeñas inconsistencias textuales.
3. Todos los pedidos tienen cliente, fecha de orden y fecha de envío válidas.
4. Las fechas del CSV pueden convertirse limpiamente a tipo `DATE`.
5. Las bases de datos Bronze y Silver existen en la misma instancia de PostgreSQL.

---

# 3. Preparación del entorno

## 3.1 Crear entorno virtual
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

## 3.2 Crear entorno virtual
```bash
pip install -r requirements.txt
```
## 3.2 Configurar variables de entorno
Crear archivo `.env`   
```bash
PGUSER=postgres
PGPASSWORD=admin
PGHOST=localhost
PGPORT=5432

PGDATABASE_BRONZE=bronze
BRONZE_DATABASE_URL=silver
```
---

# 4. Creación de bases de datos

Ejecutar en psql:
```bash
CREATE DATABASE bronze;
CREATE DATABASE silver;
```

---

# 5. Ejecutar scripts SQL

Ejecutar en psql:
```bash
psql -U postgres -d bronze -f sql/bronze_schema.sql
psql -U postgres -d silver -f sql/silver_schema.sql
```
---

# 6. Ejecución del pipeline ETL

## 6.1 Descarga de la base de datos
Ejecutar el archivo `download_dataset.py` para descargar el dataset.
```bash
python download_dataset.py
```
Esto devuelve una ruta que usaremos en el siguiente paso. El CSV quedará almacenado en la caché local de kagglehub, por ejemplo:
```bash
C:\Users\<usuario>\.cache\kagglehub\datasets\vivek468\superstore-dataset-final\versions\1\Superstore.csv
```


## 6.2 ETL CSV → Bronze
Ejecutar ETL usando la ruta dónde se guarda el archivo descargado desde Kagglehub
```bash
python -m etl.etl_csv_to_bronze "C:\Users\<usuario>\.cache\kagglehub\datasets\vivek468\superstore-dataset-final\versions\1\Superstore.csv"
```

## 6.3 ETL Bronze → Silver
```bash
python -m etl.etl_bronze_to_silver
```

---

# 7. Tests automatizados

## 7.1 Ejecutar tests
```bash
pytest -vv
```
## 7.2 ¿Qué validan los tests?
- Que `test_etl_csv_to_bronze` valide limpieza y mapeo del CSV.
- Que `test_dimensions` valide la creación correcta de las dimensiones.
- Que `test_facts` valide que los hechos se construyen correctamente.
Los tests usan dataframes pequeños y controlados (unit tests).