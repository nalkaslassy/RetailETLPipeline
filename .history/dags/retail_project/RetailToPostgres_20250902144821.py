import sys
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ----------------------
# Config (env-overridable)
# ----------------------
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
# default to the values used by the compose Postgres service so local runs work out of the box
PG_DB   = os.getenv("PG_DB",   "retail")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

PG_URL  = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=disable"

# ----------------------
# Args
# ----------------------
if len(sys.argv) < 2:
    print("Usage: spark-submit RetailToPostgres.py <path_to_csv>")
    sys.exit(1)

csv_file = sys.argv[1]
print(f"Processing file: {csv_file}")
print(f"DB target: host={PG_HOST} db={PG_DB} user={PG_USER}")

# ----------------------
# Spark session (container-safe)
# ----------------------
spark = (
    SparkSession.builder
    .appName("Retail ETL with Upsert")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", "/tmp/spark")
    # Always pull the JDBC driver via packages (works inside container)
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

# ----------------------
# Schema
# ----------------------
schema = StructType([
    StructField("InvoiceNo",   StringType(),  True),
    StructField("StockCode",   StringType(),  True),
    StructField("Description", StringType(),  True),
    StructField("Quantity",    IntegerType(), True),
    StructField("InvoiceDate", StringType(),  True),
    StructField("UnitPrice",   DoubleType(),  True),
    StructField("CustomerID",  DoubleType(),  True),
    StructField("Country",     StringType(),  True),
])

# ----------------------
# Load CSV
# ----------------------
df_raw = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(csv_file)
)

# Transform
df = (
    df_raw
    .withColumn(
        "InvoiceDateTS",
        F.coalesce(
            F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
            F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm:ss"),
            F.to_timestamp("InvoiceDate")
        )
    )
    .withColumn("TotalPrice", F.col("Quantity") * F.col("UnitPrice"))
)

common_filters = (
    F.col("CustomerID").isNotNull() &
    (F.col("UnitPrice") > 0) &
    F.col("InvoiceDateTS").isNotNull()
)

sales = (
    df.filter(common_filters & (F.col("Quantity") > 0))
      .withColumn("month", F.to_date(F.date_trunc("month", "InvoiceDateTS")))
)

returns = (
    df.filter(common_filters & (F.col("Quantity") < 0))
      .withColumn("month", F.to_date(F.date_trunc("month", "InvoiceDateTS")))
)

# ----------------------
# Stable IDs
# ----------------------
def add_id(frame, id_col):
    return frame.withColumn(
        id_col,
        F.sha2(
            F.concat_ws(
                "||",
                F.col("InvoiceNo"),
                F.col("StockCode"),
                F.col("InvoiceDateTS").cast("string"),
                F.col("Quantity").cast("string"),
                F.col("UnitPrice").cast("string"),
                F.col("CustomerID").cast("string"),
            ),
            256,
        ),
    )

sales_out   = add_id(sales,   "sale_id")
returns_out = add_id(returns, "return_id")

# ----------------------
# Normalize column names (lowercase to match Postgres defaults)
# ----------------------
def to_lowercase(df_in):
    for c in df_in.columns:
        df_in = df_in.withColumnRenamed(c, c.lower())
    return df_in

sales_out   = to_lowercase(sales_out)
returns_out = to_lowercase(returns_out)

# ----------------------
# DB helpers
# ----------------------
def ensure_schema_and_tables():
    ddl = """
    CREATE SCHEMA IF NOT EXISTS retail;

    CREATE TABLE IF NOT EXISTS retail.sales_clean (
        sale_id       text PRIMARY KEY,
        invoiceno     text,
        stockcode     text,
        description   text,
        quantity      integer,
        invoicedatets timestamp,
        unitprice     double precision,
        customerid    double precision,
        country       text,
        totalprice    double precision,
        month         date
    );

    CREATE TABLE IF NOT EXISTS retail.returns_clean (
        return_id     text PRIMARY KEY,
        invoiceno     text,
        stockcode     text,
        description   text,
        quantity      integer,
        invoicedatets timestamp,
        unitprice     double precision,
        customerid    double precision,
        country       text,
        totalprice    double precision,
        month         date
    );
    """
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(ddl)
    finally:
        conn.close()

def upsert_to_postgres(df_in, table, id_cols):
    """
    Write to a staging table via JDBC, then ON CONFLICT DO NOTHING into target.
    """
    staging_table = f"{table}_staging"

    write_opts = {
        "url": PG_URL,
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver",
        "dbtable": staging_table,
    }

    print(f"Writing data to staging table: {staging_table}")
    (
        df_in.write.format("jdbc")
        .options(**write_opts)
        .mode("overwrite")
        .save()
    )

    # Build column list in a stable order
    columns = [name for name, _dtype in df_in.dtypes]
    col_list = ", ".join(columns)
    conflict_cols = ", ".join(id_cols)

    insert_sql = f"""
    INSERT INTO {table} ({col_list})
    SELECT {col_list} FROM {staging_table}
    ON CONFLICT ({conflict_cols}) DO NOTHING;
    """

    print(f"Running upsert into {table} ...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(insert_sql)
    finally:
        conn.close()
    print(f"Upsert completed for: {table}")

# ----------------------
# Ensure targets exist, then load
# ----------------------
ensure_schema_and_tables()

sales_sel = sales_out.select(
    "sale_id", "invoiceno", "stockcode", "description", "quantity",
    "invoicedatets", "unitprice", "customerid", "country", "totalprice", "month"
)
returns_sel = returns_out.select(
    "return_id", "invoiceno", "stockcode", "description", "quantity",
    "invoicedatets", "unitprice", "customerid", "country", "totalprice", "month"
)

upsert_to_postgres(sales_sel,   "retail.sales_clean",   ["sale_id"])
upsert_to_postgres(returns_sel, "retail.returns_clean", ["return_id"])

spark.stop()
