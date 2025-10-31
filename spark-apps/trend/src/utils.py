from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    BooleanType,
    TimestampType,
)
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import psycopg2.extras

# ---- Schema cho dữ liệu K-line từ Binance ----
kline_schema = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("open_time", LongType(), True),
        StructField("close_time", LongType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("quote_volume", DoubleType(), True),
        StructField("number_of_trades", LongType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("taker_buy_volume", DoubleType(), True),
        StructField("taker_buy_quote_volume", DoubleType(), True),
        StructField("event_time", LongType(), True),
    ]
)

conn_str = "postgresql://postgres:your_password@binance-postgres:5432/crypto_db"
def create_table_if_not_exists(create_table_query, table_name: str):
    conn_str = "postgresql://postgres:your_password@binance-postgres:5432/crypto_db"
    
    # Lệnh SQL để tạo bảng nếu nó chưa tồn tại
    sql_create_table = create_table_query
    
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        cur.execute(sql_create_table)
        conn.commit()
        print(f"Đã xác minh/tạo bảng {table_name} thành công.")
    except Exception as e:
        print(f"Lỗi khi tạo bảng {table_name}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def upsert_dataframe_to_postgres(df, table_name: str, key_columns: list, conn_string: str = None, batch_size: int = 1000):
    """
    Upsert a PySpark DataFrame into a PostgreSQL table.
    - df: pyspark.sql.DataFrame
    - table_name: target table name in Postgres
    - key_columns: list of column names that form the conflict target (primary/key)
    - conn_string: optional connection string, defaults to module-level conn_str
    - batch_size: number of rows per execute_values page
    """

    if conn_string is None:
        conn_string = globals().get("conn_str")
    if not conn_string:
        raise ValueError("No connection string provided and module-level conn_str not set.")
    if not key_columns:
        raise ValueError("key_columns must be a non-empty list of column names to perform the upsert.")

    columns = list(df.columns)
    # Collect rows to driver as tuples (beware memory for very large data)
    rows = [tuple(row[c] for c in columns) for row in df.collect()]
    if not rows:
        return 0

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # Build SQL safely with identifiers
        fields_sql = sql.SQL(", ").join([sql.Identifier(c) for c in columns])
        pkey_sql = sql.SQL(", ").join([sql.Identifier(c) for c in key_columns])

        # Prepare update assignments for columns not in key_columns
        non_key_cols = [c for c in columns if c not in key_columns]
        if non_key_cols:
            updates_sql = sql.SQL(", ").join(
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in non_key_cols
            )
            query_comp = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT ({pkey}) DO UPDATE SET {updates}").format(
                table=sql.Identifier(table_name),
                fields=fields_sql,
                pkey=pkey_sql,
                updates=updates_sql,
            )
        else:
            # If all columns are keys, do nothing on conflict
            query_comp = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT ({pkey}) DO NOTHING").format(
                table=sql.Identifier(table_name),
                fields=fields_sql,
                pkey=pkey_sql,
            )

        query_str = query_comp.as_string(conn)
        execute_values(cur, query_str, rows, page_size=batch_size)
        conn.commit()
        return len(rows)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()