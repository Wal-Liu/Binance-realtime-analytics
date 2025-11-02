import psycopg2
import google.generativeai as genai
import json
from pathlib import Path
from datetime import datetime
import time
import os

# Get the directory where this script is located
SCRIPT_DIR = Path(__file__).parent

# Load configurations
GEMINI_CONFIG_FILE = SCRIPT_DIR / "configs" / "gemini.json"
POSTGRES_CONFIG_FILE = SCRIPT_DIR / "configs" / "postgres.json"

with open(GEMINI_CONFIG_FILE, "r", encoding="utf-8") as f:
    GEMINI_CONFIG = json.load(f)

with open(POSTGRES_CONFIG_FILE, "r", encoding="utf-8") as f:
    POSTGRES_CONFIG = json.load(f)

# Configure Gemini
genai.configure(api_key=GEMINI_CONFIG["api_key"])
model = genai.GenerativeModel(GEMINI_CONFIG["model"])

# List of tables to analyze (based on your workspace)
TABLES_TO_ANALYZE = [
    # "rsi_indicators",
    # "stochastic_indicators",
    # "mfi_14d",
    "bollinger_bands_1m_agg",
    # "donchian_channel",
    "qvb_1m",
    # "crypto_volume",
    # "crypto_ma",
    # "crypto_aroon",
    # "crypto_adx"
]

def get_postgres_connection():
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        database=POSTGRES_CONFIG["database"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"]
    )

def create_suggest_table(conn, table_name):
    """Create suggestion table if not exists."""
    suggest_table_name = f"{table_name}_suggest"
    
    cursor = conn.cursor()
    
    # Create table with generic structure
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {suggest_table_name} (
        id SERIAL PRIMARY KEY,
        analysis_time TIMESTAMP NOT NULL DEFAULT NOW(),
        symbol VARCHAR(20),
        interval VARCHAR(10),
        suggestion TEXT NOT NULL,
        raw_data JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    cursor.execute(create_table_query)
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{suggest_table_name}_analysis_time ON {suggest_table_name}(analysis_time);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{suggest_table_name}_symbol ON {suggest_table_name}(symbol);")
    
    conn.commit()
    cursor.close()
    print(f"Created/verified table: {suggest_table_name}")

def get_latest_data(conn, table_name, limit=60):
    """Get latest 60 rows from table (1 hour of data)."""
    cursor = conn.cursor()
    
    # Try to get timestamp column (different tables have different column names)
    timestamp_columns = [
        "close_time", "timestamp", "window_start", "time_start", 
        "start_time", "alert_time", "analysis_time", "created_at", "window_end"
    ]
    
    # Get table columns
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)
    
    columns = [row[0] for row in cursor.fetchall()]
    
    if not columns:
        print(f"Table {table_name} not found or has no columns")
        cursor.close()
        return None, None
    
    # Find timestamp column
    order_by_column = None
    for ts_col in timestamp_columns:
        if ts_col in columns:
            order_by_column = ts_col
            break
    
    if not order_by_column:
        # Use first column as fallback
        order_by_column = columns[0]
    
    # Query latest data
    query = f"""
        SELECT * FROM {table_name}
        ORDER BY {order_by_column} DESC
        LIMIT {limit};
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    
    return columns, rows

def format_data_for_gemini(table_name, columns, rows):
    """Format data for Gemini analysis."""
    if not rows:
        return None
    
    data_text = f"Phân tích dữ liệu từ bảng {table_name} (60 dòng gần nhất - 1 tiếng gần nhất):\n\n"
    data_text += "Cột: " + ", ".join(columns) + "\n\n"
    
    # Chỉ hiển thị 10 dòng đầu và 10 dòng cuối để không quá dài
    for i, row in enumerate(rows[:10], 1):
        data_text += f"Dòng {i}: "
        row_data = {}
        for col, val in zip(columns, row):
            row_data[col] = str(val)
            data_text += f"{col}={val}, "
        data_text = data_text.rstrip(", ") + "\n"
    
    if len(rows) > 20:
        data_text += f"\n... (Bỏ qua {len(rows) - 20} dòng ở giữa) ...\n\n"
    
    for i, row in enumerate(rows[-10:], len(rows) - 9):
        data_text += f"Dòng {i}: "
        row_data = {}
        for col, val in zip(columns, row):
            row_data[col] = str(val)
            data_text += f"{col}={val}, "
        data_text = data_text.rstrip(", ") + "\n"
    
    return data_text

def get_gemini_suggestion(table_name, data_text):
    """Get suggestion from Gemini."""
    prompt = f"""
Bạn là chuyên gia phân tích thị trường tiền điện tử. Phân tích ngắn gọn dữ liệu sau:

{data_text}

Trả lời NGẮN GỌN (100-150 từ) bao gồm:
1. Xu hướng chính trong 1 giờ qua
2. Tín hiệu kỹ thuật (nếu có)
3. Đề xuất: MUA/BÁN/GIỮ (1 câu lý do)
4. Rủi ro chính (nếu có)

Chỉ viết các điểm chính, không dài dòng.
"""
    
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return f"Lỗi khi gọi Gemini API: {str(e)}"

def insert_suggestion(conn, table_name, columns, rows, suggestion):
    """Insert suggestion into suggest table."""
    suggest_table_name = f"{table_name}_suggest"
    cursor = conn.cursor()
    
    # Extract symbol and interval if available
    symbol = None
    interval = None
    
    if 'symbol' in columns:
        symbol_idx = columns.index('symbol')
        symbol = rows[0][symbol_idx] if rows else None
    
    if 'interval' in columns:
        interval_idx = columns.index('interval')
        interval = rows[0][interval_idx] if rows else None
    
    # Convert rows to JSON
    raw_data = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):
            # Convert datetime to string for JSON serialization
            if isinstance(val, datetime):
                row_dict[col] = val.isoformat()
            else:
                row_dict[col] = str(val)
        raw_data.append(row_dict)
    
    # Insert suggestion
    insert_query = f"""
    INSERT INTO {suggest_table_name} (analysis_time, symbol, interval, suggestion, raw_data)
    VALUES (NOW(), %s, %s, %s, %s);
    """
    
    cursor.execute(insert_query, (symbol, interval, suggestion, json.dumps(raw_data)))
    conn.commit()
    cursor.close()
    print(f"Inserted suggestion into {suggest_table_name}")

def analyze_table(conn, table_name):
    """Analyze a single table."""
    print(f"\n{'='*60}")
    print(f"Analyzing table: {table_name}")
    print(f"{'='*60}")
    
    # Create suggest table
    create_suggest_table(conn, table_name)
    
    # Get latest data
    columns, rows = get_latest_data(conn, table_name)
    
    if not rows:
        print(f"No data found in {table_name}")
        return
    
    print(f"Found {len(rows)} rows")
    
    # Format data for Gemini
    data_text = format_data_for_gemini(table_name, columns, rows)
    
    if not data_text:
        print(f"Could not format data for {table_name}")
        return
    
    # Get Gemini suggestion
    print("Calling Gemini API...")
    suggestion = get_gemini_suggestion(table_name, data_text)
    
    print(f"\nSuggestion:\n{'-'*60}\n{suggestion}\n{'-'*60}\n")
    
    # Insert suggestion
    insert_suggestion(conn, table_name, columns, rows, suggestion)

def main():
    """Main function."""
    print("\n" + "="*60)
    print("Starting Crypto Intelligence Suggestion System")
    print(f"Using Gemini model: {GEMINI_CONFIG['model']}")
    print("="*60)
    
    try:
        conn = get_postgres_connection()
        print("Connected to PostgreSQL database")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return
    
    try:
        for table_name in TABLES_TO_ANALYZE:
            try:
                analyze_table(conn, table_name)
                # Sleep to avoid rate limiting
                time.sleep(2)
            except Exception as e:
                print(f"Error analyzing {table_name}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print("\n" + "="*60)
        print("Analysis complete!")
        print("="*60)
        
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    cnt = 0
    while True:
        main()
        print(f"Completed iteration {cnt + 1}, sleeping for 30 seconds...")
        time.sleep(30)

