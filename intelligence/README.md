# Intelligence Suggestion System

Hệ thống phân tích dữ liệu crypto sử dụng Gemini AI để đưa ra góp ý và nhận xét.

## Cấu hình

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Cấu hình Gemini API

Chỉnh sửa file `configs/gemini.json` và thêm API key của bạn:

```json
{
  "api_key": "YOUR_GEMINI_API_KEY_HERE",
  "model": "gemini-pro"
}
```

Lấy API key tại: https://makersuite.google.com/app/apikey

### 3. Cấu hình PostgreSQL

File `configs/postgres.json` đã được cấu hình sẵn theo docker-compose:

```json
{
  "host": "postgres",
  "port": 5432,
  "database": "crypto_db",
  "user": "postgres",
  "password": "your_password"
}
```

## Chạy chương trình

### Chạy một lần

```bash
python suggestion.py
```

### Chạy định kỳ (mỗi 5 phút)

```bash
chmod +x run_suggestion.sh
./run_suggestion.sh
```

### Chạy trong Docker

Thêm vào `docker-compose.yml`:

```yaml
intelligence:
  build:
    context: ./ingestion
    dockerfile: ingestion.Dockerfile
  container_name: binance-intelligence
  volumes:
    - ./intelligence:/app
  working_dir: /app
  command: python -u suggestion.py
  depends_on:
    - postgres
  environment:
    - PYTHONUNBUFFERED=1
```

## Danh sách bảng được phân tích

- `rsi_indicators` → `rsi_indicators_suggest`
- `stochastic_indicators` → `stochastic_indicators_suggest`
- `mfi_14d` → `mfi_14d_suggest`
- `bollinger_bands_1m_agg` → `bollinger_bands_1m_agg_suggest`
- `donchian_channel` → `donchian_channel_suggest`
- `qvb_1m` → `qvb_1m_suggest`
- `crypto_volume` → `crypto_volume_suggest`
- `crypto_ma` → `crypto_ma_suggest`
- `crypto_aroon` → `crypto_aroon_suggest`
- `crypto_adx` → `crypto_adx_suggest`

## Cấu trúc bảng suggest

Mỗi bảng `*_suggest` có cấu trúc:

```sql
CREATE TABLE {table_name}_suggest (
    id SERIAL PRIMARY KEY,
    analysis_time TIMESTAMP NOT NULL DEFAULT NOW(),
    symbol VARCHAR(20),
    interval VARCHAR(10),
    suggestion TEXT NOT NULL,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Kiểm tra kết quả

### Kết nối PostgreSQL

```bash
# Local
psql -h localhost -p 15432 -U postgres -d crypto_db

# Docker
docker exec -it binance-postgres psql -U postgres -d crypto_db
```

### Query dữ liệu

```sql
-- Xem tất cả bảng suggest
\dt *_suggest

-- Xem góp ý mới nhất cho RSI
SELECT 
    analysis_time,
    symbol,
    suggestion,
    created_at 
FROM rsi_indicators_suggest 
ORDER BY created_at DESC 
LIMIT 5;

-- Xem góp ý mới nhất cho Bollinger Bands
SELECT 
    analysis_time,
    symbol,
    suggestion 
FROM bollinger_bands_1m_agg_suggest 
ORDER BY created_at DESC 
LIMIT 5;

-- Xem raw data chi tiết
SELECT 
    symbol,
    suggestion,
    raw_data::json 
FROM crypto_adx_suggest 
ORDER BY created_at DESC 
LIMIT 1;
```

## Tính năng

- ✅ Tự động lấy 5 dòng dữ liệu mới nhất (5 phút gần nhất) từ mỗi bảng
- ✅ Sử dụng Gemini AI để phân tích xu hướng
- ✅ Đưa ra nhận xét về tín hiệu kỹ thuật
- ✅ Đề xuất hành động (MUA/BÁN/GIỮ)
- ✅ Cảnh báo rủi ro
- ✅ Lưu trữ góp ý vào bảng suggest tương ứng
- ✅ Lưu trữ raw data dạng JSON để tham khảo

## Troubleshooting

### Lỗi kết nối PostgreSQL

Kiểm tra:
- PostgreSQL container đã chạy chưa: `docker ps | grep postgres`
- Cấu hình đúng trong `configs/postgres.json`
- Port 15432 có bị block không

### Lỗi Gemini API

Kiểm tra:
- API key đã đúng chưa
- Đã enable Gemini API chưa
- Rate limit (chương trình có sleep 2s giữa mỗi bảng)

### Không tìm thấy bảng

Kiểm tra:
- Các Spark job đã chạy và tạo bảng chưa
- Tên bảng trong `TABLES_TO_ANALYZE` có đúng không
