FROM python:3.11-slim

# Đặt working directory
WORKDIR /app

# Copy toàn bộ project (nếu chỉ muốn phần ingestion thì chỉ copy phần đó)
COPY requirements.txt .

# Cài thư viện
RUN pip install --no-cache-dir -r requirements.txt

# Lệnh mặc định khi container chạy
# CMD ["python","-u","src/makeStream.py"]
