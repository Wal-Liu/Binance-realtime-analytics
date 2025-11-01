CREATE TABLE rsi_indicators (
    symbol VARCHAR(20),
    interval VARCHAR(10),
    close_time BIGINT,
    close_price DECIMAL(16, 8),
    rsi DOUBLE PRECISION,
    -- Tạo một Primary Key để tránh trùng lặp nếu chạy lại
    PRIMARY KEY (symbol, interval, close_time)
);