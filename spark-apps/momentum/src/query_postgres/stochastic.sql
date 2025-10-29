CREATE TABLE stochastic_indicators (
    symbol VARCHAR(20),
    interval VARCHAR(10),
    close_time BIGINT,
    close_price DECIMAL(16, 8),
    percent_k DOUBLE PRECISION,
    percent_d DOUBLE PRECISION,
    -- Tạo một Primary Key để tránh trùng lặp
    PRIMARY KEY (symbol, interval, close_time)
);