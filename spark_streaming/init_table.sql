-- Script khởi tạo bảng PostgreSQL cho Spark Streaming
-- Chạy script này trên PostgreSQL local trước khi deploy Spark

-- Kết nối vào database
\c house_warehouse;

-- Tạo bảng house_data_final
CREATE TABLE IF NOT EXISTS house_data_final (
    id INTEGER,
    price DOUBLE PRECISION,
    sqft INTEGER,
    bedrooms INTEGER,
    bathrooms DOUBLE PRECISION,
    location VARCHAR(255),
    year_built INTEGER,
    condition VARCHAR(50),
    is_expensive VARCHAR(10),
    processed_by VARCHAR(20),
    created_at TIMESTAMP,
    PRIMARY KEY (id, created_at)
);

-- Tạo index để query nhanh hơn
CREATE INDEX IF NOT EXISTS idx_created_at ON house_data_final(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processed_by ON house_data_final(processed_by);
CREATE INDEX IF NOT EXISTS idx_location ON house_data_final(location);
CREATE INDEX IF NOT EXISTS idx_is_expensive ON house_data_final(is_expensive);

-- Hiển thị thông tin bảng
\d house_data_final;

-- Test query
SELECT 
    processed_by,
    COUNT(*) as total_records,
    MIN(created_at) as first_record,
    MAX(created_at) as last_record
FROM house_data_final
GROUP BY processed_by;
