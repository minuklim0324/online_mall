-- 1. Order Service 전용 DB
CREATE DATABASE IF NOT EXISTS order_db;
USE order_db;

CREATE TABLE orders (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    qty INT NOT NULL,           -- 기존 order_cnt를 소스 코드(Product.java)와 맞춤
    status VARCHAR(20) DEFAULT 'PENDING', -- 초기 상태 PENDING
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Product Service 전용 DB
CREATE DATABASE IF NOT EXISTS product_db;
USE product_db;

CREATE TABLE products (
    product_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    stock_cnt INT DEFAULT 0
);

-- 초기 상품 데이터 예시
INSERT INTO products (name, price, stock_cnt) VALUES ('삼성 갤럭시 S26', 1200000, 100);
INSERT INTO products (name, price, stock_cnt) VALUES ('애플 아이폰 17', 1300000, 50);

-- 3. Payment Service 전용 DB
CREATE DATABASE IF NOT EXISTS payment_db;
USE payment_db;

CREATE TABLE payments (
    payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'SUCCESS', -- PAID, REFUNDED 등으로 관리
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);