-- Упрощенная схема БД 

-- 1. Основная таблица с чеками (все в одной таблице)
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id SERIAL PRIMARY KEY,
    doc_id VARCHAR(50) NOT NULL,
    store_id INTEGER NOT NULL,
    cash_id INTEGER NOT NULL,
    item VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount DECIMAL(10, 2) DEFAULT 0 CHECK (discount_amount >= 0),
    total_price DECIMAL(12, 2) GENERATED ALWAYS AS (quantity * unit_price - discount_amount) STORED,
    receipt_date DATE NOT NULL,
    file_name VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Таблица для отслеживания обработанных файлов
CREATE TABLE IF NOT EXISTS processed_files (
    file_id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL UNIQUE,
    file_path TEXT,
    records_count INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT
);

-- 3. Индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_receipts_doc_id ON receipts(doc_id);
CREATE INDEX IF NOT EXISTS idx_receipts_date ON receipts(receipt_date);
CREATE INDEX IF NOT EXISTS idx_receipts_store ON receipts(store_id);
CREATE INDEX IF NOT EXISTS idx_receipts_store_cash ON receipts(store_id, cash_id);
CREATE INDEX IF NOT EXISTS idx_receipts_category ON receipts(category);
CREATE INDEX IF NOT EXISTS idx_receipts_file ON receipts(file_name);
CREATE INDEX IF NOT EXISTS idx_processed_files_name ON processed_files(file_name);

-- 4. Представление для аналитики
CREATE OR REPLACE VIEW sales_summary AS
SELECT 
    receipt_date,
    store_id,
    cash_id,
    COUNT(DISTINCT doc_id) as receipts_count,
    COUNT(*) as items_count,
    SUM(unit_price * quantity) as total_sales,
    SUM(discount_amount) as total_discount,
    SUM(unit_price * quantity - discount_amount) as net_sales
FROM receipts 
GROUP BY receipt_date, store_id, cash_id
ORDER BY receipt_date DESC, store_id, cash_id;

-- Комментарии к таблицам
COMMENT ON TABLE receipts IS 'Основная таблица с данными о продажах из CSV файлов';
COMMENT ON TABLE processed_files IS 'Таблица для отслеживания обработанных CSV файлов';