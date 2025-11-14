-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_staff;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS agg_kpi_revenue_by_dimension;
DROP TABLE IF EXISTS agg_kpi_status_by_order_type;
DROP TABLE IF EXISTS agg_customer_metrics;
DROP TABLE IF EXISTS agg_product_type_distribution;
DROP VIEW IF EXISTS vw_transaction_details;
DROP VIEW IF EXISTS vw_monthly_revenue;
DROP VIEW IF EXISTS vw_product_performance;
DROP VIEW IF EXISTS vw_customer_segmentation;

DROP TABLE IF EXISTS csv_readed;

-- ============================================================================
-- check if the file is readed
-- ============================================================================
CREATE TABLE csv_readed (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL
);

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Dimension: Product
-- ----------------------------------------------------------------------------
CREATE TABLE dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT UNIQUE NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    product_type TEXT NOT NULL,
    unit_price REAL NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_brand ON dim_product(brand);
CREATE INDEX idx_dim_product_model ON dim_product(model);
CREATE INDEX idx_dim_product_type ON dim_product(product_type);

-- ----------------------------------------------------------------------------
-- Dimension: Customer
-- ----------------------------------------------------------------------------
CREATE TABLE dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT UNIQUE NOT NULL,
    customer_age INTEGER NOT NULL,
    customer_gender TEXT NOT NULL,
    age_group TEXT NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_age_group ON dim_customer(age_group);
CREATE INDEX idx_dim_customer_gender ON dim_customer(customer_gender);

-- ----------------------------------------------------------------------------
-- Dimension: Date
-- ----------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend INTEGER NOT NULL,
    year_month TEXT NOT NULL
);

CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_dim_date_month ON dim_date(year, month);
CREATE INDEX idx_dim_date_year_month ON dim_date(year_month);

-- ----------------------------------------------------------------------------
-- Dimension: Staff
-- ----------------------------------------------------------------------------
CREATE TABLE dim_staff (
    staff_key INTEGER PRIMARY KEY AUTOINCREMENT,
    staff_id TEXT UNIQUE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- Dimension: Supplier
-- ----------------------------------------------------------------------------
CREATE TABLE dim_supplier (
    supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_id TEXT UNIQUE NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Fact: Transactions
-- ----------------------------------------------------------------------------
CREATE TABLE fact_transactions (
    transaction_key INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Foreign Keys to Dimensions
    product_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    staff_key INTEGER NOT NULL,
    supplier_key INTEGER NOT NULL,
    
    -- Degenerate Dimensions (transaction-specific attributes)
    transaction_id TEXT UNIQUE NOT NULL,
    transaction_datetime TIMESTAMP NOT NULL,
    order_type TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    transaction_status TEXT NOT NULL,
    
    -- Measures (Numeric Facts)
    quantity INTEGER NOT NULL,
    total_amount REAL NOT NULL,
    discount_applied REAL NOT NULL,
    delivery_time_min INTEGER NOT NULL,
    customer_rating REAL NOT NULL,
    inventory_level INTEGER NOT NULL,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key),
    FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key)
);

-- Indexes for fact table (for fast filtering and aggregation)
CREATE INDEX idx_fact_product ON fact_transactions(product_key);
CREATE INDEX idx_fact_customer ON fact_transactions(customer_key);
CREATE INDEX idx_fact_date ON fact_transactions(date_key);
CREATE INDEX idx_fact_staff ON fact_transactions(staff_key);
CREATE INDEX idx_fact_supplier ON fact_transactions(supplier_key);
CREATE INDEX idx_fact_order_type ON fact_transactions(order_type);
CREATE INDEX idx_fact_status ON fact_transactions(transaction_status);
CREATE INDEX idx_fact_datetime ON fact_transactions(transaction_datetime);

-- ============================================================================
-- AGGREGATE/SUMMARY TABLES (for Dashboard Performance)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- KPI Summary: Revenue by Dimension
-- ----------------------------------------------------------------------------
CREATE TABLE agg_kpi_revenue_by_dimension (
    dimension TEXT NOT NULL,
    dimension_value TEXT NOT NULL,
    total_amount REAL NOT NULL,
    transaction_count INTEGER NOT NULL,
    avg_transaction_value REAL NOT NULL,
    
    PRIMARY KEY (dimension, dimension_value)
);

-- ----------------------------------------------------------------------------
-- KPI Summary: Transaction Status by Order Type
-- ----------------------------------------------------------------------------
CREATE TABLE agg_kpi_status_by_order_type (
    order_type TEXT NOT NULL,
    transaction_status TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    
    PRIMARY KEY (order_type, transaction_status)
);

-- ----------------------------------------------------------------------------
-- Customer Analysis: Metrics by Demographics
-- ----------------------------------------------------------------------------
CREATE TABLE agg_customer_metrics (
    age_group TEXT NOT NULL,
    gender TEXT NOT NULL,
    year_month TEXT NOT NULL,
    avg_discount_applied REAL NOT NULL,
    avg_customer_rating REAL NOT NULL,
    transaction_count INTEGER NOT NULL,
    total_revenue REAL NOT NULL,
    
    PRIMARY KEY (age_group, gender, year_month)
);

-- ----------------------------------------------------------------------------
-- Product Analysis: Product Type Distribution
-- ----------------------------------------------------------------------------
CREATE TABLE agg_product_type_distribution (
    dimension TEXT NOT NULL,
    dimension_value TEXT NOT NULL,
    product_type TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    total_revenue REAL NOT NULL,
    
    PRIMARY KEY (dimension, dimension_value, product_type)
);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- View: Complete Transaction Details
-- ----------------------------------------------------------------------------
CREATE VIEW vw_transaction_details AS
SELECT 
    f.transaction_id,
    f.transaction_datetime,
    f.order_type,
    f.payment_method,
    f.transaction_status,
    
    -- Product details
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.model,
    p.product_type,
    p.unit_price,
    
    -- Customer details
    c.customer_id,
    c.customer_age,
    c.customer_gender,
    c.age_group,
    
    -- Date details
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    d.day_name,
    
    -- Staff and Supplier
    st.staff_id,
    su.supplier_id,
    
    -- Measures
    f.quantity,
    f.total_amount,
    f.discount_applied,
    f.delivery_time_min,
    f.customer_rating,
    f.inventory_level
    
FROM fact_transactions f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_staff st ON f.staff_key = st.staff_key
JOIN dim_supplier su ON f.supplier_key = su.supplier_key;

-- ----------------------------------------------------------------------------
-- View: Monthly Revenue Summary
-- ----------------------------------------------------------------------------
CREATE VIEW vw_monthly_revenue AS
SELECT 
    d.year,
    d.month,
    d.year_month,
    d.month_name,
    COUNT(f.transaction_key) as transaction_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value,
    SUM(f.quantity) as total_quantity_sold
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.transaction_status = 'Completed'
GROUP BY d.year, d.month, d.year_month, d.month_name
ORDER BY d.year, d.month;

-- ----------------------------------------------------------------------------
-- View: Product Performance
-- ----------------------------------------------------------------------------
CREATE VIEW vw_product_performance AS
SELECT 
    p.category,
    p.brand,
    p.model,
    p.product_name,
    p.product_type,
    COUNT(f.transaction_key) as transaction_count,
    SUM(f.quantity) as total_quantity_sold,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value,
    AVG(f.customer_rating) as avg_rating
FROM fact_transactions f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.transaction_status = 'Completed'
GROUP BY p.category, p.brand, p.model, p.product_name, p.product_type;

-- ----------------------------------------------------------------------------
-- View: Customer Segmentation
-- ----------------------------------------------------------------------------
CREATE VIEW vw_customer_segmentation AS
SELECT 
    c.age_group,
    c.customer_gender,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    COUNT(f.transaction_key) as transaction_count,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_value,
    AVG(f.customer_rating) as avg_rating,
    AVG(f.discount_applied) as avg_discount
FROM fact_transactions f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.transaction_status = 'Completed'
GROUP BY c.age_group, c.customer_gender;

-- ============================================================================
-- COMMENTS
-- ============================================================================

-- Star Schema Benefits:
-- 1. Simple and intuitive structure for BI tools
-- 2. Optimized for read-heavy analytical queries
-- 3. Denormalized dimensions for faster joins
-- 4. Pre-aggregated tables for dashboard performance
-- 5. Clear separation of facts (measures) and dimensions (attributes)

-- Usage:
-- 1. Run this script to create the schema: sqlite3 mobile_shop_dw.db < schema.sql
-- 2. Use etl_star_schema.py to populate the tables
-- 3. Query using views for common analytics needs

