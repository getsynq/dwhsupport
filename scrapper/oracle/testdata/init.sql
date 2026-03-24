-- ============================================================
-- Schema: synq_a (primary test schema)
-- Created by Docker via APP_USER env var.
-- ============================================================

-- Table with various column types
CREATE TABLE synq_a.products (
    id NUMBER(10) NOT NULL PRIMARY KEY,
    name NVARCHAR2(200) NOT NULL,
    description CLOB,
    price NUMBER(10,2) NOT NULL,
    quantity NUMBER(10) DEFAULT 0 NOT NULL,
    weight FLOAT(53),
    is_active NUMBER(1) DEFAULT 1 NOT NULL,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE,
    category VARCHAR2(100),
    sku CHAR(12),
    binary_data RAW(256)
)
;

-- Comments on table and columns
COMMENT ON TABLE synq_a.products IS 'Product catalog with inventory tracking'
;

COMMENT ON COLUMN synq_a.products.id IS 'Unique product identifier'
;

COMMENT ON COLUMN synq_a.products.name IS 'Product display name'
;

-- Indexes
CREATE UNIQUE INDEX synq_a.idx_products_sku ON synq_a.products(sku)
;

CREATE INDEX synq_a.idx_products_category ON synq_a.products(category)
;

CREATE INDEX synq_a.idx_products_created_at ON synq_a.products(created_at)
;

-- Insert test data
INSERT INTO synq_a.products (id, name, description, price, quantity, weight, is_active, category, sku)
VALUES (1, 'Widget A', 'A standard widget', 19.99, 100, 0.5, 1, 'Electronics', 'WIDG-A-00001')
;

INSERT INTO synq_a.products (id, name, description, price, quantity, weight, is_active, category, sku)
VALUES (2, 'Widget B', 'A premium widget', 49.99, 50, 1.2, 1, 'Electronics', 'WIDG-B-00002')
;

INSERT INTO synq_a.products (id, name, description, price, quantity, weight, is_active, category, sku)
VALUES (3, 'Gadget X', 'Discontinued gadget', 9.99, 0, 0.3, 0, 'Accessories', 'GADG-X-00003')
;

-- Table with composite primary key and unique constraint
CREATE TABLE synq_a.order_items (
    order_id NUMBER(10) NOT NULL,
    item_id NUMBER(10) NOT NULL,
    product_id NUMBER(10) NOT NULL,
    quantity NUMBER(10) NOT NULL,
    unit_price NUMBER(10,2) NOT NULL,
    discount NUMBER(5,2) DEFAULT 0,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, item_id),
    CONSTRAINT uq_order_product UNIQUE (order_id, product_id)
)
;

COMMENT ON TABLE synq_a.order_items IS 'Line items within customer orders'
;

INSERT INTO synq_a.order_items (order_id, item_id, product_id, quantity, unit_price, discount)
VALUES (1, 1, 1, 2, 19.99, 0)
;

INSERT INTO synq_a.order_items (order_id, item_id, product_id, quantity, unit_price, discount)
VALUES (1, 2, 2, 1, 49.99, 5.00)
;

INSERT INTO synq_a.order_items (order_id, item_id, product_id, quantity, unit_price, discount)
VALUES (2, 1, 3, 5, 9.99, 0)
;

-- View: active products
CREATE VIEW synq_a.active_products AS
SELECT id, name, price, quantity, category
FROM synq_a.products
WHERE is_active = 1
;

COMMENT ON TABLE synq_a.active_products IS 'Currently active products only'
;

-- View: order summary
CREATE VIEW synq_a.order_summary AS
SELECT
    oi.order_id,
    COUNT(*) AS item_count,
    SUM(oi.quantity * oi.unit_price - oi.discount) AS total_amount
FROM synq_a.order_items oi
GROUP BY oi.order_id
;

-- Gather statistics so table metrics return row counts
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS('SYNQ_A', 'PRODUCTS');
    DBMS_STATS.GATHER_TABLE_STATS('SYNQ_A', 'ORDER_ITEMS');
END;
;

-- ============================================================
-- Schema: synq_b (secondary schema for scope filtering)
-- ============================================================

CREATE USER synq_b IDENTIFIED BY "SynqTest1" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS
;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO synq_b
;

CREATE TABLE synq_b.customers (
    id NUMBER(10) NOT NULL PRIMARY KEY,
    email NVARCHAR2(255) NOT NULL,
    full_name NVARCHAR2(200),
    region VARCHAR2(50)
)
;

CREATE UNIQUE INDEX synq_b.idx_customers_email ON synq_b.customers(email)
;

INSERT INTO synq_b.customers (id, email, full_name, region)
VALUES (1, 'alice@example.com', 'Alice Smith', 'US')
;

INSERT INTO synq_b.customers (id, email, full_name, region)
VALUES (2, 'bob@example.com', 'Bob Jones', 'EU')
;

CREATE VIEW synq_b.customer_regions AS
SELECT DISTINCT region FROM synq_b.customers WHERE region IS NOT NULL
;

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS('SYNQ_B', 'CUSTOMERS');
END;
;

COMMIT
;

-- ============================================================
-- SYNQ monitoring user with realistic minimal permissions
-- Mirrors what a real SYNQ integration would use:
-- 1. Catalog ingestion: metadata from ALL_* views
-- 2. Automated metrics: row counts/sizes from ALL_TABLES stats
-- 3. Custom monitors: SELECT on monitored schemas
-- ============================================================

CREATE USER synq IDENTIFIED BY "SynqTest1" DEFAULT TABLESPACE USERS
;

-- Basic connectivity
GRANT CREATE SESSION TO synq
;

-- Catalog & metrics: ALL_* views only show objects the user has access to,
-- so we grant SELECT on the target schemas' tables/views.
GRANT SELECT ANY TABLE TO synq
;

-- Dictionary access for V$PARAMETER (table size = BLOCKS * db_block_size)
-- and full visibility into ALL_* catalog views across schemas
GRANT SELECT ANY DICTIONARY TO synq
;
