-- ============================================================
-- Schema: schema_a (primary test schema)
-- ============================================================
CREATE SCHEMA schema_a;
GO

-- Table with various column types, primary key, and indexes
CREATE TABLE schema_a.products (
    id INT NOT NULL PRIMARY KEY,
    name NVARCHAR(200) NOT NULL,
    description NVARCHAR(MAX),
    price DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL DEFAULT 0,
    weight FLOAT,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2(3) NOT NULL DEFAULT SYSDATETIME(),
    updated_at DATETIMEOFFSET(7),
    category VARCHAR(100),
    sku CHAR(12),
    metadata NVARCHAR(MAX),
    binary_data VARBINARY(256)
);
GO

-- Add extended properties (comments) on table and columns
EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Product catalog with inventory tracking',
    @level0type = N'SCHEMA', @level0name = N'schema_a',
    @level1type = N'TABLE',  @level1name = N'products';
GO

EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Unique product identifier',
    @level0type = N'SCHEMA', @level0name = N'schema_a',
    @level1type = N'TABLE',  @level1name = N'products',
    @level2type = N'COLUMN', @level2name = N'id';
GO

EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Product display name',
    @level0type = N'SCHEMA', @level0name = N'schema_a',
    @level1type = N'TABLE',  @level1name = N'products',
    @level2type = N'COLUMN', @level2name = N'name';
GO

-- Create indexes on products
CREATE UNIQUE INDEX idx_products_sku ON schema_a.products(sku);
GO

CREATE INDEX idx_products_category ON schema_a.products(category);
GO

CREATE INDEX idx_products_created_at ON schema_a.products(created_at);
GO

-- Insert test data
INSERT INTO schema_a.products (id, name, description, price, quantity, weight, is_active, category, sku)
VALUES
    (1, N'Widget A', N'A standard widget', 19.99, 100, 0.5, 1, 'Electronics', 'WIDG-A-00001'),
    (2, N'Widget B', N'A premium widget', 49.99, 50, 1.2, 1, 'Electronics', 'WIDG-B-00002'),
    (3, N'Gadget X', N'Discontinued gadget', 9.99, 0, 0.3, 0, 'Accessories', 'GADG-X-00003');
GO

-- Table with composite primary key and unique constraints
CREATE TABLE schema_a.order_items (
    order_id INT NOT NULL,
    item_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount DECIMAL(5,2) DEFAULT 0,
    CONSTRAINT pk_order_items PRIMARY KEY (order_id, item_id),
    CONSTRAINT uq_order_product UNIQUE (order_id, product_id)
);
GO

EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Line items within customer orders',
    @level0type = N'SCHEMA', @level0name = N'schema_a',
    @level1type = N'TABLE',  @level1name = N'order_items';
GO

INSERT INTO schema_a.order_items (order_id, item_id, product_id, quantity, unit_price, discount)
VALUES
    (1, 1, 1, 2, 19.99, 0),
    (1, 2, 2, 1, 49.99, 5.00),
    (2, 1, 3, 5, 9.99, 0);
GO

-- View: active products
CREATE VIEW schema_a.active_products AS
SELECT id, name, price, quantity, category
FROM schema_a.products
WHERE is_active = 1;
GO

EXEC sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Currently active products only',
    @level0type = N'SCHEMA', @level0name = N'schema_a',
    @level1type = N'VIEW',   @level1name = N'active_products';
GO

-- View: order summary
CREATE VIEW schema_a.order_summary AS
SELECT
    oi.order_id,
    COUNT(*) AS item_count,
    SUM(oi.quantity * oi.unit_price - oi.discount) AS total_amount
FROM schema_a.order_items oi
GROUP BY oi.order_id;
GO

-- ============================================================
-- Schema: schema_b (secondary schema for scope filtering tests)
-- ============================================================
CREATE SCHEMA schema_b;
GO

CREATE TABLE schema_b.customers (
    id INT NOT NULL PRIMARY KEY,
    email NVARCHAR(255) NOT NULL,
    full_name NVARCHAR(200),
    region VARCHAR(50)
);
GO

CREATE UNIQUE INDEX idx_customers_email ON schema_b.customers(email);
GO

INSERT INTO schema_b.customers (id, email, full_name, region)
VALUES
    (1, N'alice@example.com', N'Alice Smith', 'US'),
    (2, N'bob@example.com', N'Bob Jones', 'EU');
GO

CREATE VIEW schema_b.customer_regions AS
SELECT DISTINCT region FROM schema_b.customers WHERE region IS NOT NULL;
GO

-- ============================================================
-- SYNQ monitoring login/user with realistic minimal permissions
-- Mirrors what a real SYNQ integration would use:
-- 1. Catalog ingestion: sys.objects, sys.columns, sys.schemas, extended properties
-- 2. Automated metrics: sys.partitions, sys.allocation_units, sys.indexes
-- 3. Custom monitors: SELECT on monitored schemas
-- ============================================================

CREATE LOGIN synq WITH PASSWORD = 'SynqTest1!';
GO

CREATE USER synq FOR LOGIN synq;
GO

-- Server-level: list databases (sys.databases, sys.server_principals)
-- Note: in production, this requires a server-level GRANT on master:
--   USE master; GRANT VIEW ANY DATABASE TO synq;
-- In Azure SQL Edge, any login can see sys.databases by default.

-- Catalog & metrics: read sys catalog views (objects, columns, partitions, indexes)
GRANT VIEW DATABASE STATE TO synq;
GO

-- View definitions: read view SQL from sys.sql_modules
GRANT VIEW DEFINITION TO synq;
GO

-- Custom monitors: SELECT on data in all schemas
GRANT SELECT ON SCHEMA::schema_a TO synq;
GO

GRANT SELECT ON SCHEMA::schema_b TO synq;
GO
