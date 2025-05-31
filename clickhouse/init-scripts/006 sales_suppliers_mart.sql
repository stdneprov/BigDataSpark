USE sales_suppliers_mart;

CREATE TABLE IF NOT EXISTS most_price_suppliers (
    supplier_id UInt64,
    supplier_name String,
    total_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (total_price);

CREATE TABLE IF NOT EXISTS average_product_price_by_supplier  (
    supplier_id UInt64,
    supplier_name String,
    average_product_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (supplier_id);

CREATE TABLE IF NOT EXISTS sales_distribution_by_suppliers_countries (
    suppliers_country String,
    sales_quantity UInt64,
    share Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (suppliers_country);
