USE sales_stores_mart;

CREATE TABLE IF NOT EXISTS most_price_store (
    store_id UInt64,
    store_name String,
    total_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (total_price);

CREATE TABLE IF NOT EXISTS sales_distribution_by_countries (
    country String,
    sales_quantity UInt64,
    share Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (country);

CREATE TABLE IF NOT EXISTS average_price_by_shop (
    store_id UInt64,
    store_name String,
    average_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (store_id);
