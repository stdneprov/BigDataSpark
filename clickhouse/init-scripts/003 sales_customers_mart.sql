USE sales_customers_mart;

CREATE TABLE IF NOT EXISTS most_buyimg_customers (
    customer_id UInt64,
    customer_first_name String,
    customer_last_name String,
    customer_email String,
    total_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (total_price);

CREATE TABLE IF NOT EXISTS customers_distribution_by_countries (
    country String,
    customers_quantity UInt64,
    share Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (country);

CREATE TABLE IF NOT EXISTS average_price_by_customers (
    customer_id UInt64,
    customer_first_name String,
    customer_last_name String,
    average_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (customer_id);
