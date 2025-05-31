USE sales_quality_mart;

CREATE TABLE IF NOT EXISTS most_rating_products (
    product_id UInt64,
    product_name String,
    rating Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (rating);

CREATE TABLE IF NOT EXISTS least_rating_products (
    product_id UInt64,
    product_name String,
    rating Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (rating);

CREATE TABLE IF NOT EXISTS rating_sales_quantity_correlation (
    correlation Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (correlation);

CREATE TABLE IF NOT EXISTS most_reviews_products (
    product_id UInt64,
    product_name String,
    reviews UInt64
) ENGINE = MergeTree()
ORDER BY (reviews);
