USE sales_products_mart;

CREATE TABLE IF NOT EXISTS most_saling_products (
    product_id UInt64,
    product_name String,
    sales_count UInt64
) ENGINE = MergeTree()
ORDER BY (sales_count);

CREATE TABLE IF NOT EXISTS total_price_by_product_categories (
    category_id UInt64,
    category_name String,
    total_price Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (category_id);

CREATE TABLE IF NOT EXISTS average_rating_and_reviews_count_by_products (
    product_id UInt64,
    product_name String,
    rating Decimal32(1),
    reviews UInt64
) ENGINE = MergeTree()
ORDER BY (product_id);
