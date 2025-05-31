USE sales_time_mart;

CREATE TABLE IF NOT EXISTS monthly_trends (
    month Date,
    total_price Decimal64(2),
    sales_count UInt64
) ENGINE = MergeTree()
ORDER BY (month);

CREATE TABLE IF NOT EXISTS month_over_month_price (
    month Date,
    total_price Decimal64(2),
    sales_count UInt64,
    m_o_m_change Nullable(Decimal64(2)),
    m_o_m_change_share Nullable(Decimal64(2))
) ENGINE = MergeTree()
ORDER BY (month);

CREATE TABLE IF NOT EXISTS average_sales_quantity_by_months (
    month Date,
    average_sales_quantity Decimal64(2)
) ENGINE = MergeTree()
ORDER BY (month);
