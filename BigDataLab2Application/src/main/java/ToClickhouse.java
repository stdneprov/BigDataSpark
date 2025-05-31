import java.util.Collections;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class ToClickhouse {

    public static void convertSalesProductMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_products_mart";
        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        
        Dataset<Row> products = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_products", postgresConnectionProperties);
        
        Dataset<Row> salesGroupedByProductId = sales.groupBy("product_id")
            .agg(
                functions.sum("quantity").alias("sales_count"),
                functions.sum("total_price").alias("total_price")
            );

        Dataset<Row> productSales = products.join(salesGroupedByProductId, products.col("product_id").equalTo(salesGroupedByProductId.col("product_id")), "left");

        
        productSales.select(
            products.col("product_id"),
            products.col("name").alias("product_name"),
            salesGroupedByProductId.col("sales_count")
        ).orderBy(functions.desc("sales_count")).limit(10).write().mode("append").jdbc(clickhouseJdbcUrl, "most_saling_products", clickhouseConnectionProperties);

        Dataset<Row> categories = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_product_categories", postgresConnectionProperties);

        Dataset<Row> productSalesGroupedByCategoryId = productSales.groupBy(products.col("category_id"))
            .agg(
                functions.sum("total_price").alias("total_price")
            );

        productSalesGroupedByCategoryId.printSchema();

        categories.join(productSalesGroupedByCategoryId, categories.col("category_id").equalTo(productSalesGroupedByCategoryId.col("category_id")))
        .select(
            categories.col("category_id"),
            categories.col("name").alias("category_name"),
            productSalesGroupedByCategoryId.col("total_price")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "total_price_by_product_categories", clickhouseConnectionProperties);

        products.select(
            functions.col("product_id"),
            functions.col("name").alias("product_name"),
            functions.col("rating"),
            functions.col("reviews")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "average_rating_and_reviews_count_by_products", clickhouseConnectionProperties);
        
    }

    public static void convertSalesCustomersMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_customers_mart";
        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        
        Dataset<Row> customers = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_customers", postgresConnectionProperties);

        Dataset<Row> countries = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_countries", postgresConnectionProperties);
        
        Dataset<Row> salesGroupedByCustomerId = sales.groupBy("customer_id")
            .agg(
                functions.sum("total_price").alias("total_price"),
                functions.avg("total_price").alias("average_price")
            );

        Dataset<Row> customerSales = customers.join(salesGroupedByCustomerId, customers.col("customer_id").equalTo(salesGroupedByCustomerId.col("customer_id")), "left");
        
        customerSales.select(
            customers.col("customer_id"),
            customers.col("first_name").alias("customer_first_name"),
            customers.col("last_name").alias("customer_last_name"),
            salesGroupedByCustomerId.col("total_price")
        ).orderBy(functions.desc("total_price")).limit(10).write().mode("append").jdbc(clickhouseJdbcUrl, "most_buyimg_customers", clickhouseConnectionProperties);

        long totalCustomers = salesGroupedByCustomerId.select("customer_id").distinct().count();
        Dataset<Row> customersGroupedByCountryId = customers.groupBy("country_id")
        .agg(
            functions.count("customer_id").alias("customers_quantity"),
            functions.round(
                functions.count("customer_id")
                    .divide(totalCustomers)
                    .multiply(100),
                2
            ).alias("share")
        );

        Dataset<Row> countriesCustomers = countries.join(customersGroupedByCountryId, countries.col("country_id").equalTo(customersGroupedByCountryId.col("country_id")), "right");
        

        countriesCustomers.select(
            countries.col("name").alias("country"),
            customersGroupedByCountryId.col("customers_quantity"),
            customersGroupedByCountryId.col("share")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "customers_distribution_by_countries", clickhouseConnectionProperties);

        customerSales.select(
            customers.col("customer_id"),
            customers.col("first_name").alias("customer_first_name"),
            customers.col("last_name").alias("customer_last_name"),
            salesGroupedByCustomerId.col("average_price")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "average_price_by_customers", clickhouseConnectionProperties);
    }

    public static void convertSalesSuppliersMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_suppliers_mart";
        Dataset<Row> products = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_products", postgresConnectionProperties);
        
        Dataset<Row> suppliers = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_suppliers", postgresConnectionProperties);

        Dataset<Row> countries = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_countries", postgresConnectionProperties);

        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        
        Dataset<Row> salesGroupedByProductId = sales.groupBy("product_id")
            .agg(
                functions.sum("quantity").alias("sales_count"),
                functions.sum("total_price").alias("total_price")
            );

        Dataset<Row> productSales = products.join(salesGroupedByProductId, products.col("product_id").equalTo(salesGroupedByProductId.col("product_id")), "left");

        Dataset<Row> productsSalesGroupedBySupplierId = productSales.groupBy(functions.col("supplier_id")).agg(functions.sum("total_price").alias("total_price"), functions.sum("sales_count").alias("sales_count"));
        Dataset<Row> suppliersProductsSales = suppliers.join(productsSalesGroupedBySupplierId, suppliers.col("supplier_id").equalTo(productsSalesGroupedBySupplierId.col("supplier_id")), "left");
        suppliersProductsSales.select(
            suppliers.col("supplier_id"),
            suppliers.col("name").alias("supplier_name"),
            productsSalesGroupedBySupplierId.col("total_price")
        ).orderBy(functions.desc("total_price")).limit(5).write().mode("append").jdbc(clickhouseJdbcUrl, "most_price_suppliers", clickhouseConnectionProperties);


        Dataset<Row> productsGroupedBySupplierId = products.groupBy("supplier_id")
            .agg(
                functions.avg("price").alias("average_product_price")
            );

        Dataset<Row> suppliersProducts = suppliers.join(productsGroupedBySupplierId, suppliers.col("supplier_id").equalTo(productsGroupedBySupplierId.col("supplier_id")), "left");

        suppliersProducts.select(
            suppliers.col("supplier_id"),
            suppliers.col("name").alias("supplier_name"),
            productsGroupedBySupplierId.col("average_product_price")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "average_product_price_by_supplier", clickhouseConnectionProperties);

        Dataset<Row> suppliersProductsSalesGroupedByCountryId = suppliersProductsSales.groupBy("country_id").agg(functions.sum("sales_count").alias("sales_count"));
        Dataset<Row> countrySuppliersProductsSales = countries.join(suppliersProductsSalesGroupedByCountryId, countries.col("country_id").equalTo(suppliersProductsSalesGroupedByCountryId.col("country_id")), "right");
        long totalSales = sales.agg(functions.sum("quantity")).first().getLong(0);
        countrySuppliersProductsSales.select(
            countries.col("name").alias("suppliers_country"),
            countrySuppliersProductsSales.col("sales_count").alias("sales_quantity"),
            countrySuppliersProductsSales.col("sales_count").divide(totalSales).multiply(100).alias("share")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "sales_distribution_by_suppliers_countries", clickhouseConnectionProperties);

    }

    public static void convertSalesStoresMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        Logger logger = Logger.getLogger("org.stdneprov.bigdata.lab2.ToClickhouse");
        logger.info("sales_stores_mart");
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_stores_mart";
        Dataset<Row> products = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_products", postgresConnectionProperties);
        
        Dataset<Row> stores = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_stores", postgresConnectionProperties);

        Dataset<Row> countries = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_countries", postgresConnectionProperties);

        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        
        
        Dataset<Row> salesGroupedByProductId = sales.groupBy("product_id", "store_id").agg(
            functions.sum("total_price").alias("total_price"), 
            functions.sum("quantity").alias("sales_quantity"),
            functions.avg("total_price").alias("average_price")
        );

        salesGroupedByProductId.printSchema();

        Dataset<Row> productsSalesGroupedByProductId = products.join(salesGroupedByProductId, products.col("product_id").equalTo(salesGroupedByProductId.col("product_id")), "left");
        Dataset<Row> productsSalesGroupedByProductIdGroupedByStoreId = productsSalesGroupedByProductId.groupBy("store_id").agg(
            functions.sum("total_price").alias("total_price"),
            functions.sum("sales_quantity").alias("sales_quantity"),
            functions.avg("total_price").alias("average_price")
        );
        logger.info("productsSalesGroupedByProductIdGroupedByStoreId");

        Dataset<Row> storesProductsSalesGroupedByProductIdGroupedByStoreId = stores.join(productsSalesGroupedByProductIdGroupedByStoreId, stores.col("store_id").equalTo(productsSalesGroupedByProductIdGroupedByStoreId.col("store_id")), "left");
        storesProductsSalesGroupedByProductIdGroupedByStoreId.select(
            stores.col("store_id"),
            stores.col("name").alias("store_name"),
            productsSalesGroupedByProductIdGroupedByStoreId.col("total_price")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "most_price_store", clickhouseConnectionProperties);

        long totalSales = sales.agg(functions.sum("quantity")).first().getLong(0);

        Dataset<Row> storesProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId = storesProductsSalesGroupedByProductIdGroupedByStoreId.groupBy("country_id").agg(functions.sum("total_price").alias("total_price"), functions.sum("sales_quantity").alias("sales_quantity"));
        Dataset<Row> countryStoresProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId = countries.join(storesProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId, countries.col("country_id").equalTo(storesProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId.col("country_id")), "right");
        countryStoresProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId.select(
            countries.col("name").alias("country"),
            storesProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId.col("sales_quantity"),
            storesProductsSalesGroupedByProductIdGroupedByStoreIdGroupedByCountryId.col("sales_quantity").divide(totalSales).multiply(100).alias("share")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "sales_distribution_by_countries", clickhouseConnectionProperties);

        storesProductsSalesGroupedByProductIdGroupedByStoreId.select(
            stores.col("store_id"),
            stores.col("name").alias("store_name"),
            productsSalesGroupedByProductIdGroupedByStoreId.col("average_price")
        ).write().mode("append").jdbc(clickhouseJdbcUrl, "average_price_by_shop", clickhouseConnectionProperties);
    }

    public static void convertSalesTimeMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_time_mart";
        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        
        Dataset<Row> monthlyTrends = sales.groupBy(functions.date_trunc("MON", functions.col("date")).alias("month")).agg(
            functions.sum("total_price").alias("total_price"),
            functions.sum("quantity").alias("sales_count")
        ).orderBy(functions.col("month"));


        Dataset<Row> prevMonths = monthlyTrends.select(
            functions.add_months(functions.col("month"), 1).alias("month"),
            functions.col("total_price").alias("prev_total_price"),
            functions.col("sales_count").alias("prev_sales_count")
        );

        Dataset<Row> monthOverMonth = monthlyTrends.join(prevMonths, monthlyTrends.col("month").equalTo(prevMonths.col("month")), "left")
        .select(
            monthlyTrends.col("month"),
            monthlyTrends.col("total_price"),
            monthlyTrends.col("sales_count"),
            monthlyTrends.col("total_price").minus(prevMonths.col("prev_total_price")).alias("m_o_m_change"),
            monthlyTrends.col("total_price").minus(prevMonths.col("prev_total_price")).divide(monthlyTrends.col("total_price")).multiply(100).alias("m_o_m_change_share")
        ).orderBy(monthlyTrends.col("month"));

        Dataset<Row> averageSalesQuantityByMonths = sales.groupBy(functions.date_trunc("MON", functions.col("date")).alias("month")).agg(
            functions.avg("quantity").alias("average_sales_quantity")
        ).orderBy(functions.col("month"));

        monthlyTrends.write().mode("append").jdbc(clickhouseJdbcUrl, "monthly_trends", clickhouseConnectionProperties);
        monthOverMonth.write().mode("append").jdbc(clickhouseJdbcUrl, "month_over_month_price", clickhouseConnectionProperties);
        averageSalesQuantityByMonths.write().mode("append").jdbc(clickhouseJdbcUrl, "average_sales_quantity_by_months", clickhouseConnectionProperties);
    }

    public static void convertSalesQualityMart(
        SparkSession spark, 
        String postgresJdbcUrl,
        Properties postgresConnectionProperties,
        String clickhouseJdbcBaseUrl,
        Properties clickhouseConnectionProperties
    ) {
        String clickhouseJdbcUrl = clickhouseJdbcBaseUrl + "sales_quality_mart";
        Dataset<Row> sales = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.fact_sales", postgresConnectionProperties);
        Dataset<Row> products = spark.read()
            .jdbc(postgresJdbcUrl, "snowflake.dim_products", postgresConnectionProperties);
        
        Dataset<Row> mostRatingProducts = products.select(
            functions.col("product_id"),
            functions.col("name").alias("product_name"),
            functions.col("rating")
        ).orderBy(functions.desc("rating")).limit(1);

        Dataset<Row> leastRatingProducts = products.select(
            functions.col("product_id"),
            functions.col("name").alias("product_name"),
            functions.col("rating")
        ).orderBy(functions.asc("rating")).limit(1);

        Dataset<Row> salesGroupedByProductId = sales.groupBy("product_id").agg(
            functions.sum("quantity").alias("sales_quantity")
        );

        Dataset<Row> productsSalesGroupedByProductId = products.join(salesGroupedByProductId, products.col("product_id").equalTo(salesGroupedByProductId.col("product_id")), "left");

        Double correlation = productsSalesGroupedByProductId.groupBy(functions.col("rating")).agg(
            functions.avg("sales_quantity").alias("avg_sales")
        ).stat().corr("rating", "avg_sales", "pearson");

        Dataset<Row> correlationDataset = spark.createDataFrame(
                Collections.singletonList(RowFactory.create(correlation)),
                new StructType(new StructField[]{
                        new StructField("correlation", DataTypes.DoubleType, false, Metadata.empty())
                })
        );

        Dataset<Row> mostReviewsProducts = products.select(
            functions.col("product_id"),
            functions.col("name").alias("product_name"),
            functions.col("reviews")
        ).orderBy(functions.desc("reviews"));


        mostReviewsProducts.write().mode("append").jdbc(clickhouseJdbcUrl, "most_reviews_products", clickhouseConnectionProperties);
        correlationDataset.write().mode("append").jdbc(clickhouseJdbcUrl, "rating_sales_quantity_correlation", clickhouseConnectionProperties);
        leastRatingProducts.write().mode("append").jdbc(clickhouseJdbcUrl, "least_rating_products", clickhouseConnectionProperties);
        mostRatingProducts.write().mode("append").jdbc(clickhouseJdbcUrl, "most_rating_products", clickhouseConnectionProperties);
    }

    public static void convert() {
        SparkSession spark = SparkSession.builder()
            .appName("BigDataSpark|ToClickhouse")
            .config("spark.master", "spark://spark-master:7077")
            .config("spark.submit.deployMode", "client")
            .getOrCreate();
        String postgresJdbcUrl = "jdbc:postgresql://postgres:5432/lab2";
        Properties postgresConnectionProperties = new Properties();
        postgresConnectionProperties.put("user", "stdneprov");
        postgresConnectionProperties.put("password", "password");
        postgresConnectionProperties.put("driver", "org.postgresql.Driver");
        Properties clickhouseConnectionProperties = new Properties();
        clickhouseConnectionProperties.put("user", "stdneprov");
        clickhouseConnectionProperties.put("password", "password");
        clickhouseConnectionProperties.put("driver", "com.clickhouse.jdbc.ClickHouseDriver");
        String clickhouseJdbcBaseUrl = "jdbc:clickhouse://clickhouse:8123/";
        convertSalesProductMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );

        convertSalesCustomersMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );
        convertSalesSuppliersMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );
        convertSalesStoresMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );
        convertSalesTimeMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );
        convertSalesQualityMart(
            spark,
            postgresJdbcUrl,
            postgresConnectionProperties,
            clickhouseJdbcBaseUrl,
            clickhouseConnectionProperties
        );

        spark.stop();
    }
}