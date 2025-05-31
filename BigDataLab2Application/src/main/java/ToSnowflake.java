import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;


public class ToSnowflake {

    public static void convert() {
        Logger logger = Logger.getLogger("org.stdneprov.bigdata.lab2.ToSnowflake");
        logger.setLevel(Level.DEBUG);
        // Создаем SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("BigDataSpark|ToSnowflake")
            .config("spark.master", "spark://spark-master:7077")
            .config("spark.submit.deployMode", "client")
            .getOrCreate();

        // Параметры подключения к PostgreSQL
        String jdbcUrl = "jdbc:postgresql://postgres:5432/lab2";
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "stdneprov");
        connectionProperties.put("password", "password");
        connectionProperties.put("driver", "org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "stdneprov", "password");
        Statement stmt = conn.createStatement()) {

        // Очистка таблиц в правильном порядке (сначала зависимые, затем базовые)
        String[] truncateCommands = {
            // Фактические таблицы (должны быть очищены первыми)
            "TRUNCATE TABLE snowflake.fact_sales RESTART IDENTITY CASCADE",
            
            // Основные dimension таблицы
            "TRUNCATE TABLE snowflake.dim_customers RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_sellers RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_products RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_stores RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_pets RESTART IDENTITY CASCADE",
            
            // Справочные таблицы
            "TRUNCATE TABLE snowflake.dim_suppliers RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_pet_breeds RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_pet_types RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_product_categories RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_pet_categories RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_countries RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_states RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_cities RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_colors RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_brands RESTART IDENTITY CASCADE",
            "TRUNCATE TABLE snowflake.dim_materials RESTART IDENTITY CASCADE"
        };

        for (String cmd : truncateCommands) {
            try {
                stmt.executeUpdate(cmd);
                logger.info("Successfully truncated: " + cmd);
            } catch (SQLException e) {
                logger.error("Error truncating table: " + cmd, e);
            }
        }
    } catch (SQLException e) {
        logger.error("Database connection error during truncate", e);
    }

        // Загрузка данных из PostgreSQL
        Dataset<Row> mockData = spark.read()
            .jdbc(jdbcUrl, "mock_data", connectionProperties).withColumn("additional_to_id", 
                functions.floor((functions.col("id").minus(1)).divide(1000)).multiply(1000)
            );

        mockData.select(
                functions.col("customer_pet_type").alias("name"))
                .distinct()
                .where("customer_pet_type IS NOT NULL")
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_pet_types", connectionProperties);
        
        logger.info("snowflake.dim_pet_types");

        // 2. Вставка в snowflake.dim_pet_breeds (с предварительной загрузкой snowflake.dim_pet_types)
        Dataset<Row> petTypes = spark.read()
                .jdbc(jdbcUrl, "snowflake.dim_pet_types", connectionProperties);

        mockData.join(petTypes, 
                mockData.col("customer_pet_type").equalTo(petTypes.col("name")), "left")
                .select(
                        petTypes.col("pet_type_id"),
                        mockData.col("customer_pet_breed").alias("name"))
                .where("customer_pet_breed IS NOT NULL")
                .distinct()
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_pet_breeds", connectionProperties);
        logger.info("snowflake.dim_pet_breeds");

        // 3. Вставка в snowflake.dim_pets
        Dataset<Row> petBreeds = spark.read()
                .jdbc(jdbcUrl, "snowflake.dim_pet_breeds", connectionProperties);

        mockData.withColumn("rn", functions.row_number().over(
            Window.partitionBy(functions.col("sale_customer_id").plus(functions.col("additional_to_id"))).orderBy(functions.desc("id"))))
                .filter("rn = 1 and supplier_name IS NOT NULL")
                .drop("rn")
                .join(petTypes, 
                        mockData.col("customer_pet_type").equalTo(petTypes.col("name")), "left")
                .join(petBreeds, 
                        mockData.col("customer_pet_breed").equalTo(petBreeds.col("name"))
                        .and(petTypes.col("pet_type_id").equalTo(petBreeds.col("pet_type_id"))), "left")
                .select(
                        mockData.col("sale_customer_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)).alias("pet_id"),
                        petBreeds.col("pet_breed_id").alias("breed_id"),
                        mockData.col("customer_pet_name").alias("name"))
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_pets", connectionProperties);
        logger.info("snowflake.dim_pets");

        // 4. Вставка в snowflake.dim_countries
        Dataset<Row> customerCountries = mockData.select(
                functions.col("customer_country").alias("name"))
                .where("customer_country IS NOT NULL");
        
        Dataset<Row> sellerCountries = mockData.select(
                functions.col("seller_country").alias("name"))
                .where("seller_country IS NOT NULL");
        
        Dataset<Row> storeCountries = mockData.select(
                functions.col("store_country").alias("name"))
                .where("store_country IS NOT NULL");
        
        Dataset<Row> supplierCountries = mockData.select(
                functions.col("supplier_country").alias("name"))
                .where("supplier_country IS NOT NULL");

        customerCountries.union(sellerCountries)
                .union(storeCountries)
                .union(supplierCountries)
                .distinct()
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_countries", connectionProperties);
        logger.info("snowflake.dim_countries");

        // 5. Вставка в snowflake.dim_states
        mockData.select(
                functions.col("store_state").alias("name"))
                .where("store_state IS NOT NULL")
                .distinct()
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_states", connectionProperties);

        logger.info("snowflake.dim_states");

        // 6. Вставка в snowflake.dim_cities
        Dataset<Row> storeCities = mockData.select(
                functions.col("store_city").alias("name"))
                .where("store_city IS NOT NULL");

        
        Dataset<Row> supplierCities = mockData.select(
                functions.col("supplier_city").alias("name"))
                .where("supplier_city IS NOT NULL");

        storeCities.union(supplierCities)
                .distinct()
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_cities", connectionProperties);
        
        logger.info("snowflake.dim_cities");

        // 7. Вставка в snowflake.dim_customers
        Dataset<Row> countries = spark.read()
                .jdbc(jdbcUrl, "snowflake.dim_countries", connectionProperties);

        mockData.withColumn("rn", functions.row_number().over(
            Window.partitionBy(functions.col("sale_customer_id").plus(functions.col("additional_to_id"))).orderBy(functions.desc("id"))))
                .filter("rn = 1 and supplier_name IS NOT NULL")
                .drop("rn")
                .join(countries, 
                        mockData.col("customer_country").equalTo(countries.col("name")), "left")
                .select(
                        mockData.col("sale_customer_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)).alias("customer_id"),
                        mockData.col("customer_first_name").alias("first_name"),
                        mockData.col("customer_last_name").alias("last_name"),
                        mockData.col("customer_email").alias("email"),
                        mockData.col("customer_age").alias("age"),
                        countries.col("country_id"),
                        mockData.col("customer_postal_code").alias("postal_code"),
                        mockData.col("sale_customer_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)).alias("pet_id"))
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_customers", connectionProperties);

        logger.info("snowflake.dim_customers");

        // 4. snowflake.dim_sellers
        
        WindowSpec sellerWindow = Window.partitionBy(
                mockData.col("sale_seller_id").plus(
                        functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)))
                .orderBy(functions.desc("id"));
        
        mockData.withColumn("rn", functions.row_number().over(sellerWindow))
                .filter("rn = 1 AND sale_seller_id IS NOT NULL")
                .drop("rn")
                .join(countries, mockData.col("seller_country").equalTo(countries.col("name")), "left")
                .select(
                        mockData.col("sale_seller_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)).alias("seller_id"),
                        mockData.col("seller_first_name").alias("first_name"),
                        mockData.col("seller_last_name").alias("last_name"),
                        mockData.col("seller_email").alias("email"),
                        countries.col("country_id"),
                        mockData.col("seller_postal_code").alias("postal_code"))
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_sellers", connectionProperties);

        // 5. snowflake.dim_stores
        Dataset<Row> states = spark.read().jdbc(jdbcUrl, "snowflake.dim_states", connectionProperties);
        Dataset<Row> cities = spark.read().jdbc(jdbcUrl, "snowflake.dim_cities", connectionProperties);
        
        WindowSpec storeWindow = Window.partitionBy("store_name", "store_email")
                                     .orderBy(functions.desc("id"));
        
        mockData.withColumn("rn", functions.row_number().over(storeWindow))
                .filter("rn = 1 AND store_name IS NOT NULL")
                .drop("rn")
                .join(countries, mockData.col("store_country").equalTo(countries.col("name")), "left")
                .join(states, mockData.col("store_state").equalTo(states.col("name")), "left")
                .join(cities, mockData.col("store_city").equalTo(cities.col("name")), "left")
                .select(
                        mockData.col("store_name").alias("name"),
                        mockData.col("store_location").alias("location"),
                        countries.col("country_id"),
                        states.col("state_id"),
                        cities.col("city_id"),
                        mockData.col("store_phone").alias("phone"),
                        mockData.col("store_email").alias("email"))
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_stores", connectionProperties);

        // 6. snowflake.dim_products
        Dataset<Row> productCategories = spark.read().jdbc(jdbcUrl, "snowflake.dim_product_categories", connectionProperties);
        Dataset<Row> petCategories = spark.read().jdbc(jdbcUrl, "snowflake.dim_pet_categories", connectionProperties);
        Dataset<Row> colors = spark.read().jdbc(jdbcUrl, "snowflake.dim_colors", connectionProperties);
        Dataset<Row> brands = spark.read().jdbc(jdbcUrl, "snowflake.dim_brands", connectionProperties);
        Dataset<Row> materials = spark.read().jdbc(jdbcUrl, "snowflake.dim_materials", connectionProperties);
        Dataset<Row> suppliers = spark.read().jdbc(jdbcUrl, "snowflake.dim_suppliers", connectionProperties);
        
        WindowSpec productWindow = Window.partitionBy(
                mockData.col("sale_product_id").plus(
                        functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)))
                .orderBy(functions.desc("id"));
        
        mockData.withColumn("rn", functions.row_number().over(productWindow))
                .filter("rn = 1 AND sale_product_id IS NOT NULL")
                .drop("rn")
                .join(productCategories, mockData.col("product_category").equalTo(productCategories.col("name")), "left")
                .join(petCategories, mockData.col("pet_category").equalTo(petCategories.col("name")), "left")
                .join(colors, mockData.col("product_color").equalTo(colors.col("name")), "left")
                .join(brands, mockData.col("product_brand").equalTo(brands.col("name")), "left")
                .join(materials, mockData.col("product_material").equalTo(materials.col("name")), "left")
                .join(suppliers, mockData.col("supplier_email").equalTo(suppliers.col("email")), "left")
                .select(
                        mockData.col("sale_product_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000)).alias("product_id"),
                        mockData.col("product_name").alias("name"),
                        petCategories.col("pet_category_id"),
                        productCategories.col("category_id"),
                        mockData.col("product_price").alias("price"),
                        mockData.col("product_weight").alias("weight"),
                        colors.col("color_id"),
                        mockData.col("product_size").alias("size"),
                        brands.col("brand_id"),
                        materials.col("material_id"),
                        mockData.col("product_description").alias("description"),
                        mockData.col("product_rating").alias("rating"),
                        mockData.col("product_reviews").alias("reviews"),
                        functions.to_date(mockData.col("product_release_date"), "M/d/yyyy").alias("release_date"),
                        functions.to_date(mockData.col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
                        suppliers.col("supplier_id"))
                .write().mode("append").jdbc(jdbcUrl, "snowflake.dim_products", connectionProperties);

        // 7. snowflake.fact_sales
        Dataset<Row> customers = spark.read().jdbc(jdbcUrl, "snowflake.dim_customers", connectionProperties);
        Dataset<Row> sellers = spark.read().jdbc(jdbcUrl, "snowflake.dim_sellers", connectionProperties);
        Dataset<Row> products = spark.read().jdbc(jdbcUrl, "snowflake.dim_products", connectionProperties);
        Dataset<Row> stores = spark.read().jdbc(jdbcUrl, "snowflake.dim_stores", connectionProperties);
        
        mockData.join(customers, 
                        mockData.col("sale_customer_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000))
                        .equalTo(customers.col("customer_id")), "left")
                .join(sellers, 
                        mockData.col("sale_seller_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000))
                        .equalTo(sellers.col("seller_id")), "left")
                .join(products, 
                        mockData.col("sale_product_id").plus(
                                functions.floor(mockData.col("id").minus(1).divide(1000)).multiply(1000))
                        .equalTo(products.col("product_id")), "left")
                .join(stores, mockData.col("store_email").equalTo(stores.col("email")), "left")
                .select(
                        customers.col("customer_id"),
                        sellers.col("seller_id"),
                        products.col("product_id"),
                        stores.col("store_id"),
                        mockData.col("sale_quantity").alias("quantity"),
                        mockData.col("sale_total_price").alias("total_price"),
                        functions.to_date(mockData.col("sale_date"), "M/d/yyyy").alias("date"))
                .where("quantity > 0")
                .write().mode("append").jdbc(jdbcUrl, "snowflake.fact_sales", connectionProperties);

        spark.stop();
    }
}