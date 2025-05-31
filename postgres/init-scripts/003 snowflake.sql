CREATE SCHEMA snowflake;

CREATE TABLE snowflake.dim_pet_types (
    pet_type_id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE snowflake.dim_pet_breeds (
    pet_breed_id SERIAL PRIMARY KEY,
    pet_type_id INTEGER REFERENCES snowflake.dim_pet_types(pet_type_id) ON DELETE SET NULL,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_pets (
    pet_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    breed_id INTEGER REFERENCES snowflake.dim_pet_breeds(pet_breed_id) ON DELETE SET NULL
);

CREATE TABLE snowflake.dim_countries (
    country_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_states (
    state_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_cities (
    city_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    age INTEGER,
    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,
    postal_code VARCHAR(20),
    pet_id INTEGER REFERENCES snowflake.dim_pets(pet_id) ON DELETE SET NULL
);

CREATE TABLE snowflake.dim_sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,
    postal_code VARCHAR(20)
);

CREATE TABLE snowflake.dim_stores (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,
    state_id INTEGER REFERENCES snowflake.dim_states(state_id) ON DELETE SET NULL,
    city_id INTEGER REFERENCES snowflake.dim_cities(city_id) ON DELETE SET NULL,
    phone VARCHAR(50),
    email VARCHAR(255)
);

CREATE TABLE snowflake.dim_product_categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_pet_categories (
    pet_category_id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE snowflake.dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city_id INTEGER REFERENCES snowflake.dim_cities(city_id) ON DELETE SET NULL,
    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL
);

CREATE TABLE snowflake.dim_colors (
    color_id SERIAL PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE snowflake.dim_brands (
    brand_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_materials (
    material_id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TABLE snowflake.dim_products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    pet_category_id INTEGER REFERENCES snowflake.dim_pet_categories(pet_category_id) ON DELETE SET NULL,
    category_id INTEGER REFERENCES snowflake.dim_product_categories(category_id) ON DELETE SET NULL,
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    color_id INTEGER REFERENCES snowflake.dim_colors(color_id) ON DELETE SET NULL,
    size VARCHAR(50),
    brand_id INTEGER REFERENCES snowflake.dim_brands(brand_id) ON DELETE SET NULL,
    material_id INTEGER REFERENCES snowflake.dim_materials(material_id) ON DELETE SET NULL,
    description TEXT,
    rating DECIMAL(3,1),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    supplier_id INTEGER REFERENCES snowflake.dim_suppliers(supplier_id) ON DELETE SET NULL
);

CREATE TABLE snowflake.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES snowflake.dim_customers(customer_id) ON DELETE SET NULL,
    seller_id INTEGER REFERENCES snowflake.dim_sellers(seller_id) ON DELETE SET NULL,
    product_id INTEGER REFERENCES snowflake.dim_products(product_id) ON DELETE SET NULL,
    store_id INTEGER REFERENCES snowflake.dim_stores(store_id) ON DELETE SET NULL,
    quantity INTEGER,
    total_price DECIMAL(10,2),
    date DATE
);
