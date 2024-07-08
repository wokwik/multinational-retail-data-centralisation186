--
--SQL code to convert tables into Star Schema
--

-- Find maximum length of text columns
SELECT 
	MAX(length(CAST(card_number AS Text)))as max_card_number
	,MAX(length(CAST(store_code AS Text)))as max_store_code
	,MAX(length(CAST(product_code AS Text)))as max_product_code
FROM orders_table

-- "max_card_number"	"max_store_code"	"max_product_code"
-- 19	                12	                11


-- Change orders_table to hold correct data types and drop not required columns if they still exist from python clean code!
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid AS UUID),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid AS UUID),
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT,
	DROP COLUMN IF EXISTS index,
	DROP COLUMN IF EXISTS level_0;


-- Find maximum length of county code columns
SELECT 
	MAX(length(CAST(country_code AS Text)))as max_card_number
FROM dim_users

-- "max_card_number"
-- 3

-- Change orders_table to hold correct data types
ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(225),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING CAST(date_of_birth AS DATE),
	ALTER COLUMN country_code TYPE VARCHAR(3),
	ALTER COLUMN user_uuid TYPE UUID USING CAST(user_uuid AS UUID),
	ALTER COLUMN join_date TYPE DATE USING CAST(join_date AS DATE);


-- select the maximum length of store_code and country_code
SELECT
	MAX(length(CAST(store_code AS Text)))as max_store_code
	,MAX(length(CAST(country_code AS Text)))as max_country_code
FROM dim_store_details;

-- "max_store_code"	"max_country_code"
-- 12	            2

-- Casts dim_store_details columns to correct data typesand drop index column if exists
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING CAST(longitude AS FLOAT),
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date as DATE),
	ALTER COLUMN store_type TYPE VARCHAR(255) DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING CAST(latitude AS FLOAT),
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255),
    DROP COLUMN IF EXISTS index;

-- Casts dim_store_details columns to correct data types and drop index column if exists
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING (CASE WHEN longitude = 'N/A' THEN NULL ELSE longitude::FLOAT END),
	ALTER COLUMN locality TYPE VARCHAR(255) USING (CASE WHEN locality = 'N/A' THEN NULL ELSE locality::VARCHAR(255) END),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date as DATE),
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING (CASE WHEN latitude = 'N/A' THEN NULL ELSE latitude::FLOAT END),
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255),
    DROP COLUMN IF EXISTS index;

-- might as well change address N/A to null
UPDATE dim_store_details 
	SET address = NULL
WHERE address = 'N/A';

-- check change address to Null worked
SELECT * FROM public.dim_store_details
	where address is NULL

-- remove £ character from product_price column using SQL.
UPDATE dim_products 
    SET product_price = REPLACE(product_price, '£', '');

-- Add weight_class new column
ALTER TABLE dim_products 
    ADD COLUMN weight_class VARCHAR;

-- set weight_class values based on weight values
UPDATE dim_products
	SET weight_class =
		CASE 
			WHEN weight < 2.0 THEN 'Light'
			WHEN weight >= 2 
				AND weight < 40 THEN 'Mid_Sized'
			WHEN weight >= 40 
				AND weight <140 THEN 'Heavy'
			WHEN weight >= 140 THEN 'Truck_Required'
		END;

-- Find maximum length of text columns in dim_products
SELECT 
	MAX(length(CAST(product_code AS Text)))as max_product_code
	,MAX(length(CAST(weight_class AS Text)))as max_weight_class
	,MAX(length(CAST("EAN" AS Text)))as max_EAN -- note use of double quote for capital letters EAN column name.
FROM dim_products

-- "max_product_code"	"max_weight_class"	"max_ean"
-- 11	                14	                17

-- Renames removed to still_available column in dim_products
ALTER TABLE dim_products 
	RENAME COLUMN removed to still_available;

-- Casts dim_products table columns to correct data types and drop index column if exists
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT USING CAST(product_price AS FLOAT),
	ALTER COLUMN weight TYPE FLOAT USING CAST(weight AS FLOAT),
	ALTER COLUMN "EAN" TYPE VARCHAR(17), -- note use of double quote for capital letters EAN column name.
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING CAST(date_added AS DATE),
	ALTER COLUMN uuid TYPE UUID USING CAST(uuid AS UUID),
	ALTER COLUMN still_available TYPE BOOLEAN USING (CASE WHEN still_available = 'Still_avaliable' THEN TRUE ELSE FALSE END),
	ALTER COLUMN weight_class TYPE VARCHAR(14),
	DROP COLUMN IF EXISTS index;

