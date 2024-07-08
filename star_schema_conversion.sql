--
--SQL code to convert tables into Star Schema
--

-- Find maximum length of text columns
SELECT 
	MAX(length(CAST(card_number AS Text))) AS max_card_number
	,MAX(length(CAST(store_code AS Text))) AS max_store_code
	,MAX(length(CAST(product_code AS Text))) AS max_product_code
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
	MAX(length(CAST(country_code AS Text))) AS max_card_number
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
	MAX(length(CAST(store_code AS Text))) AS max_store_code
	,MAX(length(CAST(country_code AS Text))) AS max_country_code
FROM dim_store_details;

-- "max_store_code"	"max_country_code"
-- 12	            2

-- Casts dim_store_details columns to correct data typesand drop index column if exists
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING CAST(longitude AS FLOAT),
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date AS DATE),
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
	ALTER COLUMN opening_date TYPE DATE USING CAST(opening_date AS DATE),
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
	MAX(length(CAST(product_code AS Text))) AS max_product_code
	,MAX(length(CAST(weight_class AS Text))) AS max_weight_class
	,MAX(length(CAST("EAN" AS Text))) AS max_EAN -- note use of double quote for capital letters EAN column name.
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

-- Find maximum length of text columns in dim_date_times
SELECT 
	MAX(length(CAST(month AS Text))) AS max_month
	,MAX(length(CAST(year AS Text))) AS max_year
	,MAX(length(CAST(day AS Text))) AS max_day
	,MAX(length(CAST(time_period AS Text))) AS max_time_period
FROM dim_date_times;

-- "max_month"	"max_year"	"max_day"	"max_time_period"
-- 2	        4	        2	        10

-- Casts dim_date_times table columns to correct data types
ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2),
	ALTER COLUMN year TYPE VARCHAR(4),
	ALTER COLUMN day TYPE VARCHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING CAST(date_uuid AS UUID);

-- Sanity check - Find maximum length card number - found it includes ??, needs cleaning in python.
SELECT 
	MAX(length(CAST(card_number AS Text))) AS max_card_number
	,MAX(length(CAST(expiry_date AS Text))) AS max_expiry_date
	,card_number
FROM dim_card_details
GROUP BY card_number
ORDER BY length(max(cast(card_number AS TEXT))) DESC
LIMIT 1;

-- Sanity check - Find card_numbers not including numerical characters. found 26 entries with ? at start.
select * from dim_card_details where card_number !~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$'
select * from dim_card_details where card_number !~ E'^[0-9]+\$'

-- Find maximum length of text columns in dim_card_details
SELECT 
	MAX(length(CAST(card_number AS Text))) AS max_card_number
	,MAX(length(CAST(expiry_date AS Text))) AS max_expiry_date
FROM dim_card_details

-- "max_card_number"	"max_expiry_date"
-- 19	                5


-- Casts dim_card_details table columns to correct data types
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING CAST(date_payment_confirmed AS DATE);

-- Modify dim tables with primary keys for columns with equivalents in the orders_table
ALTER TABLE dim_card_details
	ADD CONSTRAINT pk_card_nuber PRIMARY KEY (card_number);
	
ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid);
	
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code);
	
ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code);
	
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid);


-- adding foreign key consraints raises errors due to some cards in orders_table are missing in dim_card_detaisl table.

-- locate card_number values that are present in orders_table but not in dim_card_details table
SELECT ot.card_number 
FROM orders_table AS ot
LEFT JOIN dim_card_details AS ct
ON ot.card_number = ct.card_number
WHERE ct.card_number IS NULL;

-- insert those card_number values in to the dim_card_details
INSERT INTO dim_card_details (card_number)
SELECT DISTINCT orders_table.card_number
FROM orders_table
WHERE orders_table.card_number NOT IN 
	(SELECT dim_card_details.card_number
	FROM dim_card_details);


-- add the foreign keys to the orders_table now.
ALTER TABLE orders_table
    ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
	ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
	ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
	ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
	ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);