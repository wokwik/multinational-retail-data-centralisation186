-- SQL Queries for gleaning insights from the warehouse

-- 1
-- which countries we currently operate in and which country now has the most stores.
SELECT 	country_code
		,COUNT(country_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- "country_code"	"total_no_stores"
-- "GB"	            266
-- "DE"	            141
-- "US"	            34

-- I got 1 store more for GB, where the instructions state 265. that is because I decided to clean staff_number raw by removing 'n' from 3n9 and not delete the row!


-- 2
-- The business stakeholders would like to know which locations currently have the most stores.
-- They would like to close some stores before opening more in other locations.
-- Find out which locations have the most stores currently.

SELECT locality, COUNT(locality) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
limit 10;

-- "locality"	    "total_no_stores"
-- "Chapletown"	    14
-- "Belper"	        13
-- "Bushey"	        12
-- "Exeter"	        11
-- "Rutherglen"	    10
-- "Arbroath"	    10
-- "High Wycombe"	10
-- "Surbiton"	    9
-- "Lancing"	    9
-- "Aberdeen"	    9



-- 3
-- Query the database to find out which months have produced the most sales.

SELECT ROUND(SUM(pt.product_price * ot.product_quantity)::NUMERIC,2) AS total_sales
	, dt.month
FROM orders_table AS ot
	LEFT JOIN dim_date_times AS dt ON ot.date_uuid = dt.date_uuid
	LEFT JOIN dim_products AS pt ON ot.product_code = pt.product_code
GROUP BY dt.month
ORDER BY total_sales DESC
LIMIT 10;

-- "total_sales"	"month"
-- 673295.68	    "8"
-- 668041.45	    "1"
-- 657335.84	    "10"
-- 650321.43	    "5"
-- 645741.70	    "7"
-- 645463.00	    "3"
-- 635578.99	    "6"
-- 635329.09	    "12"
-- 633993.62	    "9"
-- 630757.08	    "11"


-- 4
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made for online and offline purchases.

SELECT 
	COUNT(ot.product_quantity) AS numbers_of_sales,
	SUM(ot.product_quantity) AS product_quantity_count,
	CASE 
		WHEN st.store_type = 'Web Portal' THEN 'Web' 
		ELSE 'Offline'
	END AS location
FROM orders_table AS ot
	LEFT JOIN dim_store_details AS st ON ot.store_code = st.store_code
GROUP BY location
ORDER BY product_quantity_count;

-- "numbers_of_sales"	"product_quantity_count"	"location"
-- 26957	            107739	                    "Web"
-- 93166	            374047	                    "Offline"


-- 5
-- The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.

SELECT 
	st.store_type AS store_type,
	ROUND(SUM(ot.product_quantity * pt.product_price)::NUMERIC,2) AS total_sales,
	
	ROUND((SUM(ot.product_quantity * pt.product_price)	/
			(SELECT 
					SUM(ot.product_quantity * pt.product_price) 
				FROM orders_table AS ot
	 				LEFT JOIN dim_products AS pt ON ot.product_code = pt.product_code
			)*100)::NUMERIC, 2) AS "percentage_total(%)"

FROM orders_table AS ot
	LEFT JOIN dim_store_details AS st ON ot.store_code = st.store_code
	LEFT JOIN dim_products AS pt ON ot.product_code = pt.product_code
GROUP BY store_type
ORDER BY total_sales DESC;

-- "store_type"	    "total_sales"	"percentage_total(%)"
-- "Local"          3440896.52	    44.56
-- "Web Portal"	    1726547.05	    22.36
-- "Super Store"	1224293.65	    15.85
-- "Mall Kiosk"	    698791.61	    9.05
-- "Outlet"	        631804.81	    8.18

-- numbers are a little different from project instructions, because I suspect I kept payments where card numbers have ?, whereas instructors probably removed the entire rows for those cards!

