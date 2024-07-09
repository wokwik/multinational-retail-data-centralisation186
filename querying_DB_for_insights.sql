-- SQL Queries for gleaning insights from the warehouse

-- 1
-- which countries we currently operate in and which country now has the most stores.
SELECT 
	country_code
	, COUNT(country_code) AS total_no_stores
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

SELECT 
	locality
	, COUNT(locality) AS total_no_stores
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

SELECT 
	ROUND(SUM(pt.product_price * ot.product_quantity)::NUMERIC,2) AS total_sales
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
	COUNT(ot.product_quantity) AS numbers_of_sales
	, SUM(ot.product_quantity) AS product_quantity_count
	, CASE 
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
	st.store_type AS store_type
	, ROUND(SUM(ot.product_quantity * pt.product_price)::NUMERIC,2) AS total_sales
	, ROUND((SUM(ot.product_quantity * pt.product_price)	/
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


-- 6
-- The company stakeholders want assurances that the company has been doing well recently.
-- Find which months in which years have had the most sales historically.

SELECT 
	dt.year
	, dt.month
	, ROUND(sum(ot.product_quantity*pt.product_price)::NUMERIC,2) AS revenue
FROM orders_table AS ot
	JOIN dim_date_times AS dt ON  ot.date_uuid = dt.date_uuid
	JOIN dim_products AS pt ON  ot.product_code = pt.product_code
	JOIN dim_store_details AS st ON ot.store_code = st.store_code
GROUP BY dt.month, dt.year
ORDER BY SUM(ot.product_quantity*pt.product_price) DESC
LIMIT 10;

-- "year"	"month"	"revenue"
-- "1994"	"3"		27936.77
-- "2019"	"1"		27356.14
-- "2009"	"8"		27091.67
-- "1997"	"11"	26679.98
-- "2018"	"12"	26310.97
-- "2019"	"8"		26277.72
-- "2017"	"9"		26236.67
-- "2010"	"5"		25798.12
-- "1996"	"8"		25648.29
-- "2000"	"1"		25614.54


-- 7
-- The operations team would like to know the overall staff numbers in each location around the world.
-- Perform a query to determine the staff numbers in each of the countries the company sells in.

SELECT 
	SUM(staff_numbers) AS total_staff_numbers
	, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- "total_staff_numbers"	"country_code"
-- 13307					"GB"
-- 6123						"DE"
-- 1384						"US"


-- 8
-- The sales team is looking to expand their territory in Germany.
-- Determine which type of store is generating the most sales in Germany.

SELECT 
	ROUND(sum(ot.product_quantity*pt.product_price)::NUMERIC, 2) AS total_sales
	, st.store_type
	, MAX(st.country_code) AS country_code
FROM orders_table AS ot
	LEFT JOIN dim_store_details AS st ON ot.store_code = st.store_code
	LEFT JOIN dim_products AS pt ON ot.product_code = pt.product_code
WHERE st.country_code = 'DE'
GROUP BY st.store_type, st.country_code
ORDER BY total_sales;

-- "total_sales"	"store_type"	"country_code"
-- 198373.57		"Outlet"		"DE"
-- 247634.20		"Mall Kiosk"	"DE"
-- 384625.03		"Super Store"	"DE"
-- 1109909.59		"Local"			"DE"


-- 9
-- Sales would like the get an accurate metric for how quickly the company is making sales.
-- Determine the average time taken between each sale grouped by year

WITH time_table(hour, minutes, seconds, day, month, year, date_uuid) AS (
	SELECT 
		EXTRACT(hour from CAST(timestamp AS time)) AS hour,
		EXTRACT(minute from CAST(timestamp AS time)) AS minutes,
		EXTRACT(second from CAST(timestamp AS time)) AS seconds,
		day AS day,
		month AS month,
		year AS year,
		date_uuid
	FROM dim_date_times
),

timestamp_table(timestamp, date_uuid, year) AS (
	SELECT MAKE_TIMESTAMP(CAST(time_table.year AS int), CAST(time_table.month AS int),
						  CAST(time_table.day AS int), CAST(time_table.hour AS int),	
						  CAST(time_table.minutes AS int), CAST(time_table.seconds AS float)) AS order_timestamp,
		time_table.date_uuid AS date_uuid,
		time_table.year AS year
	FROM time_table
),

time_stamp_diffs(year, time_diff) AS (
	SELECT 
		timestamp_table.year
		, timestamp_table.timestamp - LAG(timestamp_table.timestamp) OVER (ORDER BY timestamp_table.timestamp ASC) AS time_diff
	FROM orders_table
	JOIN timestamp_table ON orders_table.date_uuid = timestamp_table.date_uuid
),

year_time_diffs(year, average_time_diff) AS (
	SELECT year, AVG(time_diff) AS average_time_diff
	FROM time_stamp_diffs
	GROUP BY year
	ORDER BY average_time_diff desc
)
	
SELECT 
	year
	, CONCAT('hours: ', EXTRACT(HOUR FROM average_time_diff),
					' minutes: ', EXTRACT(MINUTE FROM average_time_diff),
				   ' seconds: ', CAST(EXTRACT(SECOND FROM average_time_diff) AS int),
				   ' milliseconds: ', CAST(EXTRACT(MILLISECOND FROM average_time_diff) AS int))
FROM year_time_diffs;

-- "year"	"concat"
-- "2013"	"hours: 2  minutes: 17  seconds: 12  milliseconds: 12300"
-- "1993"	"hours: 2  minutes: 15  seconds: 36  milliseconds: 35857"
-- "2002"	"hours: 2  minutes: 13  seconds: 50  milliseconds: 50413"
-- "2022"	"hours: 2  minutes: 13  seconds: 6  milliseconds: 6314"
-- "2008"	"hours: 2  minutes: 13  seconds: 3  milliseconds: 2803"
-- "1995"	"hours: 2  minutes: 12  seconds: 59  milliseconds: 58973"
-- "2016"	"hours: 2  minutes: 12  seconds: 58  milliseconds: 58125"
-- "2011"	"hours: 2  minutes: 12  seconds: 19  milliseconds: 19018"
-- "2020"	"hours: 2  minutes: 12  seconds: 4  milliseconds: 3535"
-- "2012"	"hours: 2  minutes: 11  seconds: 58  milliseconds: 58069"
-- "2021"	"hours: 2  minutes: 11  seconds: 56  milliseconds: 56200"
-- "2009"	"hours: 2  minutes: 11  seconds: 18  milliseconds: 18414"
-- "2010"	"hours: 2  minutes: 11  seconds: 14  milliseconds: 13985"
-- "2007"	"hours: 2  minutes: 11  seconds: 9  milliseconds: 8939"
-- "1999"	"hours: 2  minutes: 11  seconds: 7  milliseconds: 6563"
-- "1996"	"hours: 2  minutes: 10  seconds: 59  milliseconds: 59163"
-- "2000"	"hours: 2  minutes: 10  seconds: 54  milliseconds: 54499"
-- "2019"	"hours: 2  minutes: 10  seconds: 47  milliseconds: 47080"
-- "1994"	"hours: 2  minutes: 10  seconds: 44  milliseconds: 43553"
-- "2001"	"hours: 2  minutes: 10  seconds: 39  milliseconds: 38954"
-- "2018"	"hours: 2  minutes: 10  seconds: 36  milliseconds: 35807"
-- "2004"	"hours: 2  minutes: 10  seconds: 33  milliseconds: 32996"
-- "2006"	"hours: 2  minutes: 10  seconds: 20  milliseconds: 20328"
-- "2014"	"hours: 2  minutes: 10  seconds: 8  milliseconds: 7507"
-- "1997"	"hours: 2  minutes: 9  seconds: 58  milliseconds: 58199"
-- "2015"	"hours: 2  minutes: 9  seconds: 37  milliseconds: 37417"
-- "1992"	"hours: 2  minutes: 9  seconds: 32  milliseconds: 32063"
-- "2005"	"hours: 2  minutes: 8  seconds: 60  milliseconds: 59661"
-- "2017"	"hours: 2  minutes: 8  seconds: 47  milliseconds: 46828"
-- "2003"	"hours: 2  minutes: 8  seconds: 45  milliseconds: 45492"
-- "1998"	"hours: 2  minutes: 8  seconds: 8  milliseconds: 7956"