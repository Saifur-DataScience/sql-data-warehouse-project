-- Finally, we clean & transform the Bronze crm_sales_details table
-- And insert the data into the Silver table

USE DataWarehouse; 

SELECT *
FROM Bronze.crm_sales_details; 


-- Let's check for the data quality issues in the columns

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_ord_num IS NULL OR sls_ord_num != TRIM(sls_ord_num); 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_prd_key IS NULL OR sls_prd_key != TRIM(sls_prd_key); 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_cust_id IS NULL; 


-- We have order, ship & due dates, which are actually as INT
-- Let's transform and correct these columns

-- But first, let's quality check these columns to find anomalies

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt < 19000101
OR sls_order_dt > 20500101; 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_ship_dt <= 0
OR LEN(sls_ship_dt) != 8
OR sls_ship_dt < 19000101
OR sls_ship_dt > 20500101; 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_due_dt <= 0
OR LEN(sls_due_dt) != 8
OR sls_due_dt < 19000101
OR sls_due_dt > 20500101; 


-- We have issues with only the Order Date
-- Let's clean Order Date and apply same logic for other 2 as well for any future anomalies

SELECT
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	Case WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt, 
	Case WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt, 
	Case WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price
FROM Bronze.crm_sales_details; 


-- Now check for invalid date order

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; 


-- Finally, let's check the quality for sales, quantity & price columns
-- Expectations : All these columns should not container zero, null or negative ints

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_sales <= 0 OR sls_sales IS NULL; 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_quantity <= 0 OR sls_quantity IS NULL; 

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_price <= 0 OR sls_price IS NULL; 

-- Also, the sales must be equal to price * quantity

SELECT *
FROM Bronze.crm_sales_details
WHERE sls_sales != sls_price*sls_quantity; 


-- We can derive the Price column after we fix the sales column
-- Where we have NULL in sales column, we still have price, so we can fix the NULLs accordingly

SELECT
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	Case WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt, 
	Case WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt, 
	Case WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt, 
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales, 
	sls_quantity, 
	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM Bronze.crm_sales_details; 


-- Correct the datatype for Date columns to Date instead if INT

IF OBJECT_ID ('Silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE Silver.crm_sales_details; 

CREATE TABLE Silver.crm_sales_details (
	sls_ord_num NVARCHAR (50), 
	sls_prd_key NVARCHAR (50), 
	sls_cust_id INT, 
	sls_order_dt DATE, 
	sls_ship_dt DATE, 
	sls_due_dt DATE, 
	sls_sales INT, 
	sls_quantity INT, 
	sls_price INT, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
); 


-- Insert the data from Bronze to Silver layer table

INSERT INTO Silver.crm_sales_details (
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt, 
	sls_ship_dt, 
	sls_due_dt, 
	sls_sales, 
	sls_quantity, 
	sls_price
)

SELECT
	sls_ord_num, 
	sls_prd_key, 
	sls_cust_id, 
	Case WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt, 
	Case WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt, 
	Case WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt, 
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales, 
	sls_quantity, 
	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
FROM Bronze.crm_sales_details; 


-- Check the data quality in the Silver Sales Details table

SELECT *
FROM Silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
OR sls_price IS NULL OR sls_price <= 0; 

SELECT * FROM Silver.crm_sales_details; 
