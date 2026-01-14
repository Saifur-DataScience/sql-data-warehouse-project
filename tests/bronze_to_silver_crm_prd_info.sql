-- Check for Duplicates OR Nulls in the Primary Key
-- Expectations : To not have any Nulls or Duplicates
USE DataWarehouse; 
SELECT * FROM Bronze.crm_prd_info; 

SELECT prd_id, COUNT(*) 
FROM Bronze.crm_prd_info
GROUP BY  prd_id
Having COUNT(*) > 1 OR prd_id IS NULL; 


-- Cleaning & transforming the table to insert values in Silver layer

SELECT prd_id, 
	   -- We need Category ID to join it with a table from ERP
	   REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
	   -- Creating the Prd Key separately without the cat id
	   SUBSTRING(TRIM(prd_key), 7, LEN(prd_key)) AS prd_key, 
	   TRIM(prd_nm) AS prd_nm, 
	   -- Replace Null values in cost to zero for easier aggregations
	   ISNULL(prd_cost, 0) AS prd_cost, 
	   -- Standardize prd line column with values that have full name instead of abbreviations
	   CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'N/A'
	   END AS prd_line, 
	   prd_start_dt, 
	   prd_end_dt
FROM Bronze.crm_prd_info; 


-- Check for invalida date enteries

SELECT *
FROM Bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt; 


/*We have a lot of records with end date less than the start date
This cannot be correct and hence we will have to correct this in the table*/

-- We will create a new end date based on the start date for the next record

SELECT prd_id, 
	   -- We need Category ID to join it with a table from ERP
	   REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
	   -- Creating the Prd Key separately without the cat id
	   SUBSTRING(TRIM(prd_key), 7, LEN(prd_key)) AS prd_key, 
	   TRIM(prd_nm) AS prd_nm, 
	   -- Replace Null values in cost to zero for easier aggregations
	   ISNULL(prd_cost, 0) AS prd_cost, 
	   -- Standardize prd line column with values that have full name instead of abbreviations
	   CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'N/A'
	   END AS prd_line, 
	   prd_start_dt, 
	   DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM Bronze.crm_prd_info; 


-- We have added new columns. So, we modify the schema for the Silver.crm_prd_info table

IF OBJECT_ID ('Silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE Silver.crm_prd_info; 

CREATE TABLE Silver.crm_prd_info (
	prd_id INT, 
	cat_id NVARCHAR (50), 
	prd_key NVARCHAR (50), 
	prd_nm NVARCHAR (50), 
	prd_cost INT, 
	prd_line NVARCHAR (50), 
	prd_start_dt DATE, 
	prd_end_dt DATE, 
	dwh_create_date DATETIME2 DEFAULT GETDATE()
); 

INSERT INTO Silver.crm_prd_info (
	prd_id, 
	cat_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt
)

SELECT prd_id, 
	   -- We need Category ID to join it with a table from ERP
	   REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, 
	   -- Creating the Prd Key separately without the cat id
	   SUBSTRING(TRIM(prd_key), 7, LEN(prd_key)) AS prd_key, 
	   TRIM(prd_nm) AS prd_nm, 
	   -- Replace Null values in cost to zero for easier aggregations
	   ISNULL(prd_cost, 0) AS prd_cost, 
	   -- Standardize prd line column with values that have full name instead of abbreviations
	   CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'N/A'
	   END AS prd_line, 
	   prd_start_dt, 
	   DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM Bronze.crm_prd_info; 

SELECT * FROM Silver.crm_prd_info; 
