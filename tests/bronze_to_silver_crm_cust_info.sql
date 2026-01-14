-- Check for Duplicates OR Nulls in the Primary Key
-- Expectations : To not have any Nulls or Duplicates

SELECT * FROM Bronze.crm_cust_info; 

SELECT cst_id, COUNT(*)
FROM Bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 

SELECT * FROM Bronze.crm_cust_info
WHERE cst_id = 29466; 

SELECT * FROM Bronze.crm_cust_info
WHERE cst_id IS NULL; 

-- Keep only rows that were created recently

SELECT *
FROM (
SELECT *, 
	   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS dup_flag
FROM Bronze.crm_cust_info) t
WHERE dup_flag = 1 AND cst_id IS NOT NULL; 


-- We have inconsistency in firstname & last name columns

SELECT cst_firstname
FROM Bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_lastname
FROM Bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Remove leading / trailing spaces from firstname and lastname

SELECT cst_id, 
	   cst_key, 
	   TRIM(cst_firstname) AS cst_firstname, 
	   TRIM(cst_lastname) AS cst_lastname, 
	   cst_material_status AS cst_marital_status, 
	   cst_gndr, 
	   cst_create_date
FROM (
SELECT *, 
	   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS dup_flag
FROM Bronze.crm_cust_info) t
WHERE dup_flag = 1 AND cst_id IS NOT NULL; 


-- Check values in the marital status & gender columns

SELECT DISTINCT cst_material_status
FROM Bronze.crm_cust_info

SELECT DISTINCT cst_gndr
FROM Bronze.crm_cust_info


-- Data standardization & consistency in these columns

SELECT cst_id, 
	   cst_key, 
	   TRIM(cst_firstname) AS cst_firstname, 
	   TRIM(cst_lastname) AS cst_lastname, 
	   CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'N/A'
	   END AS cst_marital_status, 
	   CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
	   END AS cst_gndr, 
	   cst_create_date
FROM (
SELECT *, 
	   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS dup_flag
FROM Bronze.crm_cust_info) t
WHERE dup_flag = 1 AND cst_id IS NOT NULL; 


-- We have now cleaned & transformed the Cust Info table
-- Let's insert the values in the Silver layer table

INSERT INTO Silver.crm_cust_info (
	cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_marital_status, 
	cst_gndr, 
	cst_create_date
)

SELECT cst_id, 
	   cst_key, 
	   TRIM(cst_firstname) AS cst_firstname, 
	   TRIM(cst_lastname) AS cst_lastname, 
	   CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			ELSE 'N/A'
	   END AS cst_marital_status, 
	   CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'N/A'
	   END AS cst_gndr, 
	   cst_create_date
FROM (
SELECT *, 
	   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS dup_flag
FROM Bronze.crm_cust_info) t
WHERE dup_flag = 1 AND cst_id IS NOT NULL; 


-- Check Silver Cust Info table

SELECT * FROM Silver.crm_cust_info; 


-- Quality check the data in Silver Cust Info table

SELECT cst_id, COUNT(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL; 

SELECT cst_firstname
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); 

SELECT cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); 

SELECT DISTINCT cst_marital_status
FROM Silver.crm_cust_info; 

SELECT DISTINCT cst_gndr
FROM Silver.crm_cust_info; 
