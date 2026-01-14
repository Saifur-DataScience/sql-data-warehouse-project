-- Data consistency & standardization

USE DataWarehouse; 

SELECT * FROM Bronze.erp_loc_a101; 

SELECT DISTINCT cid
FROM Bronze.erp_loc_a101
WHERE TRIM(cid) NOT IN (SELECT cst_key FROM Silver.crm_cust_info); 


-- We have a '-' in the cid column in ERP Location table

SELECT cid, 
	   REPLACE(cid, '-', '') AS cid
FROM Bronze.erp_loc_a101; 


-- Checking the cntry column

SELECT DISTINCT cntry
FROM Bronze.erp_loc_a101; 

-- Let's standardize the Country column

SELECT DISTINCT cntry, 
	   CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) = 'USA' OR UPPER(TRIM(cntry)) = 'US' THEN 'United States'
			WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
			ELSE cntry
	   END AS cntry
FROM Bronze.erp_loc_a101; 


-- Let's integrate the changes together

SELECT
	REPLACE(cid, '-', '') AS cid, 
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) = 'USA' OR UPPER(TRIM(cntry)) = 'US' THEN 'United States'
			WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
			ELSE cntry
	   END AS cntry
FROM Bronze.erp_loc_a101; 


-- Insert data in the Silver layer Location table

TRUNCATE TABLE Silver.erp_loc_a101; 

INSERT INTO Silver.erp_loc_a101(
	cid, 
	cntry
)
SELECT
	REPLACE(cid, '-', '') AS cid, 
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
		 WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
		 ELSE TRIM(cntry)
	END AS cntry
FROM Bronze.erp_loc_a101; 

SELECT * FROM Silver.erp_loc_a101; 

SELECT DISTINCT cntry
FROM Silver.erp_loc_a101; 

SELECT cid
FROM Silver.erp_loc_a101
WHERE cid LIKE '%-%'; 

SELECT cid
FROM Silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM Silver.crm_cust_info); 
