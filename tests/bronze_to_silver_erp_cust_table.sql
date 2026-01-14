-- Clean & transform ERP tables now & 
-- insert data in the ERP tables

SELECT * FROM Bronze.erp_cust_az12; 

SELECT DISTINCT cid
FROM Bronze.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM Silver.crm_cust_info); 


-- We have additional string 'NAS' in the CID column in ERP table

SELECT
	cid, 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid
FROM Bronze.erp_cust_az12; 


-- Let's check quality for bdate column

SELECT bdate
FROM Bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE() OR bdate IS NULL; 


-- We have customers older than 100 years of age
-- Also, customers with bdate in the future

-- As per business logic, customers can be older, but cannot have future Birth Date

SELECT bdate, 
	   CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
	   END as bdate
FROM Bronze.erp_cust_az12; 


-- Lastly, let's check the Gender column

SELECT DISTINCT gen
FROM Bronze.erp_cust_az12; 


-- Let's correct this as well

SELECT DISTINCT gen, 
	   CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'N/A'
	   END as gen
FROM Bronze.erp_cust_az12; 


-- Integrating all the transformations together

SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid, 
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END as bdate, 
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'N/A'
	END as gen
FROM Bronze.erp_cust_az12; 


-- Insert the data in the Silver layer table

INSERT INTO Silver.erp_cust_az12 (
	cid, 
	bdate, 
	gen
)

SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid, 
	CASE WHEN bdate > GETDATE() THEN NULL
		 ELSE bdate
	END as bdate, 
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'N/A'
	END as gen
FROM Bronze.erp_cust_az12; 

SELECT * FROM Silver.erp_cust_az12; 
