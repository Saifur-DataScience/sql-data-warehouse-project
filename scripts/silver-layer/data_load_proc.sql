/*Creating stored procedure to transform & load 
data from Bronze to Silver layer*/

CREATE OR ALTER PROCEDURE Silver.load_silver AS

BEGIN
	-- Declare variables to check the run time for the whole layer
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME; 

	SET @batch_start_time = GETDATE()

	-- Declare variables to check the run time for loading data
	DECLARE @start_time DATETIME, @end_time DATETIME; 

	-- Write a TRY / CATCH Block to log errors, if any
	BEGIN TRY
		PRINT ('=========================================================='); 
		PRINT ('Loading Data in the Silver Layer'); 
		PRINT ('=========================================================='); 

		PRINT ('----------------------------------------------------------'); 
		PRINT ('Loading CRM Tables'); 
		PRINT ('----------------------------------------------------------'); 

		-- Loading Silver.crm_cust_info table
		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table : Silver.crm_cust_info'
		TRUNCATE TABLE Silver.crm_cust_info; 

		PRINT '>> Inserting Data Into: Silver.crm_cust_info'
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
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 


		-- Loading Silver.crm_prd_info table
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : Silver.crm_prd_info'
		TRUNCATE TABLE Silver.crm_prd_info; 

		PRINT '>> Inserting Data Into: Silver.crm_prd_info'
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
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 


		-- Loading Silver.crm_sales_details table
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : Silver.crm_sales_details'
		TRUNCATE TABLE Silver.crm_sales_details; 

		PRINT '>> Inserting Data Into: Silver.crm_sales_details'
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
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 


		PRINT ('----------------------------------------------------------'); 
		PRINT ('Loading ERP Tables'); 
		PRINT ('----------------------------------------------------------'); 

		-- Loading Silver.erp_cust_az12 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : Silver.erp_cust_az12'
		TRUNCATE TABLE Silver.erp_cust_az12; 

		PRINT '>> Inserting Data Into: Silver.erp_cust_az12'
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
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 


		-- Loading Silver.erp_loc_a101 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : Silver.erp_loc_a101'
		TRUNCATE TABLE Silver.erp_loc_a101; 

		PRINT '>> Inserting Data Into: Silver.erp_loc_a101'
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
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 


		-- Loading Silver.erp_px_cat_g1v2 table
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : Silver.erp_px_cat_g1v2'
		TRUNCATE TABLE Silver.erp_px_cat_g1v2; 

		PRINT '>> Inserting Data Into: Silver.erp_px_cat_g1v2'
		INSERT INTO Silver.erp_px_cat_g1v2(
			id, 
			cat, 
			subcat, 
			maintenance
		)
		SELECT * FROM Bronze.erp_px_cat_g1v2; 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 
	
	END TRY

	BEGIN CATCH
		PRINT ('===================================================='); 
		PRINT 'ERROR OCCURED DURING LOADING THE SILVER LAYER'; 
		PRINT 'Error Message' + Error_Message (); 
		PRINT 'Error Message' + CAST(Error_Number() AS NVARCHAR); 
		PRINT 'Error Message' + CAST(Error_State() AS NVARCHAR); 
		PRINT ('===================================================='); 
	END CATCH

	SET @batch_end_time = GETDATE()
	PRINT '=============================================='; 
	PRINT '>> Runtime for Entire Batch: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'; 
	PRINT '=============================================='; 
END
