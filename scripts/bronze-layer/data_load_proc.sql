-- Bulk insert data into the tables

/* We create a Stored Procedure so that we don't have to write the query 
   everytime we want to load the data into the tables*/ 

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS

BEGIN
	-- Declare variables to check the run time for the whole layer
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME; 

	SET @batch_start_time = GETDATE()

	-- Declate variables to check the run time for loading data
	DECLARE @start_time DATETIME, @end_time DATETIME; 

	-- Write a TRY / CATCH Block to log errors, if any
	BEGIN TRY
		PRINT ('=========================================================='); 
		PRINT ('Loading Data in the Bronze Layer'); 
		PRINT ('=========================================================='); 

		PRINT ('----------------------------------------------------------'); 
		PRINT ('Loading CRM Tables'); 
		PRINT ('----------------------------------------------------------'); 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.crm_cust_info'); 
		TRUNCATE TABLE Bronze.crm_cust_info; 

		PRINT ('>>Inserting Data Into: Bronze.crm_cust_info'); 
		BULK INSERT Bronze.crm_cust_info
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.crm_prd_info'); 
		TRUNCATE TABLE Bronze.crm_prd_info; 

		PRINT ('>>Inserting Data Into: Bronze.crm_prd_info'); 
		BULK INSERT Bronze.crm_prd_info
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.crm_sales_details'); 
		TRUNCATE TABLE Bronze.crm_sales_details; 

		PRINT ('>>Inserting Data Into: Bronze.crm_sales_details'); 
		BULK INSERT Bronze.crm_sales_details
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

		PRINT ('----------------------------------------------------------'); 
		PRINT ('Loading ERP Tables'); 
		PRINT ('----------------------------------------------------------'); 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.erp_cust_az12'); 
		TRUNCATE TABLE Bronze.erp_cust_az12; 

		PRINT ('>>Inserting Data Into: Bronze.erp_cust_az12'); 
		BULK INSERT Bronze.erp_cust_az12
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.erp_loc_a101'); 
		TRUNCATE TABLE Bronze.erp_loc_a101; 

		PRINT ('>>Inserting Data Into: Bronze.erp_loc_a101'); 
		BULK INSERT Bronze.erp_loc_a101
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

		SET @start_time = GETDATE(); 
		PRINT ('>>Truncating Table: Bronze.erp_px_cat_g1v2'); 
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2; 

		PRINT ('>>Inserting Data Into: Bronze.erp_px_cat_g1v2'); 
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'D:\Data Engineering\01. SQL\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		); 
		SET @end_time = GETDATE(); 
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'; 
		PRINT '------------------------------'; 

	END TRY

	BEGIN CATCH
		PRINT ('===================================================='); 
		PRINT 'ERROR OCCURED DURING LOADING THE BRONZE LAYER'; 
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
