
/*
	=======================================================
	Stored ProcedureL Load Bronze Layer (Bronze -> Silver)
	=======================================================
	Script Purpose:
		This Proc performs the Extract process from different sources and Load data to the bronze layer
		to populate the bronze schema.

	Actions Performed:
		- Truncates bronze tables.
		- Inserts extracted data from the Sources into the Bronze tables.

	Parameters:
		None.

	Usage Example:
		EXEC bronze.load_bronze;
	=======================================================

*/

	
	
	-- want to load data from csv file to the DW
	-- using Full Loads approach

	------------- Notes -------------
	-- how to handle the file in BULK INSERT [] WITH () statement 
	-- 1st row is the header -->  (FIRSTROW = 2) > Skip 1st row
	-- specify the Delimiter -->  (FIELDTERMINATOR = ',') >> the seperator is a comma
	-- TABLOCK --> to Improve performance, Lock the entire table (no trans)

	----------------------------------------------------------
CREATE OR ALTER PROC bronze.load_bronze
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
			, @start_batch_time DATETIME, @end_batch_time DATETIME

	BEGIN TRY
		set @start_batch_time = GETDATE();
		----------------
		Print '========== Loading Bronze Layer =========='

		------------- Customer Info -------------
		Print '------------- Customer Info -------------'
		
		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.crm_cust_info;

		-- insert cust_info data
		print 'Executing INSERT'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		);
		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='

		------------- Product Info -------------
		Print '------------- Product Info -------------'
		
		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.crm_prd_info;
		-- insert prd_info data
		print 'Executing INSERT'
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		);

		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='

		------------- Sales Details -------------
		print '------------- Sales Details -------------'

		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.crm_sales_details;
		-- insert sales_details data
		print 'Executing INSERT'
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		);

		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='

		------------- Customer AZ12 -------------
		print '------------- Customer AZ12 -------------'
		
		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.erp_cust_az12;
		-- insert cust_az12 data
		print 'Executing INSERT'
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		)

		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='


		------------- Loc A101 -------------
		print '------------- Loc A101 -------------'
		
		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.erp_loc_a101;
		-- insert loc_a101 data
		print 'Executing INSERT'
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		)
		
		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='

		------------- PX CAT G1V2 -------------
		print '------------- PX CAT G1V2 -------------'
		
		set @start_time = GETDATE()
		-- in case the table is exist to AVOID data duplication
		print 'Executing TRUNCATE'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		-- insert px_cat_g1v2 data
		print 'Executing INSERT'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Projects\DataWarehouse\sql-dw-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2
			, FIELDTERMINATOR = ','
			, ROWTERMINATOR = '\n'
			, TABLOCK 
		)

		set @end_time = GETDATE()

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '==========================================='

		print '========== END Loading Bronze Layer =========='
		
		PRINT '==========================================='
		
	END TRY
	
	BEGIN CATCH	
		print '================================================'
		print 'ERROT OCCURED DURING LOADING BRONZE LAYER'
		print 'ERROR MSG >> ' + ERROR_MESSAGE();
		print 'ERROR NO. >> ' + CAST ( ERROR_NUMBER() AS NVARCHAR);
		print 'ERROR STATUS >> ' + CAST ( ERROR_STATE() AS NVARCHAR);
		print '================================================'

	END CATCH

	set @end_batch_time = GETDATE();

	PRINT 'The Whole Batch Completion Time >> ' + CAST (DATEDIFF(SECOND, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' Seconds.'
END;