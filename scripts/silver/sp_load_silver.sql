/*
	=======================================================
	Stored ProcedureL Load Silver Layer (Bronze -> Silver)
	=======================================================
	Script Purpose:
		This Proc performs the ETL (Extract -> from the bronze layer, Transform -> cleansing, Load -> to the silver layer) process 
		to populate the silver schema from the bronze schema

	Actions Performed:
		- Truncates silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

	Parameters:
		None.

	Usage Example:
		EXEC silver.load_silver;
	=======================================================

*/

CREATE OR ALTER PROC silver.load_silver
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
			, @start_batch_time DATETIME, @end_batch_time DATETIME

	BEGIN TRY
		set @start_batch_time = GETDATE();
		----------------
		Print '========== Loading Silver Layer ==========';

		------------- Customer Info -------------
		Print '------------- Customer Info -------------';
		
		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE.';
		TRUNCATE TABLE silver.crm_cust_info;

		-- insert cust_info data
		print 'Executing INSERT.';
		INSERT INTO silver.crm_cust_info
		(
		cst_id
		, cst_key
		, cst_firstname
		, cst_lAStname
		, cst_gndr
		, cst_marital_status
		, cst_create_date
		
		)
		SELECT cst_t.cst_id
		, cst_t.cst_key
		, TRIM(cst_t.cst_firstname) AS cst_firstname
		, TRIM(cst_t.cst_lAStname) AS cst_lAStname
		, CASE UPPER(TRIM(cst_t.cst_gndr)) -- in CASE, letters are not capitalized and have unwanted spaces
			WHEN 'M' then 'Male'
			WHEN 'F' then 'Female'
			else 'n/a' -- not availabe
		END cst_gndr
		, CASE UPPER(TRIM(cst_t.cst_marital_status))
			WHEN 'S' then 'Single'
			WHEN 'M' then 'Married'
			else 'n/a'
		END cst_marital_status
		, cst_t.cst_create_date
		
		FROM (
		SELECT *
		, ROW_NUMBER() OVER 
			(PARTITION BY cst_id 
			ORDER BY cst_create_date DESC) flag_lASt
		FROM bronze.crm_cust_info 
		) cst_t
		WHERE flag_lASt = 1 -- No duplication
		AND cst_id IS NOT NULL; -- has null values

		set @end_time = GETDATE();
		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '===========================================';

		------------- Product Info -------------
		Print '------------- Product Info -------------';
		
		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE.';
		TRUNCATE TABLE silver.crm_prd_info;
		-- insert prd_info data
		print 'Executing INSERT.';
		INSERT INTO silver.crm_prd_info
		(
			prd_id
			, cat_id
			, prd_key
			, prd_nm
			, prd_cost
			, prd_line
			, prd_start_dt
			, prd_end_dt
			, is_current
			
		)
		SELECT 
			prd_id
			, REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_') AS cat_id
			, SUBSTRING(TRIM(prd_key), 7, LEN(prd_key)) AS prd_key
			, prd_nm
			, ISNULL(prd_cost, 0) prd_cost
			, CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line
			, CAST( prd_start_dt AS DATE) AS prd_start_dt
			, CAST( DATEADD(DAY, -1, lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) prd_end_dt
			, CASE WHEN prd_end_dt IS NULL THEN 1 ELSE 0 END is_current
			
		FROM bronze.crm_prd_info;

		set @end_time = GETDATE();
		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '===========================================';

		------------- Sales Details -------------
		print '------------- Sales Details -------------';

		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE.';
		TRUNCATE TABLE silver.crm_sales_details;
		-- insert sales_details data
		print 'Executing INSERT.';

		with cte as (
		select 
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_order_dt AS NVARCHAR) AS DATE)
		END AS sls_order_dt
		, CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_ship_dt AS NVARCHAR) AS DATE)
		END AS sls_ship_dt
		, CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST (CAST(sls_due_dt AS NVARCHAR) AS DATE)
		END AS sls_due_dt
		, CASE
			WHEN sls_sales IS NULL 
				OR sls_sales <= 0 
				OR sls_sales != (sls_quantity * ABS(sls_price))
			THEN (sls_quantity * ABS(sls_price))
			ELSE sls_sales
		END sls_sales
		, sls_quantity
		, CASE
			WHEN sls_price IS NULL
				OR sls_price <= 0 
			THEN (sls_sales / NULLIF(sls_quantity, 0))
			ELSE sls_price
		END sls_price
		from bronze.crm_sales_details
		)

		INSERT INTO silver.crm_sales_details
		(
			sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, sls_order_dt
		, sls_ship_dt
		, sls_due_dt
		, sls_sales
		, sls_quantity
		, sls_price
		
		)
		select
		sls_ord_num
		, sls_prd_key
		, sls_cust_id
		, ISNULL(sls_order_dt, DATEADD(DAY, -7, sls_ship_dt)) sls_order_dt
		, sls_ship_dt
		, sls_due_dt
		, sls_sales
		, sls_quantity
		, sls_price
		
		from cte ;

		set @end_time = GETDATE();
		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '===========================================';

		------------- Customer AZ12 -------------
		print '------------- Customer AZ12 -------------';
		
		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE';
		TRUNCATE TABLE silver.erp_cust_az12;
		-- insert cust_az12 data
		print 'Executing INSERT';	
		insert into silver.erp_cust_az12
		(
		cid
		, bdate
		, gen
		
		)
		select 
		 CASE 
			WHEN cid like 'NAS%' 
			THEN SUBSTRING(cid, 4, len(cid)) 
			ELSE cid 
		END cid
		 , CASE
			WHEN bdate > GETDATE() 
			THEN NULL
			ELSE bdate
		 END bdate 
		 , CASE
			WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		 END gen
		 
		from bronze.erp_cust_az12;
		
		set @end_time = GETDATE();
		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '===========================================';

		---------- Customer Location ----------
		print '------------- Loc A101 -------------'
		
		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE';
		TRUNCATE TABLE silver.erp_loc_a101;
		-- insert loc_a101 data
		print 'Executing INSERT';
		insert into silver.erp_loc_a101
		(
		cid
		, cntry
		
		)
		select 
		REPLACE(TRIM(cid), '-', '') cid
		, CASE
			WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
			WHEN TRIM(cntry) in ('DE', 'Germany') THEN 'Germany'
			WHEN TRIM(cntry) in ('United States', 'US', 'USA') THEN 'United States'
			ELSE TRIM(cntry)
		END cntry
		
		from bronze.erp_loc_a101;
		
		set @end_time = GETDATE();

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		PRINT '===========================================';
		------------------------------------------

		---------- Product Categories ----------
		print '------------- PX CAT G1V2 -------------';
		
		set @start_time = GETDATE();
		-- in case the table exists to AVOID data duplication
		print 'Executing TRUNCATE';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		-- insert px_cat_g1v2 data
		print 'Executing INSERT';
		insert into silver.erp_px_cat_g1v2
		(
		id
		, cat
		, subcat
		, maintenance
		
		)
		select 
		id
		, cat
		, subcat
		, maintenance
		
		from bronze.erp_px_cat_g1v2;

		set @end_time = GETDATE();

		PRINT 'Loading Duration >> ' + CAST ( DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds.';

		------------------------------------------
		
		PRINT '===========================================';

		print '========== END Loading Silver Layer ==========';
		
		PRINT '===========================================';
		
		END TRY
	
	BEGIN CATCH
		print '================================================';
		print 'ERROR OCCURRED DURING LOADING SILVER LAYER';
		print 'ERROR MSG >> ' + ERROR_MESSAGE();
		print 'ERROR NO. >> ' + CAST ( ERROR_NUMBER() AS NVARCHAR);
		print 'ERROR STATUS >> ' + CAST ( ERROR_STATE() AS NVARCHAR);
		print '================================================';

	END CATCH

	set @end_batch_time = GETDATE();

	PRINT 'The Whole Batch Completion Time >> ' + CAST (DATEDIFF(SECOND, @start_batch_time, @end_batch_time) AS NVARCHAR) + ' Seconds.';
END;
