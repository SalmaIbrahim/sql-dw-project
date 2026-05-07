/*
	=======================================================
	DDL Script: Create Bronze Layer Tables
	=======================================================
	Script Purpose:
		This script creates tables for the Bronze layer.
		The Bronze layer stores raw data exactly as it comes from source systems.

		Using the naming convension (snake_case):
			- table name -->   <soyurce system>_<entity_name>
			- column name --> the name in the source system

	Actions Performed:
		- Drops bronze tables.
		- Creates the tables defination for Bronze layer tables.
	=======================================================
*/
-- creating the bronze objects
-- table > naming convension <soyurce system>_<entity_name>
-- column name = columns in the source system

-- CRM - CSV files
-- datasets\source_crm\cust_info.csv
CREATE TABLE bronze.crm_cust_info
(
cst_id INT 
, cst_key NVARCHAR(50)
, cst_firstname NVARCHAR(50)
, cst_lastname NVARCHAR(50)
, cst_marital_status NVARCHAR(50)
, cst_gndr NVARCHAR(50)
, cst_create_date DATETIME
);

-- datasets\source_crm\prd_info.csv
CREATE TABLE bronze.crm_prd_info 
(
prd_id iNT
, prd_key NVARCHAR(50)
, prd_nm NVARCHAR(50)
, prd_cost INT
, prd_line NVARCHAR(50)
, prd_start_dt DATETIME
, prd_end_dt DATETIME
);

-- datasets\source_crm\sales_details.csv
CREATE TABLE bronze.crm_sales_details 
(
sls_ord_num NVARCHAR(50)
,sls_prd_key NVARCHAR(50)
,sls_cust_id INT
,sls_order_dt INT
,sls_ship_dt INT
,sls_due_dt INT
,sls_sales INT
,sls_quantity INT
,sls_price INT
);


-- ERP - CSV files
-- datasets\source_erp\CUST_AZ12.csv
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12 (
cid NVARCHAR(50)
, bdate DATE
, gen NVARCHAR(50)
);

-- datasets\source_erp\LOC_A101.csv
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101 (
cid NVARCHAR(50)
, cntry NVARCHAR(50) 
);


-- datasets\source_erp\PX_CAT_G1V2.csv
CREATE TABLE bronze.erp_px_cat_g1v2 (
id  NVARCHAR(50)
, cat NVARCHAR(50)
, subcat NVARCHAR(50)
, maintenance NVARCHAR(50)
);

-------------------------------------------------------

/* 
	to make sure that objects (tables) not exist you can use

	IF OBJECT_ID ('schema.object_name', 'U') IS NOT NULL -- U is for user defined table
		DROP TABLE schema.object_name
*/

-- give a try :)
--IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
--	print OBJECT_ID ('bronze.crm_cust_info', 'U');
--print 'GREAT!';