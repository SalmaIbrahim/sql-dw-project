/*
	=======================================================
	DDL Script: Create Gold Layer Views
	=======================================================
	Script Purpose:
		This script creates views for the Gold layer in the datewarhouse.
		The Gold layer represents the final dimension and fact tables (Star Scema)

		Each view performs tranformations and combines data from the Silver layer 
		to produce a clean, enriched, and business-ready dataset
		
		Using the naming convension (snake_case):
			- table name ->	dim_<entity_name> for dimensions
							fact_<entint_name> for facts
			- column name -> meaningful names

	Actions Performed:
		- Represents the data of dimensions and fact tables

	Usage Example:
		- Execute: 
			select * from gold.dim_<name>
			select * from gold.fact_<name>
		- Usage:
			use directly for the analytics and reporting

	=======================================================
*/


---------------- Customer Dimension ---------------- 
CREATE OR ALTER VIEW gold.dim_customers
AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_sk -- surrogate key
	, cst_id AS customer_id
	, cst_key AS customer_number
	, cst_firstname AS first_name
	, cst_lastname AS last_name
	, cl.cntry AS country
	, cb.bdate AS birthdate
	, DATEDIFF(Year, bdate, GETDATE()) age
	, CASE 
		WHEN cst_gndr = 'n/a' THEN isnull(cb.gen, 'n/a')
		ELSE cst_gndr
	END AS gender
	, cst_marital_status AS marital_status
	, cst_create_date AS created_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cb
	ON ci.cst_key = cb.cid
LEFT JOIN silver.erp_loc_a101 cl
	ON ci.cst_key = cl.cid

GO
----------------------------------------------------
---------------- Product Dimension -----------------

CREATE OR ALTER VIEW gold.dim_products
AS
SELECT 
	ROW_NUMBER() over(ORDER BY prd_id) AS product_sk
	, prd_id AS product_id
	, prd_key AS product_number
	, prd_nm AS product_name
	, cat_id AS category_id
	, pc.cat AS category_name
	, pc.subcat AS subcategory_name
	, pc.maintenance 
	, prd_line AS product_line
	, prd_cost AS product_cost
	, prd_start_dt AS start_date
	, prd_end_dt AS end_date
	, is_current
FROM silver.crm_prd_info pin
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pin.cat_id = pc.id
--WHERE prd_end_dt IS NULL -- to get current or latest info (as SCD type1)

GO
----------------------------------------------------
-------------------- Fact Sales --------------------
CREATE OR ALTER VIEW gold.fact_sales
AS
SELECT
	sls_ord_num AS order_number 
	, pr.product_sk
	, cus.customer_sk
	, sls_order_dt AS order_date
	, sls_ship_dt AS ship_date
	, sls_due_dt AS due_date
	, sls_sales AS sales_amount
	, sls_quantity AS quantity
	, sls_price AS product_price

FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers cus
	ON sd.sls_cust_id = cus.customer_id
LEFT JOIN gold.dim_products pr 
	ON sd.sls_prd_key = pr.product_number

GO
----------------------------------------------------

