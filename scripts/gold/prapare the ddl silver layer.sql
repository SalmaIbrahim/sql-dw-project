---------------------------------------------------------
--- Customer Dimension

select 
	ROW_NUMBER() over (order by cst_id) as customer_key -- surrogate key
	, cst_id as customer_id
	, cst_key as customer_number
	, cst_firstname as first_name
	, cst_lastname as last_name
	, cl.cntry as country
	, cb.bdate as birthdate
	, DATEDIFF(Year, bdate, GETDATE()) customer_age
	, case 
		when cst_gndr = 'n/a' then isnull(cb.gen, 'n/a')
		else cst_gndr
	end as gender
	, cst_marital_status as marital_status
	, cst_create_date as created_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 cb
	on ci.cst_key = cb.cid
left join silver.erp_loc_a101 cl
	on ci.cst_key = cl.cid


---------------------------------------------------------
-- check for duplication

--select count(1), t.customer from (
--select 
--cst_id customer
--, cst_key customer_number
--, cst_firstname first_name
--, cst_lastname last_name
----, cst_gndr gender
--, cb.gen gender
--, cst_create_date created_date
--, cb.bdate birthdate
--, cl.cntry country
--from silver.crm_cust_info ci
--left join silver.erp_cust_az12 cb
--	on ci.cst_key = cb.cid
--left join silver.erp_loc_a101 cl
--	on ci.cst_key = cl.cid
--) t group by customer having count(1) >  -- no duplications

----------------------------------
---- same column? (gender) 
--select 
--distinct cst_gndr, gen
--, case 
--	when cst_gndr = 'n/a' then isnull(cb.gen, 'n/a')
--	else cst_gndr
--end gender
--from silver.crm_cust_info ci
--left join silver.erp_cust_az12 cb
--	on ci.cst_key = cb.cid
--left join silver.erp_loc_a101 cl
--	on ci.cst_key = cl.cid
----------------------------------
-- rename columns with meaningful names and using the general princioles => naming conventions (snake_case, English language, no reserved words)

---------------------------------------------------------
--- Product Dimension

select 
	ROW_NUMBER() over(order by prd_id) as product_sk
	, prd_id as product_id
	, prd_key as product_number
	, prd_nm as product_name
	, cat_id as category_id
	, pc.cat as category_nam
	, pc.subcat as subcategory_name
	, pc.maintenance 
	, prd_line as product_line
	, prd_cost as product_cost
	, prd_start_dt as start_date
	--, prd_end_dt as end_date
from silver.crm_prd_info pin
left join silver.erp_px_cat_g1v2 pc
	on pin.cat_id = pc.id
where prd_end_dt is null -- to get current or latest info (as SCD type1)

---- NOTE: for point-in-time analysis or auditin --- will use all as SCD type2 (remove the where condition)
---------------------------------------------------------
-- check for duplication

--select prd_key, cat_id, prd_start_dt, count(1) 
--from (
--select 
--	prd_id
--	, prd_key
--	, prd_nm
--	, cat_id
--	, pc.cat
--	, pc.subcat
--	, pc.maintenance
--	, prd_line
--	, prd_cost
--	, prd_start_dt
--	, prd_end_dt
--from silver.crm_prd_info pin
--left join silver.erp_px_cat_g1v2 pc
--	on pin.cat_id = pc.id
--) t
--group by prd_key, cat_id, prd_start_dt
--having count(1) > 1 -- no duplication
-----------------
-- sort columns into logical groups
-- naming convension and meaingful

---------------------------------------------------------
--- Sales Fact

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
---------------------------------------------------------
---------------------------------------------------------
