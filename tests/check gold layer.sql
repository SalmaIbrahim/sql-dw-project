--select *from silver.crm_cust_info
--select * from bronze.crm_cust_info
--select *from silver.crm_prd_info where prd_id is null
--select *from silver.crm_sales_details
--select *from silver.erp_cust_az12
--select *from silver.erp_loc_a101
--select *from silver.erp_px_cat_g1v2

----exec silver.load_silver

--select * from gold.dim_customers
--select * from gold.dim_products
--select * from gold.fact_sales

-- check for customer surrogate key and product sk
select * 
from gold.fact_sales f
--left join gold.dim_customers c
--	on f.customer_sk = c.customer_sk
left join gold.dim_products p
	on f.product_sk = p.product_sk
--where c.customer_sk is null
where p.product_sk is null
