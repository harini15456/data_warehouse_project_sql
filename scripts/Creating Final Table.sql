SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid

--TIP: AFTER JOINING TABLE, CHECK IF ANY DUPLICATES WERE INTRODUCED BY THE JOIN LOGIC
SELECT cst_id,COUNT(*) FROM
(SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid)t GROUP BY cst_id HAVING COUNT(*)>1

--There are 2 cols of gender so perform Data Integration
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_create_date,
	ca.bdate,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master table for gender info
	ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid

--Rename columns to friendly and meaningful names
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master table for gender info
	ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birthdate,
	la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid

--Order the columns
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master table for gender info
	ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid


--decide whether it is fact or dim table
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master table for gender info
	ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid

--Create Object
--We use Views
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master table for gender info
	ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key=la.cid

SELECT * FROM gold.dim_customers


--Create Dimension Products
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
FROM silver.crm_prd_info pn;

--Filter out all historical data
--If end date is NULL it is the current Info
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
FROM silver.crm_prd_info pn
WHERE pn.prd_end_dt IS NULL;

--Check duplicates after join
select prd_key, count(*) FROM (
select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL
)t GROUP BY prd_key HAVING COUNT(*)>1;

--Sort
select 
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL

--Rename
select 
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL

--It is dimension Table
--Prepare a primary key for it-PK i.e Surrogate key
select 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL

--create view
CREATE VIEW gold.dim_products AS
select 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL

select * from gold.dim_products;


--CREATE FACT TABLE
--BUILDING FACT: Use the dimensions's surrogate keys instead of IDs to easily connect facts with dimensions
SELECT 
sd.sls_ord_num,
sd.sls_prd_key,
sd.sls_cust_id,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd;

SELECT 
sd.sls_ord_num,
pr.product_key,
cu.customer_key,
sd.sls_order_dt,
sd.sls_ship_dt,
sd.sls_due_dt,
sd.sls_sales,
sd.sls_quantity,
sd.sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id

--Rename
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id

--Create View
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id

select * from gold.fact_sales;

--Foreign key Integrity(Dimensions)
--Check if all dim tables can successfully join to the fact table
select * from gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key=f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
WHERE p.product_key IS NULL