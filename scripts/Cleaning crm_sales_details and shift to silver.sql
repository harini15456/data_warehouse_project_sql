select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;

--Orderdate must always be earlier than shipping or due date
select * FROM bronze.crm_sales_details where sls_order_dt> sls_ship_dt or sls_order_dt>sls_due_dt;

--For last 3 rows check the business rule: THE SALES EQUAL TO QUANTITY * PRICE and -ve ,zero ,nulls are not allowed
select DISTINCT
sls_sales,sls_quantity,sls_price 
from bronze.crm_sales_details 
where sls_sales!=sls_quantity*sls_price 
or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
or sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quantity,sls_price;

--Solution-ss
select DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price) 
		THEN sls_quantity*ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price<=0 
		THEN sls_sales/NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price
from bronze.crm_sales_details 
where sls_sales!=sls_quantity*sls_price 
or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
or sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
ORDER BY sls_sales,sls_quantity,sls_price;


INSERT INTO silver.crm_sales_details(
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
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price) 
			THEN sls_quantity*ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price<=0 
			THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;

select * from silver.crm_sales_details;