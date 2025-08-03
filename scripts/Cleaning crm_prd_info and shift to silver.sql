--bronze.crm_prd_info (Data Cleaning and inserting to Silver)

 SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

--check for primary key
SELECT prd_id,count(*) FROM bronze.crm_prd_info group by prd_id having count(*)>1 or prd_id IS NULL;

--prd_key split it into 2 info
 SELECT 
	prd_id,
	prd_key,
	SUBSTRING(prd_key,1,5) AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

SELECT distinct id from bronze.erp_px_cat_g1v2;
--There is underscore instead of '-'
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

SELECT sls_prd_key from bronze.crm_sales_details;

--Replace NULL
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

--Cardinality will be low if there is short form like f for female
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info;

--Check for Invalid Date Orders
SELECT * FROM bronze.crm_prd_info WHERE prd_end_dt<prd_start_dt
--#Solution 1:Switch start and end date-There may br overlapping. Each record must have start date
--#Solution 2:Derive end Date from Start date. The end date shkd be start date of next record-1
--There is no data of time so change it off from datetime to dateusing CAST
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info;

--Update DDL scripts of silver
--Since we created 1 extra columns go to created silver table query and add these col_name inti it and change stdate and enddt to DATE datatype

INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt )
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'other Sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
from bronze.crm_prd_info;

select distinct prd_line from silver.crm_prd_info;
select * from silver.crm_prd_info;