--SILVER CRM_CUST_INFO

--INSQLquery8 we have found that there are duplicates, so check the complete data, then we found that the one with 
--latest date has the complete info rather than Null values.

SELECT *
FROM(
SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info)t
where flag_last=1;

--Check for unwanted spaces with data having string values
select cst_firstname from silver.crm_cust_info where cst_firstname!=TRIM(cst_firstname);

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
)t where flag_last=1;

--Data standardization and consistency
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;

INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
	
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	 WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
	 ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last=1;

SELECT * FROM silver.crm_cust_info;