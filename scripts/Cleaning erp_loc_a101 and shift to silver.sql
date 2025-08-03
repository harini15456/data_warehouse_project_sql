SELECT 
cid,
cntry
FROM bronze.erp_loc_a101;

select cst_key from silver.crm_cust_info;

SELECT 
REPLACE(cid,'-','') AS cid,
cntry
FROM bronze.erp_loc_a101;

select distinct cntry FROM bronze.erp_loc_a101 ORDER BY cntry;

SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101 
ORDER BY cntry;


INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101 
ORDER BY cntry;
