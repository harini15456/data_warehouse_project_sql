select
cid,
bdate,
gen
from bronze.erp_cust_az12;

select * from [silver].[crm_cust_info];

select 
CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END AS cid,
bdate,
gen
from bronze.erp_cust_az12;

select 
CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END ASbdate,
gen
from bronze.erp_cust_az12;


INSERT INTO silver.erp_cust_az12(
cid,bdate,gen)

select 
CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	 ELSE bdate
END ASbdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12;