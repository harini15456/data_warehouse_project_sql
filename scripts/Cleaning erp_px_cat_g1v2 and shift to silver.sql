SELECT 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

--check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat!=TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance!=TRIM(maintenance);

--Data Standardization and consistency
select distinct
cat
from bronze.erp_px_cat_g1v2;


INSERT INTO silver.erp_px_cat_g1v2(
id,cat,subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2