EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================================================';

		PRINT '-----------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT ' >> Inserting data Into:bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		--Select count(*) FROM bronze.crm_cust_info;

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT ' >> Inserting data Into:bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT ' >> Inserting data Into:bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		PRINT '-----------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT ' >> Inserting data Into:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT ' >> Inserting data Into:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		SET @start_time = GETDATE();
		PRINT ' >> Truncating Table:bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT ' >> Inserting data Into:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\harini\Desktop\Lets Learn\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>---------------'

		SET @batch_end_time = GETDATE();
		PRINT '============================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '>> - Total Load Duration: '+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '============================================';
	END TRY
	BEGIN CATCH
		PRINT '=============================================';
		PRINT 'EEROR OCCURED DURING LOADING THE BRONZE LAYER';
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message'+ CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================';


	END CATCH
END


--1.Bulk inserting the data into tables.
--2.Put the truncate command.
--3.Add print statements.
--4.Add a try catch block using begin and end.
--5.Create a procedure so you need not run this whole thing always.
--6.To check the performance add the datetime by setting the datetime.
--7.Now check the whole batch time of bronze level.