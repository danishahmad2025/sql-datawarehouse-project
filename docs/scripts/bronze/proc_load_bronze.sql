/*********************************************************************************************
stored procedure:load bronze layer(source -> bronze)
 Purpose:
   - Loads raw CSV files into the 'bronze' schema.
   - Truncates old data and bulk inserts fresh data from CRM and ERP sources.
   - Tracks load time and handles errors.
stores procedure doesnot accept parameters or return any value.

*while inserting (bronze.erp_cust_az12) i have use (ROWTERMINATOR = '0x0d0a',) instead of (ROWTERMINATOR = '/n',)
because the table contain some hidden newline characters 
 
How to Run:
   1. Run this script once to create/update the procedure.
   2. Then execute it with:  EXEC bronze.load_bronze;
  eg:   GO
        EXEC bronze.load_bronze;

*********************************************************************************************/

CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME ;
    BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT'==============================================================';
    PRINT'loading bronze layer';
    PRINT'==============================================================';

    PRINT'-----------------------------------------------------------';
    PRINT'loading crm table';
    PRINT'-----------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT'>>truncating the table:bronze.crm_cust_info>> ';
    TRUNCATE TABLE bronze.crm_cust_info;
    PRINT'>>inserting the data into table:bronze.crm_cust_info>> ';
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/source_crm/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';


    SET @start_time = GETDATE();
    PRINT'>>truncating the table:bronze.crm_prd_info>> ';
    TRUNCATE TABLE bronze.crm_prd_info;
    PRINT'>>inserting the data into the table:bronze.crm_prd_info>> ';
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/source_crm/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';





    SET @start_time = GETDATE();
    PRINT'>>truncating the table:bronze.crm_sales_details>> ';
    TRUNCATE TABLE bronze.crm_sales_details;
    PRINT'>>inserting the data into the table:bronze.crm_sales_details>> ';
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/source_crm/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';




    PRINT'-----------------------------------------------------------';
    PRINT'loading erp table';
    PRINT'-----------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT'>>truncating the table:bronze.erp_loc_a101>> ';
    TRUNCATE TABLE bronze.erp_loc_a101;
    PRINT'>>inserting the data into the table:bronze.erp_loc_a101>> ';
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/source_erp/loc_a101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';



    SET @start_time = GETDATE();
    PRINT'>>truncating the table: bronze.erp_cust_az12>> ';
    TRUNCATE TABLE bronze.erp_cust_az12;
    PRINT'>>inserting the data into the table: bronze.erp_cust_az12>> ';
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/source_erp/cust_az12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0d0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';


 
    SET @start_time = GETDATE();
    PRINT'>>truncating the table:  bronze.erp_px_cat_g1v2>> ';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    PRINT'>>inserting the data into the table:bronze.erp_px_cat_g1v2>> ';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/source_erp/px_cat_g1v2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n',
        TABLOCK
    );
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';
    SET @batch_end_time = GETDATE();
    print'=================================';
    print'loading bronze layer completed';
    print'>>total load duration:'+ CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR) +'second';


END TRY
BEGIN CATCH
print'================================================';
print'error occur during loading bronze layer';
print'error message'+ error_message();
print'error message'+CAST(error_number()AS NVARCHAR);
PRINT'error message'+CAST(error_state()AS NVARCHAR);
END CATCH
END;
