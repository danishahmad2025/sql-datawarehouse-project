/*********************************************************************************************
stored procedure:load silver layer(source -> silver)
 Purpose:
   -this stored procedure performs ETL(Extract,Transform,Load) process to populated 'silver' schema
    tables from 'bronze' schema
Action Performed:
                 -Truncate silver tables.
                 -Inserts transformed and cleansed data from bronze tables.

*stores procedure doesnot accept parameters or return any value
 
 How to Run:
   1. Run this script once to create/update the procedure.
   2. Then execute it with:  EXEC bronze.load_bronze;
  eg:   GO
        EXEC bronze.load_silver;

*********************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME ;
    BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT'==============================================================';
    PRINT'loading silver layer';
    PRINT'==============================================================';

    PRINT'-----------------------------------------------------------';
    PRINT'loading crm table';
    PRINT'-----------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT'>>>truncating table:silver.crm_cust_info>>>';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT'>>inserting the data into table:silver.crm_cust_info>> ';
    INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_martial_status,
    cst_gndr,
    cst_create_date)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_martial_status)) ='S' THEN 'single'
        WHEN UPPER(TRIM(cst_martial_status)) ='M' THEN 'married'
        ELSE 'n/a'
    END AS cst_martial_status,

    CASE
        WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'female'
        WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
 FROM(
    SELECT*,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER By cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
 ) t 
 WHERE flag_last = 1;
 SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';



    SET @start_time = GETDATE();
    PRINT'>>>truncating table:silver.crm_prd_info>>>';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT'>>inserting the data into table:silver.crm_prd_info>> ';
    INSERT INTO silver.crm_prd_info(
    prd_id ,
    cat_id ,
    prd_key ,
    prd_nm ,
    prd_cost ,
    prd_line ,
    prd_start_dt ,
    prd_end_dt
     )
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost,0)AS prd_cost,
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE )AS prd_start_dt,
    CAST( LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
    from bronze.crm_prd_info
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';

    
    PRINT'>>>truncating table:silver.crm_sales_details>>>';
    TRUNCATE TABLE silver.crm_sales_details; 
    PRINT'>>inserting the data into table:silver.crm_sales_details>> ';    
    INSERT INTO silver.crm_sales_details(
    sls_order_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
    )
     SELECT
    sls_order_num ,
    sls_prd_key ,
    sls_cust_id ,
    CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END AS sls_order_dt,
   CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END AS sls_ship_dt ,
    CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END AS sls_due_dt ,
   
    CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN  sls_quantity * ABS(sls_price)
    ELSE sls_sales
    END AS sls_sales,
     sls_quantity ,
   CASE WHEN sls_price IS NULL OR sls_price <=0 
   THEN sls_sales / nullif (sls_quantity,0)
   ELSE sls_price 
   END AS sls_price
   FROM bronze.crm_sales_details
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';


    PRINT'-----------------------------------------------------------';
    PRINT'loading erp table';
    PRINT'-----------------------------------------------------------';




    PRINT'>>>truncating table:silver.erp_cust_az12>>>';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT'>>inserting the data into table:silver.erp_cust_az12>> ';
    INSERT INTO silver.erp_cust_az12(
    CID,
    BDATE,
    GEN
        )
   SELECT
   CASE WHEN CID like 'NAS%' then SUBSTRING(CID,4,LEN(CID))
   ELSE CID
   END AS CID,
   CASE WHEN BDATE> GETDATE() THEN NULL 
   ELSE BDATE
   END AS BDATE,
   CASE WHEN UPPER(TRIM(GEN))IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(GEN)) IN ('M' , 'MALE') THEN 'Male'
        ELSE 'n/a'
        END AS GEN
   from bronze.erp_cust_az12
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';




        PRINT'>>>truncating table:silver.erp_loc_a101>>>';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT'>>inserting the data into table:silver.erp_loc_a101>> ';
        INSERT INTO silver.erp_loc_a101
        (CID,CNTRY)
        SELECT 
         REPLACE(CID,'-','') CID,
    CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
    WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
    WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
   ELSE TRIM(CNTRY)
   END AS CNTRY
    from bronze.erp_loc_a101
    SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';




        PRINT'>>>truncating table:silver.erp_px_cat_g1v2>>>';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT'>>inserting the data into table:silver.erp_px_cat_g1v2>> ';
        INSERT INTO silver.erp_px_cat_g1v2(
        ID,CAT,SUBCAT,MAINTENANCE
        )
        SELECT 
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
        from bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
    print'>>load duration:'+ CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'second';
    print'>>----------------------';


    END TRY
    BEGIN CATCH
    print'================================================';
    print'error occur during loading silver layer';
    print'error message'+ error_message();
    print'error message'+CAST(error_number()AS NVARCHAR);
    PRINT'error message'+CAST(error_state()AS NVARCHAR);
    END CATCH
    END;
   
