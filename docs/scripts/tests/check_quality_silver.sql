/* 
=======================================================================================
Quality Checks
=======================================================================================
 script purpose:

            this script perform various quality checks for data consistency,accuracy
            and standardization across the 'silver' schemas. it includes check for;
            -null or duplicate primary keys.
            -unwanted spaces in string fields.
            -data standardization and consistency.
            -invalid date range and orders.
            -data consistency between related fields.

 usage notes:
        -run these checks after data loading silver layer.
        -investigate and resolve any discrepancies found during the check.
=========================================================================================
*/


-- =======================================================================
-- checking 'silver.crm_cust_info'
-- =======================================================================
-- check for nulls or duplicate in primary key
-- expectation: no results
SELECT
        cst_id,
        COUNT(*)
    FROM silver.crm_cust_info
    GROUP BY cst_id
    HAVING COUNT(*) >1 OR cst_id IS NULL;

-- check for unwanted spaces 
-- expectation: no results
SELECT
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- data standardization & consistency
SELECT DISTINCT
    cst_martial_status
FROM silver.crm_cust_info;


-- =======================================================================
-- checking 'silver.crm_prd_info'
-- =======================================================================
-- check for nulls or duplicate in primary key
-- expectation: no results
SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--check unwanted spaces
--expectation : no results
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--check for mulls or negative values in cost
--expectation : no results
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- data standarddization and consistency
SELECT DISTINCT
    prd_line
FROM silver.crm_prd_info;

-- check for invalid date orders (start date > end date)
--expectation : no results
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt < prd_end_dt;



-- =======================================================================
-- checking 'silver.crm_sales_details'
-- =======================================================================
-- check for invalid dates
-- expectation: no results
SELECT
    NULLIF(sls_due_dt,0) AS sls_due_dt
    FROM silver.crm_sales_details
    WHERE sls_due_dt <= 0
         OR LEN(sls_due_dt) != 8
         OR sls_order_dt > 20500101
         OR sls_due_dt < 19000101;

-- check for for invalid date orders (order date > shipping/due dates)
-- expectation: no results
    SELECT *
    FROM silver.crm_sales_details
    WHERE sls_order_dt > sls_ship_dt
        OR sls_order_dt > sls_due_dt;

-- check data consistency : sales = quantity * price
-- expectation: no results
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0 
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity,sls_price;



-- =======================================================================
-- checking 'silver.erp_cust_az12'
-- =======================================================================
-- identity out-of-range dates
-- expectation: birthdates between 1924-01-01 and today
SELECT DISTINCT
    BDATE
FROM silver.erp_cust_az12
WHERE BDATE < '1924-01-01'
OR BDATE > GETDATE();

--data standardization & consistency 
SELECT DISTINCT
    GEN
FROM silver>erp_cust_az12;



-- =======================================================================
-- checking 'silver.erp_loc_a101'
-- =======================================================================
-- data standardization and consistency
SELECT DISTINCT
    CNTRY
FROM silver.erp_loc_a101
ORDER BY CNTRY;



-- =======================================================================
-- checking 'silver.erp_px_cat_g1v2'
-- =======================================================================
-- check for unwanted spaces
-- expectation: no results 
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE CAT != TRIM(cat)
    OR SUBCAT != TRIM(SUBCAT)
    OR MAINTENANCE != TRIM(MAINTENANCE);

--data standardization and consistency
SELECT DISTINCT
    MAINTENANCE
FROM silver.erp_px_cat_g1v2;

