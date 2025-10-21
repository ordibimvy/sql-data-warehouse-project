/* 
=====================================================================
DDL Script: Create Bronze Tables 
=====================================================================
Purpose:
  This script creates tables in the 'bronze' schema, dropping existing 
  tables if they already exist. It ensures the DDL structure is properly 
  defined for all Bronze-layer tables.
=====================================================================
*/

-- ============================================================
-- 1. CRM Customer Info Table
-- ============================================================
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50), -- fixed typo: maritial → marital
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO

-- ============================================================
-- 2. CRM Product Info Table
-- ============================================================
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost DECIMAL(10,2), -- more realistic type
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO

-- ============================================================
-- 3. CRM Sales Details Table
-- ============================================================
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL 
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales DECIMAL(10,2),
    sls_quantity INT,
    sls_price DECIMAL(10,2)
);
GO

-- ============================================================
-- 4. ERP Location (A101)
-- ============================================================
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- ============================================================
-- 5. ERP Customer (AZ12)
-- ============================================================
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);
GO

-- ============================================================
-- 6. ERP Product Category (G1V2)
-- ============================================================
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL 
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
