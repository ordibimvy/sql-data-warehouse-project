/*
===============================================================================
Store Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
  This store procedure peformce the ETL process to populate the 'silver' schema
    tables from the 'bronze' schema.
  It performs the following actions:
  - Truncates the silver tables before loading data.
  - Inserts transformed and ceansed data from Bronze into Silver tables

Parameters: 
  none.
This stored procedure does not accept any parameters or rturn any values.

Usage Example:
  EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME = GETDATE(),
        @batch_end_time DATETIME;

    BEGIN TRY
        BEGIN TRANSACTION;

        PRINT '================ Loading Silver Layer ================';

        ----------------------------------------------------------------------
        -- CRM CUSTOMER
        ----------------------------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id, cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE WHEN UPPER(TRIM(cst_marital_status)) IN ('S','SINGLE') THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status)) IN ('M','MARRIED') THEN 'Married'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a' END,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
            FROM bronze.crm_cust_info WHERE cst

