/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None.
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE
        @start_time         DATETIME,
        @end_time           DATETIME,
        @batch_start_time   DATETIME,
        @batch_end_time     DATETIME,
        @header_separator   NVARCHAR(50);

    SET @header_separator = '================================================';
    SET @start_time = GETDATE();

    BEGIN TRY
        -- #---------------------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
            PRINT @header_separator;
            PRINT '[INFO] Loading bronze layer...';
            PRINT @header_separator;
            -- #---------------------------------------------------------------------------------
            -- Load CRM data source --
            PRINT @header_separator;
            PRINT '[INFO] Loading CRM tables';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.crm_cust_info';
                TRUNCATE TABLE bronze.crm_cust_info;

                PRINT '>> Inserting data into table: bronze.crm_cust_info';
                BULK INSERT bronze.crm_cust_info
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
                WITH (
                    FIRSTROW = 2, -- ignore header
                    FIELDTERMINATOR = ',',
                    TABLOCK -- lock whole table while copying improving the performance
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            -- SELECT * FROM bronze.crm_cust_info;
            -- SELECT COUNT(*) FROM bronze.crm_cust_info;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.crm_prd_info';
                TRUNCATE TABLE bronze.crm_prd_info;

                PRINT '>> Inserting data into table: bronze.crm_prd_info';
                BULK INSERT bronze.crm_prd_info
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.crm_sales_details';
                TRUNCATE TABLE bronze.crm_sales_details;

                PRINT '>> Inserting data into table: bronze.crm_sales_details';
                BULK INSERT bronze.crm_sales_details
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ',',
                    TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            -- Load ERP data source --
            PRINT '';
            PRINT @header_separator;
            PRINT '[INFO] Loading EPR tables';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.erp_cust_az12';
                TRUNCATE TABLE bronze.erp_cust_az12;

                BULK INSERT bronze.erp_cust_az12
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
                WITH (
                    FIRSTROW = 2, -- ignore header
                    FIELDTERMINATOR = ',',
                    TABLOCK -- lock whole table while copying improving the performance
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.erp_loc_a101';
                TRUNCATE TABLE bronze.erp_loc_a101;

                PRINT '>> Inserting data into table: bronze.erp_loc_a101';
                BULK INSERT bronze.erp_loc_a101
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
                WITH (
                    FIRSTROW = 2, -- ignore header
                    FIELDTERMINATOR = ',',
                    TABLOCK -- lock whole table while copying improving the performance
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            -- #---------------------------------------------------------------------------------
            SET @start_time = GETDATE();
                PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
                TRUNCATE TABLE bronze.erp_px_cat_g1v2;

                PRINT '>> Inserting data into table: bronze.erp_px_cat_g1v2';
                BULK INSERT bronze.erp_px_cat_g1v2
                FROM 'C:\Users\luiz-\OneDrive\Documentos\Project\DE\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
                WITH (
                    FIRSTROW = 2, -- ignore header
                    FIELDTERMINATOR = ',',
                    TABLOCK -- lock whole table while copying improving the performance
                );
            SET @end_time = GETDATE();
            PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
            PRINT @header_separator;

            PRINT @header_separator;
            PRINT '[INFO] Loading bronze layer DONE!';
            PRINT @header_separator;

        SET @batch_end_time = GETDATE();
        PRINT '>> Bronze layer load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 's';

    END TRY

    -- #---------------------------------------------------------------------------------
    BEGIN CATCH
        PRINT @header_separator;
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT @header_separator;
    END CATCH
END