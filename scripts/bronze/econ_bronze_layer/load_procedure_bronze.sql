-- UTILIZATION: 'EXEC bronze.load_econ_bronze;'

-- This script is used to create a stored procedure that performs the following tasks:
-- 1. Truncates the existing econ_value, econ_expenditures, and econ_capital_provider_payment tables in the bronze layer.
-- 2. Bulk inserts data from CSV files into the respective tables.
-- 3. Includes performance timing and load duration notices.
-- 4. Logs errors if any step fails.

CREATE OR REPLACE PROCEDURE bronze.load_econ_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    local_file_path TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    local_file_path := 'datasets\source_econ';  -- Adjust path
    batch_start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Economic Data into Bronze Layer...';
    RAISE NOTICE '================================';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Economic Value Data...';
    ------------------------------------------------------------------

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.econ_value...';
    TRUNCATE TABLE bronze.econ_value;

    RAISE NOTICE '>> Bulk inserting data into bronze.econ_value...';
    EXECUTE format(
        'COPY bronze.econ_value FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_value.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Economic Expenditures Data...';
    ------------------------------------------------------------------

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.econ_expenditures...';
    TRUNCATE TABLE bronze.econ_expenditures;

    RAISE NOTICE '>> Bulk inserting data into bronze.econ_expenditures...';
    EXECUTE format(
        'COPY bronze.econ_expenditures FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_expenditures.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Capital Provider Payment Data...';
    ------------------------------------------------------------------

    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Truncating table: bronze.econ_capital_provider_payment...';
    TRUNCATE TABLE bronze.econ_capital_provider_payment;

    RAISE NOTICE '>> Bulk inserting data into bronze.econ_capital_provider_payment...';
    EXECUTE format(
        'COPY bronze.econ_capital_provider_payment FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_capital_provider_payment.csv'
    );

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    ------------------------------------------------------------------
    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading of Economic Bronze Data Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading econ bronze data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
