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
    temp_table_name TEXT;
BEGIN
    local_file_path := 'C:\Users\Rafael\Documents\GitHub\PetroEnergy_DataWarehousing\datasets\source_econ';
    batch_start_time := CURRENT_TIMESTAMP;

    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Economic Data into Bronze Layer...';
    RAISE NOTICE '================================';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Economic Value Data...';
    ------------------------------------------------------------------
    start_time := CURRENT_TIMESTAMP;
    
    -- Create temporary table for economic value
    CREATE TEMP TABLE temp_econ_value (LIKE bronze.econ_value);
    
    -- Load CSV into temporary table
    EXECUTE format(
        'COPY temp_econ_value FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_value.csv'
    );

    -- Upsert from temporary table to bronze
    INSERT INTO bronze.econ_value
    SELECT * FROM temp_econ_value
    ON CONFLICT (year)
    DO UPDATE SET
        electricity_sales = EXCLUDED.electricity_sales,
        oil_revenues = EXCLUDED.oil_revenues,
        other_revenues = EXCLUDED.other_revenues,
        interest_income = EXCLUDED.interest_income,
        share_in_net_income_of_associate = EXCLUDED.share_in_net_income_of_associate,
        miscellaneous_income = EXCLUDED.miscellaneous_income;

    DROP TABLE temp_econ_value;
    
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Economic Expenditures Data...';
    ------------------------------------------------------------------
    start_time := CURRENT_TIMESTAMP;
    
    -- Create temporary table for expenditures
    CREATE TEMP TABLE temp_econ_expenditures (LIKE bronze.econ_expenditures);
    
    -- Load CSV into temporary table
    EXECUTE format(
        'COPY temp_econ_expenditures FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_expenditures.csv'
    );

    -- Upsert from temporary table to bronze
    INSERT INTO bronze.econ_expenditures
    SELECT * FROM temp_econ_expenditures
    ON CONFLICT (year, company_id, type)
    DO UPDATE SET
        government_payments = EXCLUDED.government_payments,
        supplier_spending_local = EXCLUDED.supplier_spending_local,
        supplier_spending_abroad = EXCLUDED.supplier_spending_abroad,
        community_investments = EXCLUDED.community_investments,
        depreciation = EXCLUDED.depreciation,
        depletion = EXCLUDED.depletion,
        others = EXCLUDED.others;

    DROP TABLE temp_econ_expenditures;
    
    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '-----------------';

    ------------------------------------------------------------------
    RAISE NOTICE 'Loading Capital Provider Payment Data...';
    ------------------------------------------------------------------
    start_time := CURRENT_TIMESTAMP;
    
    -- Create temporary table for capital provider payments
    CREATE TEMP TABLE temp_econ_capital_provider_payment (LIKE bronze.econ_capital_provider_payment);
    
    -- Load CSV into temporary table
    EXECUTE format(
        'COPY temp_econ_capital_provider_payment FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '\econ_capital_provider_payment.csv'
    );

    -- Upsert from temporary table to bronze
    INSERT INTO bronze.econ_capital_provider_payment
    SELECT * FROM temp_econ_capital_provider_payment
    ON CONFLICT (year)
    DO UPDATE SET
        interest = EXCLUDED.interest,
        dividends_to_nci = EXCLUDED.dividends_to_nci,
        dividends_to_parent = EXCLUDED.dividends_to_parent;

    DROP TABLE temp_econ_capital_provider_payment;
    
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
        -- Clean up temporary tables if they exist
        DROP TABLE IF EXISTS temp_econ_value;
        DROP TABLE IF EXISTS temp_econ_expenditures;
        DROP TABLE IF EXISTS temp_econ_capital_provider_payment;
        
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading econ bronze data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;
