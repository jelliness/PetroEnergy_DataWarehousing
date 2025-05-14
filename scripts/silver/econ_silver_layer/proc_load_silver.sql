/*
===============================================================================
Stored Procedure: Insert Trimmed Data from Bronze to Silver for Economic Data
===============================================================================
Script Purpose:
    This procedure transfers economic data from bronze tables to silver tables,
    performing data cleaning, validation, and derived calculations.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_econ_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Economic Silver Layer';
    RAISE NOTICE '================================================';

    BEGIN
        -- Loading silver.econ_value
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.econ_value';
        TRUNCATE TABLE silver.econ_value;
        RAISE NOTICE '>> Inserting Data Into: silver.econ_value';
        INSERT INTO silver.econ_value (
            year,
            electricity_sales,
            oil_revenues,
            other_revenues,
            interest_income,
            share_in_net_income_of_associate,
            miscellaneous_income
        )
        SELECT
            year,
            GREATEST(COALESCE(electricity_sales, 0), 0),
            GREATEST(COALESCE(oil_revenues, 0), 0),
            GREATEST(COALESCE(other_revenues, 0), 0),
            GREATEST(COALESCE(interest_income, 0), 0),
            GREATEST(COALESCE(share_in_net_income_of_associate, 0), 0),
            GREATEST(COALESCE(miscellaneous_income, 0), 0)
        FROM bronze.econ_value;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';

        -- Loading silver.econ_expenditures
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.econ_expenditures';
        TRUNCATE TABLE silver.econ_expenditures;
        RAISE NOTICE '>> Inserting Data Into: silver.econ_expenditures';
        INSERT INTO silver.econ_expenditures (
            year,
            company_id,
            type,
            government_payments,
            supplier_spending_local,
            supplier_spending_abroad,
            community_investments,
            depreciation,
            depletion,
            others
        )
        SELECT
            year,
            TRIM(company_id),
            TRIM(type),
            GREATEST(COALESCE(government_payments, 0), 0),
            GREATEST(COALESCE(supplier_spending_local, 0), 0),
            GREATEST(COALESCE(supplier_spending_abroad, 0), 0),
            GREATEST(COALESCE(community_investments, 0), 0),
            GREATEST(COALESCE(depreciation, 0), 0),
            GREATEST(COALESCE(depletion, 0), 0),
            GREATEST(COALESCE(others, 0), 0)
        FROM bronze.econ_expenditures;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';

        -- Loading silver.econ_capital_provider_payment
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Truncating Table: silver.econ_capital_provider_payment';
        TRUNCATE TABLE silver.econ_capital_provider_payment;
        RAISE NOTICE '>> Inserting Data Into: silver.econ_capital_provider_payment';
        INSERT INTO silver.econ_capital_provider_payment (
            year,
            interest,
            dividends_to_nci,
            dividends_to_parent
        )
        SELECT
            year,
            GREATEST(COALESCE(interest, 0), 0),
            GREATEST(COALESCE(dividends_to_nci, 0), 0),
            GREATEST(COALESCE(dividends_to_parent, 0), 0)
        FROM bronze.econ_capital_provider_payment;
        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';

        batch_end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '==========================================';
        RAISE NOTICE 'Loading Economic Silver Layer is Completed';
        RAISE NOTICE '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '==========================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '==========================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING ECONOMIC SILVER LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'Error Code: %', SQLSTATE;
            RAISE NOTICE '==========================================';
    END;
END;
$$;
