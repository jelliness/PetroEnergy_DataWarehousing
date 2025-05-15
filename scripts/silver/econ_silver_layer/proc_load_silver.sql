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
        RAISE NOTICE '>> Upserting Data Into: silver.econ_value';
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
        FROM bronze.econ_value
        ON CONFLICT (year)  -- Assuming year is unique
        DO UPDATE SET
            electricity_sales = EXCLUDED.electricity_sales,
            oil_revenues = EXCLUDED.oil_revenues,
            other_revenues = EXCLUDED.other_revenues,
            interest_income = EXCLUDED.interest_income,
            share_in_net_income_of_associate = EXCLUDED.share_in_net_income_of_associate,
            miscellaneous_income = EXCLUDED.miscellaneous_income,
            updated_at = CURRENT_TIMESTAMP;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';

        -- Loading silver.econ_expenditures
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Upserting Data Into: silver.econ_expenditures';
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
        FROM bronze.econ_expenditures
        ON CONFLICT (year, company_id, type)  -- Composite unique key
        DO UPDATE SET
            government_payments = EXCLUDED.government_payments,
            supplier_spending_local = EXCLUDED.supplier_spending_local,
            supplier_spending_abroad = EXCLUDED.supplier_spending_abroad,
            community_investments = EXCLUDED.community_investments,
            depreciation = EXCLUDED.depreciation,
            depletion = EXCLUDED.depletion,
            others = EXCLUDED.others,
            updated_at = CURRENT_TIMESTAMP;

        end_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
        RAISE NOTICE '>> -------------';

        -- Loading silver.econ_capital_provider_payment
        start_time := CLOCK_TIMESTAMP();
        RAISE NOTICE '>> Upserting Data Into: silver.econ_capital_provider_payment';
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
        FROM bronze.econ_capital_provider_payment
        ON CONFLICT (year)  -- Assuming year is unique
        DO UPDATE SET
            interest = EXCLUDED.interest,
            dividends_to_nci = EXCLUDED.dividends_to_nci,
            dividends_to_parent = EXCLUDED.dividends_to_parent,
            updated_at = CURRENT_TIMESTAMP;

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
