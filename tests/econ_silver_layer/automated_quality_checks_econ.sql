-- ======================================
-- AUTOMATED QUALITY CHECK PROCEDURE FOR ECONOMIC DATA
-- ======================================

CREATE OR REPLACE PROCEDURE quality_check_economic_data()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
    temp_result DECIMAL;
BEGIN
    RAISE NOTICE '--- STARTING ECONOMIC DATA QUALITY CHECKS ---';

    -- Check: NULL year in econ_value
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_value WHERE year IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in econ_value.year (% records)', check_result;
    END IF;

    -- Check: Duplicate year in econ_value
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT year FROM silver.econ_value GROUP BY year HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate year in econ_value (% duplicates)', check_result;
    END IF;

    -- Check: Unreasonable year values in econ_value
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_value 
    WHERE year < 2000 OR year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Unreasonable year values in econ_value (% records)', check_result;
    END IF;

    -- Check: Total revenue calculation accuracy
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_value
    WHERE total_revenue != (COALESCE(electricity_sales, 0) + COALESCE(oil_revenues, 0) + COALESCE(other_revenues, 0) + 
                            COALESCE(interest_income, 0) + COALESCE(share_in_net_income_of_associate, 0) + 
                            COALESCE(miscellaneous_income, 0));
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Total revenue calculation mismatch in econ_value (% records)', check_result;
    END IF;

    -- Check: NULL year in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures WHERE year IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in econ_expenditures.year (% records)', check_result;
    END IF;

    -- Check: NULL company_id in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures WHERE company_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in econ_expenditures.company_id (% records)', check_result;
    END IF;

    -- Check: NULL type_id in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures WHERE type_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in econ_expenditures.type_id (% records)', check_result;
    END IF;

    -- Check: Duplicate primary key in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT year, company_id, type_id FROM silver.econ_expenditures 
        GROUP BY year, company_id, type_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate primary key in econ_expenditures (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.econ_expenditures
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in econ_expenditures (% entries)', check_result;
    END IF;

    -- Check: Referential integrity - type_id in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT type_id FROM silver.econ_expenditures
        WHERE type_id NOT IN (SELECT type_id FROM ref.expenditure_type)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid type_id in econ_expenditures (% entries)', check_result;
    END IF;

    -- Check: Supplier spending calculation accuracy
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures
    WHERE total_supplier_spending != (COALESCE(supplier_spending_local, 0) + COALESCE(supplier_spending_abroad, 0));
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Supplier spending calculation mismatch in econ_expenditures (% records)', check_result;
    END IF;

    -- Check: NULL year in econ_capital_provider_payment
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_capital_provider_payment WHERE year IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in econ_capital_provider_payment.year (% records)', check_result;
    END IF;

    -- Check: Duplicate year in econ_capital_provider_payment
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT year FROM silver.econ_capital_provider_payment GROUP BY year HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate year in econ_capital_provider_payment (% duplicates)', check_result;
    END IF;

    -- Check: Total dividends interest calculation accuracy
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_capital_provider_payment
    WHERE total_dividends_interest != (COALESCE(interest, 0) + COALESCE(dividends_to_nci, 0) + COALESCE(dividends_to_parent, 0));
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Total dividends interest calculation mismatch (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in econ_value
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_value WHERE created_at > updated_at;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: created_at > updated_at in econ_value (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in econ_expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures WHERE created_at > updated_at;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: created_at > updated_at in econ_expenditures (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in econ_capital_provider_payment
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_capital_provider_payment WHERE created_at > updated_at;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: created_at > updated_at in econ_capital_provider_payment (% records)', check_result;
    END IF;

    -- Warning: Check for zero total revenue
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_value WHERE total_revenue = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero total revenue in econ_value (% records)', check_result;
    END IF;

    -- Warning: Check for zero total expenditures
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures WHERE total_expenditures = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero total expenditures in econ_expenditures (% records)', check_result;
    END IF;

    -- Warning: Check for zero total dividends interest
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.econ_capital_provider_payment WHERE total_dividends_interest = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero total dividends interest (% records)', check_result;
    END IF;

    -- Final summary
    RAISE NOTICE '--- ECONOMIC DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);

    -- Additional summary information
    SELECT COUNT(*) INTO check_result FROM silver.econ_value;
    RAISE NOTICE 'Total Economic Value Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.econ_expenditures;
    RAISE NOTICE 'Total Economic Expenditure Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.econ_capital_provider_payment;
    RAISE NOTICE 'Total Capital Provider Payment Records: %', check_result;

    RAISE NOTICE '--- END OF ECONOMIC QUALITY CHECK PROCEDURE ---';
END;
$$;

-- Execute the quality check procedure
CALL quality_check_economic_data();
