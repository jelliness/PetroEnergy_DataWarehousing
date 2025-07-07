-- ======================================
-- AUTOMATED QUALITY CHECK PROCEDURE FOR ENVIRONMENTAL DATA
-- ======================================

CREATE OR REPLACE PROCEDURE quality_check_environmental_data()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
    temp_result DECIMAL;
BEGIN
    RAISE NOTICE '--- STARTING ENVIRONMENTAL DATA QUALITY CHECKS ---';

    -- Check: NULL company_id in ref.company_main
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM ref.company_main WHERE company_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in ref.company_main.company_id (% records)', check_result;
    END IF;

    -- ===== ENVI_COMPANY_PROPERTY TABLE CHECKS =====
    -- Check: NULL cp_id in envi_company_property
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_company_property WHERE cp_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_company_property.cp_id (% records)', check_result;
    END IF;

    -- Check: Duplicate cp_id in envi_company_property
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT cp_id FROM silver.envi_company_property GROUP BY cp_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate cp_id in envi_company_property (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_company_property
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_company_property
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_company_property (% entries)', check_result;
    END IF;

    -- ===== ENVI_WATER_ABSTRACTION TABLE CHECKS =====
    -- Check: NULL wa_id in envi_water_abstraction
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_abstraction WHERE wa_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_water_abstraction.wa_id (% records)', check_result;
    END IF;

    -- Check: Duplicate wa_id in envi_water_abstraction
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT wa_id FROM silver.envi_water_abstraction GROUP BY wa_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate wa_id in envi_water_abstraction (% duplicates)', check_result;
    END IF;

    -- Check: Negative volume in envi_water_abstraction
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_abstraction WHERE volume < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative volume in envi_water_abstraction (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_water_abstraction
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_water_abstraction
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_water_abstraction (% entries)', check_result;
    END IF;

    -- ===== ENVI_WATER_DISCHARGE TABLE CHECKS =====
    -- Check: NULL wd_id in envi_water_discharge
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_discharge WHERE wd_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_water_discharge.wd_id (% records)', check_result;
    END IF;

    -- Check: Duplicate wd_id in envi_water_discharge
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT wd_id FROM silver.envi_water_discharge GROUP BY wd_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate wd_id in envi_water_discharge (% duplicates)', check_result;
    END IF;

    -- Check: Negative volume in envi_water_discharge
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_discharge WHERE volume < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative volume in envi_water_discharge (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_water_discharge
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_water_discharge
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_water_discharge (% entries)', check_result;
    END IF;

    -- ===== ENVI_WATER_CONSUMPTION TABLE CHECKS =====
    -- Check: NULL wc_id in envi_water_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_consumption WHERE wc_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_water_consumption.wc_id (% records)', check_result;
    END IF;

    -- Check: Duplicate wc_id in envi_water_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT wc_id FROM silver.envi_water_consumption GROUP BY wc_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate wc_id in envi_water_consumption (% duplicates)', check_result;
    END IF;

    -- Check: Negative volume in envi_water_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_consumption WHERE volume < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative volume in envi_water_consumption (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_water_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_water_consumption
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_water_consumption (% entries)', check_result;
    END IF;

    -- ===== ENVI_DIESEL_CONSUMPTION TABLE CHECKS =====

    -- Check: NULL dc_id in envi_diesel_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_diesel_consumption WHERE dc_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_diesel_consumption.dc_id (% records)', check_result;
    END IF;

    -- Check: Duplicate dc_id in envi_diesel_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT dc_id FROM silver.envi_diesel_consumption GROUP BY dc_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate dc_id in envi_diesel_consumption (% duplicates)', check_result;
    END IF;

    -- Check: Negative consumption in envi_diesel_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_diesel_consumption WHERE consumption < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative consumption in envi_diesel_consumption (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_diesel_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_diesel_consumption
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_diesel_consumption (% entries)', check_result;
    END IF;

    -- Check: Referential integrity - cp_id in envi_diesel_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT cp_id FROM silver.envi_diesel_consumption
        WHERE cp_id IS NOT NULL AND cp_id NOT IN (SELECT cp_id FROM silver.envi_company_property)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid cp_id in envi_diesel_consumption (% entries)', check_result;
    END IF;

    -- ===== ENVI_ELECTRIC_CONSUMPTION TABLE CHECKS =====

    -- Check: NULL ec_id in envi_electric_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_electric_consumption WHERE ec_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_electric_consumption.ec_id (% records)', check_result;
    END IF;

    -- Check: Duplicate ec_id in envi_electric_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT ec_id FROM silver.envi_electric_consumption GROUP BY ec_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate ec_id in envi_electric_consumption (% duplicates)', check_result;
    END IF;

    -- Check: Negative consumption in envi_electric_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_electric_consumption WHERE consumption < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative consumption in envi_electric_consumption (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_electric_consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_electric_consumption
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_electric_consumption (% entries)', check_result;
    END IF;

    -- ===== ENVI_NON_HAZARD_WASTE TABLE CHECKS =====
    -- Check: NULL nhw_id in envi_non_hazard_waste
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_non_hazard_waste WHERE nhw_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_non_hazard_waste.nhw_id (% records)', check_result;
    END IF;

    -- Check: Duplicate nhw_id in envi_non_hazard_waste
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT nhw_id FROM silver.envi_non_hazard_waste GROUP BY nhw_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate nhw_id in envi_non_hazard_waste (% duplicates)', check_result;
    END IF;

    -- Check: Negative waste in envi_non_hazard_waste
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_non_hazard_waste WHERE waste < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative waste in envi_non_hazard_waste (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_non_hazard_waste
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_non_hazard_waste
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_non_hazard_waste (% entries)', check_result;
    END IF;

    -- ===== ENVI_HAZARD_WASTE_GENERATED TABLE CHECKS =====
    -- Check: NULL hwg_id in envi_hazard_waste_generated
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_generated WHERE hwg_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_hazard_waste_generated.hwg_id (% records)', check_result;
    END IF;

    -- Check: Duplicate hwg_id in envi_hazard_waste_generated
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT hwg_id FROM silver.envi_hazard_waste_generated GROUP BY hwg_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate hwg_id in envi_hazard_waste_generated (% duplicates)', check_result;
    END IF;

    -- Check: Negative waste_generated in envi_hazard_waste_generated
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_generated WHERE waste_generated < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative waste_generated in envi_hazard_waste_generated (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_hazard_waste_generated
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_hazard_waste_generated
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_hazard_waste_generated (% entries)', check_result;
    END IF;

    -- ===== ENVI_HAZARD_WASTE_DISPOSED TABLE CHECKS =====
    -- Check: NULL hwd_id in envi_hazard_waste_disposed
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_disposed WHERE hwd_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in envi_hazard_waste_disposed.hwd_id (% records)', check_result;
    END IF;

    -- Check: Duplicate hwd_id in envi_hazard_waste_disposed
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT hwd_id FROM silver.envi_hazard_waste_disposed GROUP BY hwd_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate hwd_id in envi_hazard_waste_disposed (% duplicates)', check_result;
    END IF;

    -- Check: Negative waste_disposed in envi_hazard_waste_disposed
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_disposed WHERE waste_disposed < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative waste_disposed in envi_hazard_waste_disposed (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in envi_hazard_waste_disposed
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.envi_hazard_waste_disposed
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in envi_hazard_waste_disposed (% entries)', check_result;
    END IF;

    -- ===== BUSINESS LOGIC CHECKS =====
    -- Warning: Check for zero volume in water abstraction
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_abstraction WHERE volume = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero volume in envi_water_abstraction (% records)', check_result;
    END IF;

    -- Warning: Check for zero volume in water discharge
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_discharge WHERE volume = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero volume in envi_water_discharge (% records)', check_result;
    END IF;

    -- Warning: Check for zero volume in water consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_consumption WHERE volume = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero volume in envi_water_consumption (% records)', check_result;
    END IF;

    -- Warning: Check for zero consumption in diesel consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_diesel_consumption WHERE consumption = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero consumption in envi_diesel_consumption (% records)', check_result;
    END IF;

    -- Warning: Check for zero consumption in electric consumption
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.envi_electric_consumption WHERE consumption = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero consumption in envi_electric_consumption (% records)', check_result;
    END IF;

    -- Final summary
    RAISE NOTICE '--- ENVIRONMENTAL DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);

    -- Additional summary information
    SELECT COUNT(*) INTO check_result FROM silver.envi_company_property;
    RAISE NOTICE 'Total Company Property Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_abstraction;
    RAISE NOTICE 'Total Water Abstraction Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_discharge;
    RAISE NOTICE 'Total Water Discharge Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_water_consumption;
    RAISE NOTICE 'Total Water Consumption Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_diesel_consumption;
    RAISE NOTICE 'Total Diesel Consumption Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_electric_consumption;
    RAISE NOTICE 'Total Electric Consumption Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_non_hazard_waste;
    RAISE NOTICE 'Total Non-Hazardous Waste Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_generated;
    RAISE NOTICE 'Total Hazardous Waste Generated Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.envi_hazard_waste_disposed;
    RAISE NOTICE 'Total Hazardous Waste Disposed Records: %', check_result;

    RAISE NOTICE '--- END OF ENVIRONMENTAL QUALITY CHECK PROCEDURE ---';
END;
$$;

-- Execute the quality check procedure
CALL quality_check_environmental_data();
