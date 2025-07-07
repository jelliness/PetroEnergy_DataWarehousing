CREATE OR REPLACE PROCEDURE quality_check_energy_records()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
BEGIN
    RAISE NOTICE '--- STARTING DATA QUALITY CHECKS ---';

    -- Check: NULL energy_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE energy_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in energy_id (% records)', check_result;
    END IF;

    -- Check: NULL power_plant_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE power_plant_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in power_plant_id (% records)', check_result;
    END IF;

    -- Check: NULL date_generated
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE date_generated IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in date_generated (% records)', check_result;
    END IF;

    -- Check: Duplicate energy_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT energy_id FROM silver.csv_energy_records GROUP BY energy_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate energy_id (% duplicates)', check_result;
    END IF;

    -- Check: Negative energy_generated_kwh
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE energy_generated_kwh < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative values in energy_generated_kwh (% records)', check_result;
    END IF;

    -- Check: Negative co2_avoidance_kg
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE co2_avoidance_kg < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative values in co2_avoidance_kg (% records)', check_result;
    END IF;

    -- Check: Future dates
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE date_generated > CURRENT_DATE;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Records with future date_generated (% records)', check_result;
    END IF;

    -- Check: create_at > updated_at
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE create_at > updated_at;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: create_at is later than updated_at (% records)', check_result;
    END IF;

    -- Check: power_plant_id referential integrity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT power_plant_id
        FROM silver.csv_energy_records
        WHERE power_plant_id NOT IN (
            SELECT power_plant_id FROM ref.ref_power_plants
        )
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid power_plant_id not found in ref_power_plants (% entries)', check_result;
    END IF;

    -- Check: NULL company_id in ref_power_plants
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM ref.ref_power_plants WHERE company_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULL company_id in ref_power_plants (% records)', check_result;
    END IF;

    -- Check: Negative emission factor values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM ref.ref_emission_factors
    WHERE kg_co2_per_kwh < 0 OR co2_emitted_kg < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative values in ref_emission_factors (% records)', check_result;
    END IF;

    -- Check: Zero energy_generated_kwh
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csv_energy_records WHERE energy_generated_kwh = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        -- still a warning, so fail
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: energy_generated_kwh = 0 in % records', check_result;
    END IF;

   
    -- Final summary
    RAISE NOTICE '--- DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);

    RAISE NOTICE '--- END OF PROCEDURE ---';
END;
$$;

CALL quality_check_energy_records();
