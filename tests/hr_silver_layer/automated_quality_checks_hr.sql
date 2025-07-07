-- ======================================
-- AUTOMATED QUALITY CHECK PROCEDURE FOR HR DATA
-- ======================================

CREATE OR REPLACE PROCEDURE quality_check_hr_data()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
    temp_result DECIMAL;
BEGIN
    RAISE NOTICE '--- STARTING HR DATA QUALITY CHECKS ---';

    -- Check: NULL employee_id in hr_demographics
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_demographics WHERE employee_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_demographics.employee_id (% records)', check_result;
    END IF;

    -- Check: NULL company_id in hr_demographics
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_demographics WHERE company_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_demographics.company_id (% records)', check_result;
    END IF;

    -- Check: Duplicate employee_id in hr_demographics
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT employee_id FROM silver.hr_demographics GROUP BY employee_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate employee_id in hr_demographics (% duplicates)', check_result;
    END IF;

    -- Check: Unreasonable birth dates
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_demographics 
    WHERE birthdate > CURRENT_DATE 
       OR birthdate < '1940-01-01'::DATE
       OR EXTRACT(YEAR FROM AGE(birthdate)) < 18
       OR EXTRACT(YEAR FROM AGE(birthdate)) > 80;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Unreasonable birth dates in hr_demographics (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in hr_demographics
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.hr_demographics
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in hr_demographics (% entries)', check_result;
    END IF;

    -- Check: Referential integrity - position_id in hr_demographics
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT position_id FROM silver.hr_demographics
        WHERE position_id NOT IN (SELECT position_id FROM ref.hr_position)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid position_id in hr_demographics (% entries)', check_result;
    END IF;

    -- Check: NULL parental_leave_id in hr_parental_leave
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_parental_leave WHERE parental_leave_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_parental_leave.parental_leave_id (% records)', check_result;
    END IF;

    -- Check: Duplicate parental_leave_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT parental_leave_id FROM silver.hr_parental_leave GROUP BY parental_leave_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate parental_leave_id in hr_parental_leave (% duplicates)', check_result;
    END IF;

    -- Check: Negative or zero days in parental leave
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_parental_leave WHERE days <= 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative or zero days in hr_parental_leave (% records)', check_result;
    END IF;

    -- Check: Referential integrity - employee_id in hr_parental_leave
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT employee_id FROM silver.hr_parental_leave
        WHERE employee_id NOT IN (SELECT employee_id FROM silver.hr_demographics)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid employee_id in hr_parental_leave (% entries)', check_result;
    END IF;

    -- Check: Date consistency in parental leave (end_date >= start date)
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_parental_leave WHERE end_date < date;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: End date before start date in hr_parental_leave (% records)', check_result;
    END IF;

    -- Check: NULL training_id in hr_training
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_training WHERE training_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_training.training_id (% records)', check_result;
    END IF;

    -- Check: Duplicate training_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT training_id FROM silver.hr_training GROUP BY training_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate training_id in hr_training (% duplicates)', check_result;
    END IF;

    -- Check: Negative or zero values in training
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_training 
    WHERE training_hours <= 0 OR number_of_participants <= 0 OR total_training_hours <= 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative or zero values in hr_training (% records)', check_result;
    END IF;

    -- Check: Total training hours calculation
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_training
    WHERE total_training_hours != (training_hours * number_of_participants);
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Total training hours calculation mismatch (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in hr_training
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.hr_training
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in hr_training (% entries)', check_result;
    END IF;

    -- Check: NULL safety_workdata_id in hr_safety_workdata
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_safety_workdata WHERE safety_workdata_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_safety_workdata.safety_workdata_id (% records)', check_result;
    END IF;

    -- Check: Duplicate safety_workdata_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT safety_workdata_id FROM silver.hr_safety_workdata GROUP BY safety_workdata_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate safety_workdata_id in hr_safety_workdata (% duplicates)', check_result;
    END IF;

    -- Check: Negative or zero values in safety workdata
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_safety_workdata WHERE manpower <= 0 OR manhours <= 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative or zero values in hr_safety_workdata (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in hr_safety_workdata
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.hr_safety_workdata
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in hr_safety_workdata (% entries)', check_result;
    END IF;

    -- Check: NULL osh_id in hr_occupational_safety_health
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_occupational_safety_health WHERE osh_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in hr_occupational_safety_health.osh_id (% records)', check_result;
    END IF;

    -- Check: Duplicate osh_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT osh_id FROM silver.hr_occupational_safety_health GROUP BY osh_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate osh_id in hr_occupational_safety_health (% duplicates)', check_result;
    END IF;

    -- Check: Negative incident counts
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_occupational_safety_health WHERE incident_count < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative incident counts in hr_occupational_safety_health (% records)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in hr_occupational_safety_health
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.hr_occupational_safety_health
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in hr_occupational_safety_health (% entries)', check_result;
    END IF;

    -- Check: Date consistency in hr_tenure (end_date >= start_date)
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_tenure WHERE end_date < start_date;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: End date before start date in hr_tenure (% records)', check_result;
    END IF;

    -- Check: Negative tenure length
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.hr_tenure WHERE tenure_length < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative tenure length in hr_tenure (% records)', check_result;
    END IF;

    -- Check: Referential integrity - employee_id in hr_tenure
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT employee_id FROM silver.hr_tenure
        WHERE employee_id NOT IN (SELECT employee_id FROM silver.hr_demographics)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid employee_id in hr_tenure (% entries)', check_result;
    END IF;

    -- Final summary
    RAISE NOTICE '--- HR DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);

    -- Additional summary information
    SELECT COUNT(*) INTO check_result FROM silver.hr_demographics;
    RAISE NOTICE 'Total Employee Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.hr_training;
    RAISE NOTICE 'Total Training Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.hr_safety_workdata;
    RAISE NOTICE 'Total Safety Workdata Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.hr_occupational_safety_health;
    RAISE NOTICE 'Total OSH Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.hr_parental_leave;
    RAISE NOTICE 'Total Parental Leave Records: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.hr_tenure;
    RAISE NOTICE 'Total Tenure Records: %', check_result;

    RAISE NOTICE '--- END OF HR QUALITY CHECK PROCEDURE ---';
END;
$$;

-- Execute the quality check procedure
CALL quality_check_hr_data();
