-- ======================================
-- AUTOMATED QUALITY CHECK PROCEDURE FOR CSR DATA
-- ======================================

CREATE OR REPLACE PROCEDURE quality_check_csr_data()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
    temp_result DECIMAL;
BEGIN
    RAISE NOTICE '--- STARTING CSR DATA QUALITY CHECKS ---';

    -- Check: NULL program_id in csr_programs
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_programs WHERE program_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_programs.program_id (% records)', check_result;
    END IF;

    -- Check: NULL program_name in csr_programs
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_programs WHERE program_name IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_programs.program_name (% records)', check_result;
    END IF;

    -- Check: Duplicate program_id in csr_programs
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT program_id FROM silver.csr_programs GROUP BY program_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate program_id in csr_programs (% duplicates)', check_result;
    END IF;

    -- Check: NULL project_id in csr_projects
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_projects WHERE project_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_projects.project_id (% records)', check_result;
    END IF;

    -- Check: NULL project_name in csr_projects
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_projects WHERE project_name IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_projects.project_name (% records)', check_result;
    END IF;

    -- Check: Duplicate project_id in csr_projects
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT project_id FROM silver.csr_projects GROUP BY project_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate project_id in csr_projects (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - program_id in csr_projects
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT program_id FROM silver.csr_projects
        WHERE program_id NOT IN (SELECT program_id FROM silver.csr_programs)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid program_id in csr_projects not found in csr_programs (% entries)', check_result;
    END IF;

    -- Check: NULL csr_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE csr_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_activity.csr_id (% records)', check_result;
    END IF;

    -- Check: NULL company_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE company_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_activity.company_id (% records)', check_result;
    END IF;

    -- Check: NULL project_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE project_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in csr_activity.project_id (% records)', check_result;
    END IF;

    -- Check: Duplicate csr_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT csr_id FROM silver.csr_activity GROUP BY csr_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate csr_id in csr_activity (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - company_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT company_id FROM silver.csr_activity
        WHERE company_id NOT IN (SELECT company_id FROM ref.company_main)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid company_id in csr_activity not found in company_main (% entries)', check_result;
    END IF;

    -- Check: Referential integrity - project_id in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT project_id FROM silver.csr_activity
        WHERE project_id NOT IN (SELECT project_id FROM silver.csr_projects)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid project_id in csr_activity not found in csr_projects (% entries)', check_result;
    END IF;

    -- Check: Negative csr_report values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE csr_report < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative values in csr_activity.csr_report (% records)', check_result;
    END IF;

    -- Check: Negative project_expenses values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE project_expenses < 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Negative values in csr_activity.project_expenses (% records)', check_result;
    END IF;

    -- Check: Unreasonable project_year values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity 
    WHERE project_year < 2000 OR project_year > EXTRACT(YEAR FROM CURRENT_DATE) + 1;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Unreasonable project_year values in csr_activity (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in csr_programs
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_programs WHERE date_created > date_updated;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: date_created > date_updated in csr_programs (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in csr_projects
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_projects WHERE date_created > date_updated;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: date_created > date_updated in csr_projects (% records)', check_result;
    END IF;

    -- Check: date_created > date_updated in csr_activity
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE date_created > date_updated;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: date_created > date_updated in csr_activity (% records)', check_result;
    END IF;

    -- Warning: Check for zero csr_report values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE csr_report = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero values in csr_activity.csr_report (% records)', check_result;
    END IF;

    -- Warning: Check for zero project_expenses values
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity WHERE project_expenses = 0;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Zero values in csr_activity.project_expenses (% records)', check_result;
    END IF;

    -- Check: Orphaned programs (programs with no projects)
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT cp.program_id FROM silver.csr_programs cp
        LEFT JOIN silver.csr_projects cpj ON cp.program_id = cpj.program_id
        WHERE cpj.program_id IS NULL
    ) orphans;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Orphaned programs with no projects (% programs)', check_result;
    END IF;

    -- Check: Orphaned projects (projects with no activities)
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT cpj.project_id FROM silver.csr_projects cpj
        LEFT JOIN silver.csr_activity ca ON cpj.project_id = ca.project_id
        WHERE ca.project_id IS NULL
    ) orphans;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'WARNING: Orphaned projects with no activities (% projects)', check_result;
    END IF;

    -- Final summary
    RAISE NOTICE '--- CSR DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);

    -- Additional summary information
    SELECT COUNT(*) INTO check_result FROM silver.csr_programs;
    RAISE NOTICE 'Total CSR Programs: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.csr_projects;
    RAISE NOTICE 'Total CSR Projects: %', check_result;
    
    SELECT COUNT(*) INTO check_result FROM silver.csr_activity;
    RAISE NOTICE 'Total CSR Activities: %', check_result;

    RAISE NOTICE '--- END OF CSR QUALITY CHECK PROCEDURE ---';
END;
$$;

-- Execute the quality check procedure
CALL quality_check_csr_data();
