-- ======================================
-- AUTOMATED QUALITY CHECK PROCEDURE FOR PUBLIC SCHEMA
-- ======================================

CREATE OR REPLACE PROCEDURE quality_check_public_schema()
LANGUAGE plpgsql
AS $$
DECLARE
    total_checks INT := 0;
    passed_checks INT := 0;
    failed_checks INT := 0;
    check_result INT;
BEGIN
    RAISE NOTICE '--- STARTING PUBLIC SCHEMA DATA QUALITY CHECKS ---';

    -- ===== status TABLE CHECKS =====
    -- Check: NULL status_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.status WHERE status_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in status.status_id (% records)', check_result;
    END IF;

    -- Check: Duplicate status_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT status_id FROM public.status GROUP BY status_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate status_id in status (% duplicates)', check_result;
    END IF;

    -- ===== roles TABLE CHECKS =====
    -- Check: NULL role_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.roles WHERE role_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in roles.role_id (% records)', check_result;
    END IF;

    -- Check: Duplicate role_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT role_id FROM public.roles GROUP BY role_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate role_id in roles (% duplicates)', check_result;
    END IF;

    -- ===== account TABLE CHECKS =====
    -- Check: NULL account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.account WHERE account_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in account.account_id (% records)', check_result;
    END IF;

    -- Check: Duplicate account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT account_id FROM public.account GROUP BY account_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate account_id in account (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - account_role
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT account_role FROM public.account
        WHERE account_role NOT IN (SELECT role_id FROM public.roles)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid account_role in account (% entries)', check_result;
    END IF;

    -- ===== user_profile TABLE CHECKS =====
    -- Check: NULL account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.user_profile WHERE account_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in user_profile.account_id (% records)', check_result;
    END IF;

    -- Check: Duplicate account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT account_id FROM public.user_profile GROUP BY account_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate account_id in user_profile (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT account_id FROM public.user_profile
        WHERE account_id NOT IN (SELECT account_id FROM public.account)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid account_id in user_profile (% entries)', check_result;
    END IF;

    -- ===== audit_trail TABLE CHECKS =====
    -- Check: NULL audit_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.audit_trail WHERE audit_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in audit_trail.audit_id (% records)', check_result;
    END IF;

    -- Check: Duplicate audit_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT audit_id FROM public.audit_trail GROUP BY audit_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate audit_id in audit_trail (% duplicates)', check_result;
    END IF;

    -- Check: Referential integrity - account_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT account_id FROM public.audit_trail
        WHERE account_id NOT IN (SELECT account_id FROM public.account)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid account_id in audit_trail (% entries)', check_result;
    END IF;

    -- ===== record_status TABLE CHECKS =====
    -- Check: NULL cs_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM public.record_status WHERE cs_id IS NULL;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: NULLs in record_status.cs_id (% records)', check_result;
    END IF;

    -- Check: Duplicate cs_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT cs_id FROM public.record_status GROUP BY cs_id HAVING COUNT(*) > 1
    ) dup;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Duplicate cs_id in record_status (% duplicates)', check_result;
    END IF;


    -- Check: Referential integrity - status_id
    total_checks := total_checks + 1;
    SELECT COUNT(*) INTO check_result FROM (
        SELECT DISTINCT status_id FROM public.record_status
        WHERE status_id NOT IN (SELECT status_id FROM public.status)
    ) invalids;
    IF check_result = 0 THEN
        passed_checks := passed_checks + 1;
    ELSE
        failed_checks := failed_checks + 1;
        RAISE NOTICE 'FAIL: Invalid status_id in record_status (% entries)', check_result;
    END IF;

    -- Final summary
    RAISE NOTICE '--- PUBLIC SCHEMA DATA QUALITY SUMMARY ---';
    RAISE NOTICE 'Total Checks: %', total_checks;
    RAISE NOTICE 'Passed Checks: %', passed_checks;
    RAISE NOTICE 'Failed Checks: %', failed_checks;
    RAISE NOTICE 'Quality Score: %%%', ROUND((passed_checks::DECIMAL / total_checks) * 100, 2);
    RAISE NOTICE '--- END OF PUBLIC SCHEMA QUALITY CHECK PROCEDURE ---';
END;
$$;

-- Execute the quality check procedure
CALL quality_check_public_schema();
