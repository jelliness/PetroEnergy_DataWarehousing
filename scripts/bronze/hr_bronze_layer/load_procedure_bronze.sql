CREATE OR REPLACE PROCEDURE ref.load_company_main()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Creating company_main table...';
    RAISE NOTICE '================================';

    -- Drop existing table if exists
    RAISE NOTICE '>> Dropping existing table: ref.company_main (if exists)...';
    EXECUTE 'DROP TABLE IF EXISTS ref.company_main CASCADE';

    -- Create new table
    RAISE NOTICE '>> Creating new table: ref.company_main...';
    EXECUTE '
        CREATE TABLE ref.company_main (
            company_id VARCHAR(20) PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL,
            parent_company_id VARCHAR(20) REFERENCES ref.company_main(company_id) ON DELETE SET NULL,
            address TEXT
        )
    ';

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Table created successfully.';
    RAISE NOTICE '>> Operation Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
    RAISE NOTICE '================================';
    RAISE NOTICE 'company_main table creation completed.';
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred during table creation: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;