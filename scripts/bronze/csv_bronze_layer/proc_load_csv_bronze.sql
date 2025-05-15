CREATE OR REPLACE PROCEDURE bronze.load_csv_bronze(local_file_path TEXT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
BEGIN
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Bronze Layer Data with UPSERT...';
    RAISE NOTICE '================================';

    -- Set session timezone to Asia/Manila
    SET TIME ZONE 'Asia/Manila';
    RAISE NOTICE 'Session time zone set to %', current_setting('TIMEZONE');

    -- ====================
    -- Load csv_company
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_company with UPSERT...';

    DROP TABLE IF EXISTS csv_company;
    CREATE TEMP TABLE csv_company (
        company_id TEXT,
        company_name TEXT,
        resources TEXT
    );

    EXECUTE format(
        'COPY csv_company FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_company.csv'
    );

    INSERT INTO bronze.csv_company (company_id, company_name, resources)
    SELECT company_id, company_name, resources FROM csv_company
    ON CONFLICT (company_id) DO UPDATE
    SET
        company_name = EXCLUDED.company_name,
        resources = EXCLUDED.resources,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_emission_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_emission_factors with UPSERT...';

    DROP TABLE IF EXISTS csv_emission_factors;
    CREATE TEMP TABLE csv_emission_factors (
        generation_source TEXT,
        kg_co2_per_kwh TEXT
    );

    EXECUTE format(
        'COPY csv_emission_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_emission_factors.csv'
    );

    INSERT INTO bronze.csv_emission_factors (generation_source, kg_co2_per_kwh)
    SELECT generation_source, kg_co2_per_kwh FROM csv_emission_factors
    ON CONFLICT (generation_source) DO UPDATE
    SET
        kg_co2_per_kwh = EXCLUDED.kg_co2_per_kwh,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_power_plants
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_power_plants with UPSERT...';

    DROP TABLE IF EXISTS csv_power_plants;
    CREATE TEMP TABLE csv_power_plants (
        power_plant_id TEXT,
        company_id TEXT,
        site_name TEXT,
        site_address TEXT,
        city_town TEXT,
        province TEXT,
        country TEXT,
        zip TEXT
    );

    EXECUTE format(
        'COPY csv_power_plants FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_power_plants.csv'
    );

    INSERT INTO bronze.csv_power_plants (
        power_plant_id, company_id, site_name, site_address,
        city_town, province, country, zip
    )
    SELECT power_plant_id, company_id, site_name, site_address,
           city_town, province, country, zip
    FROM csv_power_plants
    ON CONFLICT (power_plant_id) DO UPDATE
    SET
        company_id = EXCLUDED.company_id,
        site_name = EXCLUDED.site_name,
        site_address = EXCLUDED.site_address,
        city_town = EXCLUDED.city_town,
        province = EXCLUDED.province,
        country = EXCLUDED.country,
        zip = EXCLUDED.zip,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_energy_records
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_energy_records with UPSERT...';

    DROP TABLE IF EXISTS csv_energy_records;
    CREATE TEMP TABLE csv_energy_records (
        power_plant_id TEXT,
        datetime TEXT,
        energy_generated TEXT,
        unit_of_measurement TEXT
    );

    EXECUTE format(
        'COPY csv_energy_records FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_energy_records.csv'
    );

    INSERT INTO bronze.csv_energy_records (
        power_plant_id, datetime, energy_generated, unit_of_measurement
    )
    SELECT power_plant_id, datetime, energy_generated, unit_of_measurement
    FROM csv_energy_records
    ON CONFLICT (power_plant_id, datetime) DO UPDATE
    SET
        energy_generated = EXCLUDED.energy_generated,
        unit_of_measurement = EXCLUDED.unit_of_measurement,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_fa_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_fa_factors with UPSERT...';

    DROP TABLE IF EXISTS csv_fa_factors;
    CREATE TEMP TABLE csv_fa_factors (
        ff_id TEXT,
        ff_name TEXT,
        ff_percentage TEXT
    );

    EXECUTE format(
        'COPY csv_fa_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_fa_factors.csv'
    );

    INSERT INTO bronze.csv_fa_factors (ff_id, ff_name, ff_percentage)
    SELECT ff_id, ff_name, ff_percentage FROM csv_fa_factors
    ON CONFLICT (ff_id) DO UPDATE
    SET
        ff_name = EXCLUDED.ff_name,
        ff_percentage = EXCLUDED.ff_percentage,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_hec_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_hec_factors with UPSERT...';

    DROP TABLE IF EXISTS csv_hec_factors;
    CREATE TEMP TABLE csv_hec_factors (
        hec_id TEXT,
        hec_value TEXT,
        hec_year TEXT,
        source_name TEXT,
        link TEXT
    );

    EXECUTE format(
        'COPY csv_hec_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_hec_factors.csv'
    );

    INSERT INTO bronze.csv_hec_factors (
        hec_id, hec_value, hec_year, source_name, source_link
    )
    SELECT hec_id, hec_value, hec_year, source_name, source_link FROM csv_hec_factors
    ON CONFLICT (hec_id) DO UPDATE
    SET
        hec_value = EXCLUDED.hec_value,
        hec_year = EXCLUDED.hec_year,
        source_name = EXCLUDED.source_name,
        source_link = EXCLUDED.source_link,
        updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$BODY$;

ALTER PROCEDURE bronze.load_csv_bronze(TEXT)
OWNER TO postgres;