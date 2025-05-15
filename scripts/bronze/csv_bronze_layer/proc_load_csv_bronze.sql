CREATE OR REPLACE PROCEDURE bronze.load_csv_bronze(local_file_path TEXT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading Bronze Layer Data with UPSERT...';
    RAISE NOTICE '================================';

    -- ====================
    -- Load csv_company
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_company with UPSERT...';

    DROP TABLE IF EXISTS temp.csv_company;
    CREATE TEMP TABLE temp.csv_company (
        company_id TEXT,
        company_name TEXT,
        resources TEXT
    );

    EXECUTE format(
        'COPY temp.csv_company FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_company.csv'
    );

    INSERT INTO bronze.csv_company (company_id, company_name, resources)
    SELECT company_id, company_name, resources FROM temp.csv_company
    ON CONFLICT (company_id) DO UPDATE
    SET
        company_name = EXCLUDED.company_name,
        resources = EXCLUDED.resources,
        dwh_date_updated = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_emission_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_emission_factors with UPSERT...';

    DROP TABLE IF EXISTS temp.csv_emission_factors;
    CREATE TEMP TABLE temp.csv_emission_factors (
        generation_source TEXT,
        kg_co2_per_kwh TEXT
    );

    EXECUTE format(
        'COPY temp.csv_emission_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_emission_factors.csv'
    );

    INSERT INTO bronze.csv_emission_factors (generation_source, kg_co2_per_kwh)
    SELECT generation_source, kg_co2_per_kwh FROM temp.csv_emission_factors
    ON CONFLICT (generation_source) DO UPDATE
    SET
        kg_co2_per_kwh = EXCLUDED.kg_co2_per_kwh,
        dwh_date_updated = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_power_plants
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_power_plants with UPSERT...';

    DROP TABLE IF EXISTS temp.csv_power_plants;
    CREATE TEMP TABLE temp.csv_power_plants (
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
        'COPY temp.csv_power_plants FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_power_plants.csv'
    );

    INSERT INTO bronze.csv_power_plants (
        power_plant_id, company_id, site_name, site_address,
        city_town, province, country, zip
    )
    SELECT power_plant_id, company_id, site_name, site_address,
           city_town, province, country, zip
    FROM temp.csv_power_plants
    ON CONFLICT (power_plant_id) DO UPDATE
    SET
        company_id = EXCLUDED.company_id,
        site_name = EXCLUDED.site_name,
        site_address = EXCLUDED.site_address,
        city_town = EXCLUDED.city_town,
        province = EXCLUDED.province,
        country = EXCLUDED.country,
        zip = EXCLUDED.zip,
        dwh_date_updated = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load csv_energy_records
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading csv_energy_records with UPSERT...';

    DROP TABLE IF EXISTS temp.csv_energy_records;
    CREATE TEMP TABLE temp.csv_energy_records (
        power_plant_id TEXT,
        datetime TEXT,
        energy_generated TEXT,
        unit_of_measurement TEXT
    );

    EXECUTE format(
        'COPY temp.csv_energy_records FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/csv_energy_records.csv'
    );

    INSERT INTO bronze.csv_energy_records (
        power_plant_id, datetime, energy_generated, unit_of_measurement
    )
    SELECT power_plant_id, datetime, energy_generated, unit_of_measurement
    FROM temp.csv_energy_records
    ON CONFLICT (power_plant_id, datetime) DO UPDATE
    SET
        energy_generated = EXCLUDED.energy_generated,
        unit_of_measurement = EXCLUDED.unit_of_measurement,
        dwh_date_updated = CURRENT_TIMESTAMP;

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
