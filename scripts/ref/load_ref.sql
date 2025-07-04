CREATE OR REPLACE PROCEDURE ref.load_ref(local_file_path TEXT)
LANGUAGE plpgsql
AS $BODY$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
BEGIN
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading ref Layer Data with UPSERT...';
    RAISE NOTICE '================================';

    -- Set session timezone to Asia/Manila
    SET TIME ZONE 'Asia/Manila';
    RAISE NOTICE 'Session time zone set to %', current_setting('TIMEZONE');

    RAISE NOTICE '>> Resetting company_main table...';
    

	INSERT INTO ref.company_main (company_id, company_name, parent_company_id, address) VALUES
	    ('PERC', 'PetroEnergy Resources Corp', NULL, NULL),
	    ('PGEC', 'PetroGreen Energy Corp', 'PERC', NULL),
	    ('PSC', 'PetroSolar Corp', 'PGEC', NULL),
	    ('PWEI', 'PetroWind Energy Inc.', 'PGEC', NULL),
	    ('MGI', 'Maibarara Geothermal Inc.', 'PGEC', NULL),
	    ('ESEC', 'EcoSolar Energy Corp', 'PGEC', NULL),
	    ('RGEC', 'Rizal Green Energy Corp', 'PGEC', NULL),
	    ('BEP_NL', 'Buhawind Energy Phillippines (Northern Luzon)', 'PGEC', NULL),
	    ('BEP_NM', 'Buhawind Energy Phillippines (Northern Mindoro)', 'PGEC', NULL),
	    ('BEP_EP', 'Buhawind Energy Phillippines (East Panay)', 'PGEC', NULL),
	    ('BGEC', 'Bugallon Green Energy Corp', 'PGEC', NULL),
	    ('SJGEC', 'San Jose Green Energy Corp', 'PGEC', NULL),
	    ('DGEC', 'Dagohoy Green Energy Corp', 'PGEC', NULL),
	    ('BKS', 'BKS Green Energy Corp', 'PGEC', NULL)
	ON CONFLICT (company_id) DO UPDATE
	SET company_name = EXCLUDED.company_name,
	    parent_company_id = EXCLUDED.parent_company_id,
	    address = EXCLUDED.address;


    -- ====================
    -- Upsert expenditure_type
    -- ====================
	RAISE NOTICE '>> Upserting expenditure_type table...';
	
	INSERT INTO ref.expenditure_type (type_id, type_description) VALUES
	    ('CS', 'Cost of Sales'),
	    ('GA', 'General and Administrative')
	ON CONFLICT (type_id) DO UPDATE
	SET
	    type_description = EXCLUDED.type_description;

    RAISE NOTICE '>> Upserting hr_positions table...';
    INSERT INTO ref.hr_position(position_id, position_name)
    VALUES 
    ('SM', 'Senior Management'),
    ('MM', 'Middle Management'),
    ('RF', 'Rank And File')
    ON CONFLICT (position_id) DO UPDATE
	SET
	    position_name = EXCLUDED.position_name;




    
    -- ====================
    -- Load ref_emission_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading ref_emission_factors with UPSERT...';

    DROP TABLE IF EXISTS ref_emission_factors;
    CREATE TEMP TABLE ref_emission_factors (
		ef_id TEXT,
        generation_source TEXT,
        kg_co2_per_kwh NUMERIC,
        co2_emitted_kg NUMERIC
    );

    EXECUTE format(
        'COPY ref_emission_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/ref_emission_factors.csv'
    );

    INSERT INTO ref.ref_emission_factors (
        ef_id, generation_source, kg_co2_per_kwh, co2_emitted_kg, create_at, updated_at
    )
    SELECT 
        ef_id,
        LOWER(generation_source),
        ROUND(kg_co2_per_kwh::DECIMAL, 4),
        ROUND(co2_emitted_kg::DECIMAL, 4),
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM ref_emission_factors
    WHERE generation_source IS NOT NULL
    ON CONFLICT (ef_id) DO UPDATE
    SET generation_source = EXCLUDED.generation_source,
        kg_co2_per_kwh = EXCLUDED.kg_co2_per_kwh,
        co2_emitted_kg = EXCLUDED.co2_emitted_kg,
        updated_at = EXCLUDED.updated_at;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load ref_power_plants
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading ref_power_plants with UPSERT...';

    DROP TABLE IF EXISTS ref_power_plants;
    CREATE TEMP TABLE ref_power_plants (
        power_plant_id TEXT,
        company_id TEXT,
        site_name TEXT,
        site_address TEXT,
        city_town TEXT,
        province TEXT,
        country TEXT,
        zip TEXT,
		ef_id TEXT
    );

    EXECUTE format(
        'COPY ref_power_plants FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/ref_power_plants.csv'
    );

    INSERT INTO ref.ref_power_plants (
		    power_plant_id,
		    company_id,
		    site_name,
		    site_address,
		    city_town,
		    province,
		    country,
		    zip,
		    ef_id,
		    create_at,
		    updated_at
		)
		SELECT 
		    pp.power_plant_id,
		    pp.company_id,
		    pp.site_name,
		    pp.site_address,
		    pp.city_town,
		    pp.province,
		    pp.country,
		    pp.zip,
		    ef_id,
		    NOW(),
		    NOW()
		FROM ref_power_plants pp
		LEFT JOIN ref.company_main com ON com.company_id = pp.company_id
		ON CONFLICT (power_plant_id) DO UPDATE
		SET 
		    company_id = EXCLUDED.company_id,
		    site_name = EXCLUDED.site_name,
		    site_address = EXCLUDED.site_address,
		    city_town = EXCLUDED.city_town,
		    province = EXCLUDED.province,
		    country = EXCLUDED.country,
		    zip = EXCLUDED.zip,
		    ef_id = EXCLUDED.ef_id,
		    updated_at = EXCLUDED.updated_at;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load ref_fa_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading ref_fa_factors with UPSERT...';

    DROP TABLE IF EXISTS ref_fa_factors;
    CREATE TEMP TABLE ref_fa_factors (
        ff_id TEXT,
        ff_name TEXT,
        ff_percentage NUMERIC,
        ff_category VARCHAR(20)
    );

    EXECUTE format(
        'COPY ref_fa_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/ref_fa_factors.csv'
    );

    INSERT INTO ref.ref_fa_factors (
			ff_id,
			ff_name,
			ff_percentage,
            ff_category,
			create_at,
			updated_at
		)
		SELECT 
			(ff_id)::VARCHAR(20),
			ff_name,
			(ff_percentage)::DECIMAL(5,4),
            ff_category,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		FROM ref_fa_factors
		ON CONFLICT (ff_id) DO UPDATE 
		SET 
			ff_name = EXCLUDED.ff_name,
			ff_percentage = EXCLUDED.ff_percentage,
            ff_category = EXCLUDED.ff_category,
			updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- ====================
    -- Load ref_hec_factors
    -- ====================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading ref_hec_factors with UPSERT...';

    DROP TABLE IF EXISTS ref_hec_factors;
    CREATE TEMP TABLE ref_hec_factors (
        hec_id TEXT,
        hec_value INT,
        hec_year INT,
        source_name TEXT,
        source_link TEXT
    );

    EXECUTE format(
        'COPY ref_hec_factors FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/ref_hec_factors.csv'
    );

    INSERT INTO ref.ref_hec_factors (
			hec_id,
			hec_value,
			hec_year,
			source_name,
			source_link,
			create_at,
			updated_at
		)
		SELECT 
			(hec_id)::VARCHAR(20),
			(hec_value)::DECIMAL(10,4),
			(hec_year)::INTEGER,
			source_name,
			source_link,
			CURRENT_TIMESTAMP,
			CURRENT_TIMESTAMP
		FROM ref_hec_factors
		ON CONFLICT (hec_id) DO UPDATE 
		SET 
			hec_value = EXCLUDED.hec_value,
			hec_year = EXCLUDED.hec_year,
			source_name = EXCLUDED.source_name,
			source_link = EXCLUDED.source_link,
			updated_at = CURRENT_TIMESTAMP;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    -- =============================
    -- Load ref_co2_equivalence
    -- =============================
    start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Loading ref_co2_equivalence with UPSERT...';

    DROP TABLE IF EXISTS ref_co2_equivalence_temp;
    CREATE TEMP TABLE ref_co2_equivalence_temp (
        equivalence_category TEXT,
        equivalence_label TEXT,
        metric VARCHAR(100),
        equivalent_value_co2_emissions DECIMAL(20, 10)
    );

    EXECUTE format(
        'COPY ref_co2_equivalence_temp FROM %L DELIMITER '','' CSV HEADER',
        local_file_path || '/ref_equivalence.csv'
    );

    INSERT INTO ref.ref_co2_equivalence (
        equivalence_category,
        equivalence_label,
        metric,
        equivalent_value_co2_emissions,
        create_at,
        updated_at
    )
    SELECT
        t.equivalence_category,
        t.equivalence_label,
        t.metric,
        t.equivalent_value_co2_emissions,
        NOW(),
        NOW()
    FROM ref_co2_equivalence_temp t
    ON CONFLICT (equivalence_label) DO UPDATE
    SET
        equivalence_category = EXCLUDED.equivalence_category,
        metric = EXCLUDED.metric,
        equivalent_value_co2_emissions = EXCLUDED.equivalent_value_co2_emissions,
        updated_at = EXCLUDED.updated_at;

    end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$BODY$;

