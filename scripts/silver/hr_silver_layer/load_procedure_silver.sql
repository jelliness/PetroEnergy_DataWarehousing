CREATE OR REPLACE PROCEDURE silver.load_hr_silver(
	load_demographics BOOLEAN DEFAULT TRUE,
    load_tenure BOOLEAN DEFAULT TRUE,
    load_parental_leave BOOLEAN DEFAULT TRUE,
    load_training BOOLEAN DEFAULT TRUE,
    load_safety_workdata BOOLEAN DEFAULT TRUE,
    load_occupational_safety_health BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
	null_count INT;
	
BEGIN
	IF load_demographics THEN
		batch_start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '================================';
		RAISE NOTICE 'Loading HR Silver Layer Data...';
		RAISE NOTICE '================================';

		-- hr_demographics
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Demographics Data...';
		RAISE NOTICE '------------------------------------------------';

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Inserting Data into silver.hr_demographics...';
	

		INSERT INTO silver.hr_demographics (
			employee_id,
			gender,
			birthdate,
			-- age,
			position_id,
			-- position_name,
			p_np,
			company_id,
			employment_status,
			date_updated
		)
		SELECT
			b.employee_id,
			b.gender,
			b.birthdate,
			-- DATE_PART('year', AGE(CURRENT_DATE, b.birthdate))::INT, -- derived age
			b.position_id,
			-- b.position_name,
			b.p_np,
			b.company_id,
			b.employment_status,
			CURRENT_TIMESTAMP
		FROM bronze.hr_demographics b
		ON CONFLICT (employee_id)
		DO UPDATE SET
			gender = EXCLUDED.gender,
			birthdate = EXCLUDED.birthdate,
			-- age = EXCLUDED.age,
			position_id = EXCLUDED.position_id,
			-- position_name = EXCLUDED.position_name,
			p_np = EXCLUDED.p_np,
			company_id = EXCLUDED.company_id,
			employment_status = EXCLUDED.employment_status,
			date_updated = CURRENT_TIMESTAMP;

		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;
-----------------------------------------------------------------------------------------------------

    -- hr_parental_leave
	IF load_parental_leave THEN
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Parental Leave Data...';
		RAISE NOTICE '------------------------------------------------';

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Inserting Data into silver.hr_parental_leave...';

		-- REMOVES DUPLICATE
		DELETE FROM silver.hr_parental_leave
		WHERE (employee_id, date) IN (
			SELECT employee_id, date FROM bronze.hr_parental_leave
		);


		INSERT INTO silver.hr_parental_leave (
			parental_leave_id,
			employee_id, 
			type_of_leave, 
			date, days, 
			end_date, 
			months_availed,
			date_updated
		)
		SELECT
			'PL' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || LPAD(ROW_NUMBER() OVER (ORDER BY employee_id, date)::TEXT, 3, '0') AS parental_leave_id,
			b.employee_id,
			b.type_of_leave,
			b.date,
			b.days,
			b.date + (b.days || ' days')::INTERVAL,
			FLOOR(b.days / 30),
			CURRENT_TIMESTAMP
		FROM bronze.hr_parental_leave b;


		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;
	
-----------------------------------------------------------------------------------------------------

    -- hr_tenure
	IF load_tenure THEN
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Tenure Data...';
		RAISE NOTICE '------------------------------------------------';

		DELETE FROM silver.hr_tenure
		WHERE employee_id IN (SELECT employee_id FROM bronze.hr_tenure);

		INSERT INTO silver.hr_tenure (
			employee_id, 
			start_date, 
			end_date, 
			-- is_active, 
			tenure_length,
			date_updated
		)
		SELECT
			b.employee_id,
			b.start_date,
			b.end_date,
			-- b.end_date IS NULL,
			ROUND(EXTRACT(DAY FROM COALESCE(b.end_date, CURRENT_DATE) - b.start_date) / 365.0, 2),
			CURRENT_TIMESTAMP
		FROM bronze.hr_tenure b;
		
		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Inserting Data into silver.hr_tenure...';

		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;

-----------------------------------------------------------------------------------------------------

    -- hr_training
	IF load_training THEN
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Training Data...';
		RAISE NOTICE '------------------------------------------------';

		RAISE NOTICE '>> Inserting data into silver.hr_training...';

		DELETE FROM silver.hr_training
		WHERE (company_id, training_title, date, training_hours) IN (
			SELECT company_id, training_title, date, training_hours FROM bronze.hr_training
		);

		INSERT INTO silver.hr_training (
			training_id,
			company_id,
			training_title,
			date,
			training_hours,
			number_of_participants,
			total_training_hours, -- derived column
			date_updated
		)
		SELECT
			'TR' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || LPAD(ROW_NUMBER() OVER (ORDER BY company_id, training_title, date)::TEXT, 3, '0') AS training_id,
			b.company_id,
			b.training_title,
			b.date,
			b.training_hours,
			b.number_of_participants,
			(b.training_hours * b.number_of_participants) AS total_training_hours, -- derived column
			CURRENT_TIMESTAMP
		FROM bronze.hr_training b;


		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;

-----------------------------------------------------------------------------------------------------

    -- hr_safety_workdata
	IF load_safety_workdata THEN
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Safety Workdata...';
		RAISE NOTICE '------------------------------------------------';

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Inserting data into silver.hr_safety_workdata...';

		DELETE FROM silver.hr_safety_workdata
		WHERE (company_id, contractor, date) IN (
			SELECT company_id, contractor, date FROM bronze.hr_safety_workdata
		);

		INSERT INTO silver.hr_safety_workdata (
			safety_workdata_id,
			company_id, 
			contractor,
			date, 
			manpower, 
			manhours,
			date_updated
		)
		SELECT
			'SWD' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || LPAD(ROW_NUMBER() OVER (ORDER BY company_id, contractor, date)::TEXT, 3, '0') AS safety_workdata_id,
			b.company_id, 
			b.contractor,
			b.date, 
			b.manpower, 
			b.manhours, 
			CURRENT_TIMESTAMP
		FROM bronze.hr_safety_workdata b;

		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;

-----------------------------------------------------------------------------------------------------

	-- hr_occupational_safety_health
	IF load_occupational_safety_health THEN
		RAISE NOTICE '------------------------------------------------';
		RAISE NOTICE 'Loading HR Occupational Safety Health data...';
		RAISE NOTICE '------------------------------------------------';

		start_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Inserting data into silver.hr_occupational_safety_health...';

		DELETE FROM silver.hr_occupational_safety_health
		WHERE (company_id, workforce_type, lost_time, date, incident_type, incident_title) IN (
			SELECT company_id, workforce_type, lost_time, date, incident_type, incident_title FROM bronze.hr_occupational_safety_health
		);

		INSERT INTO silver.hr_occupational_safety_health (
			osh_id,
			company_id,
			workforce_type,
			lost_time,
			date,
			incident_type,
			incident_title,
			incident_count,
			date_updated
		)
		SELECT
			'OSH' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD') || LPAD(ROW_NUMBER() OVER (ORDER BY company_id, workforce_type, lost_time, date, incident_type, incident_title)::TEXT, 3, '0') AS osh_id,
			b.company_id,
			b.workforce_type,
			b.lost_time,
			b.date,
			b.incident_type,
			b.incident_title,
			b.incident_count,
			CURRENT_TIMESTAMP
		FROM bronze.hr_occupational_safety_health b;

		end_time := CURRENT_TIMESTAMP;
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '-----------------';
	END IF;

-----------------------------------------------------------------------------------------------------

    batch_end_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '================================';
    RAISE NOTICE 'Loading HR silver Layer is Completed';
    RAISE NOTICE '     - Total Load Duration: % seconds', EXTRACT(EPOCH FROM batch_end_time - batch_start_time);
    RAISE NOTICE '================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '================================';
        RAISE NOTICE 'Error occurred while loading data: %', SQLERRM;
        RAISE NOTICE '================================';
END;
$$;