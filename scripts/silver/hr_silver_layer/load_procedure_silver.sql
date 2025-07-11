CREATE OR REPLACE PROCEDURE silver.load_hr_silver(IN load_demographics boolean DEFAULT true, IN load_tenure boolean DEFAULT true, IN load_parental_leave boolean DEFAULT true, IN load_training boolean DEFAULT true, IN load_safety_workdata boolean DEFAULT true, IN load_occupational_safety_health boolean DEFAULT true, IN load_from_sql boolean DEFAULT true)
 LANGUAGE plpgsql
AS $procedure$
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
			position_id,
			p_np,
			company_id,
			employment_status,
			date_updated
		)
		SELECT
			b.employee_id,
			b.gender,
			b.birthdate,
			b.position_id,
			b.p_np,
			b.company_id,
			b.employment_status,
			CURRENT_TIMESTAMP
		FROM bronze.hr_demographics b
		ON CONFLICT (employee_id)
		DO UPDATE SET
			gender = EXCLUDED.gender,
			birthdate = EXCLUDED.birthdate,
			position_id = EXCLUDED.position_id,
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

		IF load_from_sql THEN
			DELETE FROM silver.hr_parental_leave
			WHERE employee_id IN (SELECT employee_id FROM bronze.hr_parental_leave);
		END IF;

		-- Declare and compute latest sequence
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
			b.parental_leave_id,
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

		IF load_from_sql THEN
			DELETE FROM silver.hr_tenure
			WHERE employee_id IN (SELECT employee_id FROM bronze.hr_tenure);
		END IF;

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

		IF load_from_sql THEN
			DELETE FROM silver.hr_training
			WHERE (company_id, training_title, date, training_hours) IN (
				SELECT company_id, training_title, date, training_hours FROM bronze.hr_training
			);
		END IF;

		INSERT INTO silver.hr_training (
			training_id,
			company_id,
			training_title,
			date,
			training_hours,
			number_of_participants,
			total_training_hours,
			date_updated
		)
		SELECT
			b.training_id,
			b.company_id,
			b.training_title,
			b.date,
			b.training_hours,
			b.number_of_participants,
			b.training_hours * b.number_of_participants,
			CURRENT_TIMESTAMP
		FROM bronze.hr_training b
ON CONFLICT (training_id)
DO UPDATE SET
  company_id = EXCLUDED.company_id,
  training_title = EXCLUDED.training_title,
  date = EXCLUDED.date,
  training_hours = EXCLUDED.training_hours,
  number_of_participants = EXCLUDED.number_of_participants,
  total_training_hours = EXCLUDED.total_training_hours,
  date_updated = CURRENT_TIMESTAMP;

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
			b.safety_workdata_id,
			b.company_id, 
			b.contractor,
			b.date, 
			b.manpower, 
			b.manhours, 
			CURRENT_TIMESTAMP
		FROM bronze.hr_safety_workdata b
  ON CONFLICT (safety_workdata_id)
  DO UPDATE SET
    company_id = EXCLUDED.company_id,
    contractor = EXCLUDED.contractor,
    date = EXCLUDED.date,
    manpower = EXCLUDED.manpower,
    manhours = EXCLUDED.manhours,
    date_updated = CURRENT_TIMESTAMP;

		
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


		IF load_from_sql THEN
			DELETE FROM silver.hr_occupational_safety_health
			WHERE (company_id, workforce_type, lost_time, date, incident_type, incident_title) IN (
				SELECT company_id, workforce_type, lost_time, date, incident_type, incident_title FROM bronze.hr_occupational_safety_health
			);
		END IF;

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
			b.osh_id,
			b.company_id,
			b.workforce_type,
			b.lost_time,
			b.date,
			b.incident_type,
			b.incident_title,
			b.incident_count,
			CURRENT_TIMESTAMP
		FROM bronze.hr_occupational_safety_health b
ON CONFLICT (osh_id)
DO UPDATE SET
  company_id = EXCLUDED.company_id,
  workforce_type = EXCLUDED.workforce_type,
  lost_time = EXCLUDED.lost_time,
  date = EXCLUDED.date,
  incident_type = EXCLUDED.incident_type,
  incident_title = EXCLUDED.incident_title,
  incident_count = EXCLUDED.incident_count,
  date_updated = CURRENT_TIMESTAMP;

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
$procedure$