-- WARNING:
-- Running this script will drop the 'DataWarehouse' database if it exists.
-- All data in the database will be permanently deleted. Proceed with caution.

-- Terminate all other connections to the 'datawarehouse' database
RAISE NOTICE 'Terminating connections to datawarehouse...';
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();

-- Drop the database
RAISE NOTICE 'Dropping database: datawarehouse (if exists)...';
DROP DATABASE IF EXISTS datawarehouse;

-- Create the new 'datawarehouse' database
RAISE NOTICE 'Creating new database: datawarehouse...';
CREATE DATABASE datawarehouse;

RAISE NOTICE 'Database created. Please connect to datawarehouse before proceeding.';


-- Note: You must connect to 'datawarehouse' now before running the schema creation part.
-- This must be done manually in pgAdmin.

-- After switching to the 'datawarehouse' database, run the following:

-- Set the time zone
SET TIME ZONE 'Asia/Manila';
RAISE NOTICE 'Time zone set to Asia/Manila';

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;