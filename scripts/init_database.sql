-- WARNING:
-- Running this script will drop the 'DataWarehouse' database if it exists.
-- All data in the database will be permanently deleted. Proceed with caution.

-- Terminate all connections and drop the database if it exists
DO
$$
BEGIN
    IF EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse') THEN
        -- Terminate all connections to the database
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();

        -- Drop the database
        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END
$$;
-- Create the new 'datawarehouse' database
CREATE DATABASE datawarehouse;

-- Note: You must connect to 'datawarehouse' now before running the schema creation part.
-- This must be done manually in pgAdmin or your SQL client.

-- After switching to the 'datawarehouse' database, run the following:

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;