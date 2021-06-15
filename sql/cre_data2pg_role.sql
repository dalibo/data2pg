-- cre_data2pg_role.sql
-- This file belongs to data2pg, the framework that helps migrating data to PostgreSQL databases from various sources
-- This sql script creates the data2pg role on:
--   - the instance that will contain the data2pg management database
--   - the instances that contain target database to migrate
------ It must be executed by a superuser role

DO LANGUAGE plpgsql $$
BEGIN
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = 'data2pg';
-- Create the data2pg role if it doesn't exist yet
    IF NOT FOUND THEN
        CREATE ROLE data2pg;
    END IF;
-- Set its password
    ALTER ROLE data2pg LOGIN PASSWORD 'md5a511ab1156a0feba6acde5bbe60bd6e6';
-- Give GRANTs to the target databasese
    PERFORM 0 FROM pg_catalog.pg_database WHERE datname = 'data2pg';
    IF FOUND THEN
        GRANT ALL ON DATABASE fu_mig_data TO fu_mig_data;
    END IF;
END;
$$;

