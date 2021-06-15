#!/usr/bin/bash
# data2pg_init_db.sh
# This shell script initializes the data2pg management database

# Environment variables to setup
export PGHOST=localhost
export PGPORT=5432
export PGUSER=postgres
export TARGET_DB_FILE=target_database.dat

echo "============================================================="
echo "Create the data2pg role if not exits and the data2pg database"
echo "============================================================="

echo "Create the role and the database"
echo "--------------------------------"

psql postgres <<EOF

-- Perform some checks and create the role if needed
DO LANGUAGE plpgsql
\$do\$
  BEGIN
-- check the current role is a superuser
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'The current user (%) is not a superuser.', current_user;
    END IF;
-- check postgres version is >= 9.6
    IF current_setting('server_version_num')::INT < 90600 THEN
      RAISE EXCEPTION 'The current postgres version (%) is too old. It should be at least 9.6.',
        current_setting('server_version');
    END IF;
-- create the data2pg role, if it doesn't already exist
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = 'data2pg';
    IF NOT FOUND THEN
-- the role does not exist, so create it
      CREATE ROLE data2pg LOGIN PASSWORD 'md5a511ab1156a0feba6acde5bbe60bd6e6';
    END IF;
--
    RETURN;
  END;
\$do\$;

-- Create or recreate the data2pg database
DROP DATABASE IF EXISTS data2pg;
CREATE DATABASE data2pg OWNER data2pg;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => data2pg role and database successfuly created"
fi

echo "Populate the data2pg database"
echo "-----------------------------"

psql data2pg -U data2pg -f sql/data2pg_init_db.sql

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => data2pg database structure successfuly populated"
fi

echo "Load the target databases list"
echo "------------------------------"

psql data2pg -U data2pg -c "\copy data2pg.target_database FROM $TARGET_DB_FILE CSV HEADER"

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => target databases successfuly loaded. You can reload it on the data2pg database at any time, by executing the psql commands:"
  echo "      truncate target_database;"
  echo "      \copy data2pg.target_database FROM $TARGET_DB_FILE CSV HEADER"
fi
echo ""
echo "The data2pg database is ready"
echo ""
