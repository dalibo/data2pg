#!/usr/bin/bash
# data2pg_init_admin_db.sh
# This shell script initializes the data2pg administration database

echo "=================================================="
echo "    Create the Data2Pg administration database    "
echo "=================================================="

# Environment variables to setup
PGHOST_DEFAULT_VALUE=localhost
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE=data2pg
TARGET_DB_FILE_DEFAULT_VALUE=target_database.dat

DATA2PG_ADMIN_SCHEMA='data2pg03'

if [ -z ${PGHOST+x} ];
then
  echo "Environment variable PGHOST is not defined."
  echo "  => Setting PGHOST to ${PGHOST_DEFAULT_VALUE}"
  export PGHOST=${PGHOST_DEFAULT_VALUE}
else
  echo "Environment variable PGHOST is already defined to ${PGHOST}."
fi

if [ -z ${PGPORT+x} ];
then
  echo "Environment variable PGPORT is not defined."
  echo "  => Setting PGPORT to ${PGPORT_DEFAULT_VALUE}"
  export PGPORT=${PGPORT_DEFAULT_VALUE}
else
  echo "Environment variable PGPORT is already defined to ${PGPORT}."
fi

if [ -z ${PGUSER+x} ];
then
  echo "Environment variable PGUSER is not defined."
  echo "  => Setting PGUSER to ${PGUSER_DEFAULT_VALUE}."
  export PGUSER=${PGUSER_DEFAULT_VALUE}
else
  echo "Environment variable PGUSER is already defined to ${PGUSER}."
fi

if [ -z ${DATA2PG_ADMIN_SCHEMA+x} ];
then
  echo "Environment variable DATA2PG_ADMIN_SCHEMA is not defined."
  echo "  => Setting DATA2PG_ADMIN_SCHEMA to ${DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE}."
  export DATA2PG_ADMIN_SCHEMA=${DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE}
else
  echo "Environment variable DATA2PG_ADMIN_SCHEMA is already defined to ${DATA2PG_ADMIN_SCHEMA}."
fi

if [ -z ${TARGET_DB_FILE+x} ];
then
  echo "Environment variable TARGET_DB_FILE is not defined."
  echo "  => Setting TARGET_DB_FILE to ${TARGET_DB_FILE_DEFAULT_VALUE}."
  export TARGET_DB_FILE=${TARGET_DB_FILE_DEFAULT_VALUE}
else
  echo "Environment variable TARGET_DB_FILE is already defined to ${TARGET_DB_FILE}."
fi

echo "Create the role and the database"
echo "--------------------------------"

psql<<EOF
\set ON_ERROR_STOP ON

-- Perform some checks and create the role if needed
DO LANGUAGE plpgsql
\$do\$
  BEGIN
-- create the data2pg role, if it doesn't already exist in the instance
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
  exit 1
else
  echo "  => data2pg role and database successfuly created"
fi

echo "Create the data2pg administration extension"
echo "-------------------------------------------"

psql data2pg -U data2pg -v admin_schema=$DATA2PG_ADMIN_SCHEMA<<EOF
\set ON_ERROR_STOP ON

DROP EXTENSION IF EXISTS data2pg_admin;
DROP SCHEMA IF EXISTS :admin_schema CASCADE;

CREATE SCHEMA :admin_schema;
CREATE EXTENSION data2pg_admin SCHEMA :admin_schema;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => data2pg_admin extension successfuly created"
fi

echo "Load the target databases list"
echo "------------------------------"

psql data2pg -U data2pg -c "\copy ${DATA2PG_ADMIN_SCHEMA}.target_database FROM $TARGET_DB_FILE CSV HEADER"

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => target databases successfuly loaded."
fi
echo ""
echo "The data2pg administration database is ready"
echo ""
