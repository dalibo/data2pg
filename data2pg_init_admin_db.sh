#!/usr/bin/bash
# data2pg_init_admin_db.sh
# This shell script initializes the data2pg administration database

echo "=================================================="
echo "    Create the Data2Pg administration database    "
echo "=================================================="

# Environment variables
## My values
export DATA2PG_ROLE=data2pg_adm
export DATA2PG_PWD=secret
export DATA2PG_ADMIN_SCHEMA=data2pg0.6

## Default values
PGHOST_DEFAULT_VALUE=localhost
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGDATABASE_DEFAULT_VALUE=data2pg
DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE=data2pg
DATA2PG_ROLE_DEFAULT_VALUE=data2pg
DATA2PG_PWD_DEFAULT_VALUE=gp2atad
TARGET_DB_FILE_DEFAULT_VALUE=target_database.dat

if [ -z ${PGHOST+x} ];
then
  echo "Setting environment variable PGHOST to its default value: ${PGHOST_DEFAULT_VALUE}"
  export PGHOST=${PGHOST_DEFAULT_VALUE}
else
  echo "The environment variable PGHOST is already defined: ${PGHOST}"
fi

if [ -z ${PGPORT+x} ];
then
  echo "Setting environment variable PGPORT to its default value: ${PGPORT_DEFAULT_VALUE}"
  export PGPORT=${PGPORT_DEFAULT_VALUE}
else
  echo "The environment variable PGPORT is already defined: ${PGPORT}"
fi

if [ -z ${PGUSER+x} ];
then
  echo "Setting environment variable PGUSER to its default value: ${PGUSER_DEFAULT_VALUE}"
  export PGUSER=${PGUSER_DEFAULT_VALUE}
else
  echo "The environment variable PGUSER is already defined: ${PGUSER}"
fi

if [ -z ${PGDATABASE+x} ];
then
  echo "Setting environment variable PGDATABASE to its default value: ${PGDATABASE_DEFAULT_VALUE}"
  export PGDATABASE=${PGDATABASE_DEFAULT_VALUE}
else
  echo "The environment variable PGDATABASE is already defined: ${PGDATABASE}"
fi

if [ -z ${DATA2PG_ADMIN_SCHEMA+x} ];
then
  echo "Setting environment variable DATA2PG_ADMIN_SCHEMA to its default value: ${DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE}"
  export DATA2PG_ADMIN_SCHEMA=${DATA2PG_ADMIN_SCHEMA_DEFAULT_VALUE}
else
  echo "the environment variable DATA2PG_ADMIN_SCHEMA is already defined: ${DATA2PG_ADMIN_SCHEMA}"
fi

if [ -z ${DATA2PG_ROLE+x} ];
then
  echo "Setting environment variable DATA2PG_ROLE to its default value: ${DATA2PG_ROLE_DEFAULT_VALUE}"
  export DATA2PG_ROLE=${DATA2PG_ROLE_DEFAULT_VALUE}
else
  echo "The environment variable DATA2PG_ROLE is already defined: ${DATA2PG_ROLE}"
fi

if [ -z ${DATA2PG_PWD+x} ];
then
  echo "Setting environment variable DATA2PG_PWD to its default value: ${DATA2PG_PWD_DEFAULT_VALUE}"
  export DATA2PG_PWD=${DATA2PG_PWD_DEFAULT_VALUE}
else
  echo "The environment variable DATA2PG_PWD is already defined"
fi

if [ -z ${TARGET_DB_FILE+x} ];
then
  echo "Setting environment variable TARGET_DB_FILE to its default value: ${TARGET_DB_FILE_DEFAULT_VALUE}"
  export TARGET_DB_FILE=${TARGET_DB_FILE_DEFAULT_VALUE}
else
  echo "The environment variable TARGET_DB_FILE is already defined: ${TARGET_DB_FILE}"
fi

echo "Create the owner role"
echo "---------------------"

psql postgres -v pgdatabase=$PGDATABASE<<EOF
\set ON_ERROR_STOP ON

-- Create the owner role if needed and the database.
CREATE OR REPLACE FUNCTION public.create_database(p_dbname TEXT) RETURNS void LANGUAGE plpgsql AS
\$create_database\$
  BEGIN
-- Create the data2pg role
    IF NOT EXISTS
         (SELECT 0
            FROM pg_catalog.pg_roles
            WHERE rolname = 'data2pg'
         ) THEN
      CREATE ROLE data2pg NOLOGIN;
      COMMENT ON ROLE data2pg IS
        'Owner of the data2pg and/or data2pg_admin extensions.';
    END IF;
  END;
\$create_database\$;

SELECT public.create_database(:'pgdatabase');
DROP FUNCTION public.create_database;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => data2pg role successfuly created"
fi

echo "Create the database"
echo "-------------------"

psql template1 -c "DROP DATABASE IF EXISTS ${PGDATABASE}"
psql template1 -c "CREATE DATABASE ${PGDATABASE} OWNER data2pg"

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => administration database successfuly created"
fi

echo "Create the data2pg administration extension"
echo "-------------------------------------------"

psql -v admin_schema=$DATA2PG_ADMIN_SCHEMA<<EOF

\set ON_ERROR_STOP ON

CREATE OR REPLACE FUNCTION public.create_extension(p_schema TEXT) RETURNS void LANGUAGE plpgsql AS
\$create_extension\$

  BEGIN
-- Drop the extension and schema, if any.
    DROP EXTENSION IF EXISTS data2pg_admin;
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', p_schema);
-- Create the schema and extension.
    EXECUTE format('CREATE SCHEMA %I', p_schema);
    EXECUTE format('CREATE EXTENSION data2pg_admin SCHEMA %I', p_schema);
--
    RETURN;
  END;
\$create_extension\$;

SELECT public.create_extension(:'admin_schema');
DROP FUNCTION public.create_extension;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => data2pg_admin extension successfuly created"
fi

echo "Create the log on role to be used, if needed"
echo "--------------------------------------------"

psql -v data2pg_role=${DATA2PG_ROLE} -v data2pg_pwd=${DATA2PG_PWD}<<EOF
\set ON_ERROR_STOP ON

CREATE OR REPLACE FUNCTION public.create_role(p_role TEXT, p_pwd TEXT) RETURNS void LANGUAGE plpgsql AS
\$create_role\$
  BEGIN
-- If the connection role is data2pg, just set/reset its password (It has been created by the already executed "CREATE EXTENSION data2pg" statement).
    IF p_role = 'data2pg' THEN
      EXECUTE format('ALTER ROLE data2pg LOGIN PASSWORD %L', p_pwd);
    ELSE
-- Otherwise create the requested role if it does not already exist and grant it data2pg.
      PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = p_role;
      IF NOT FOUND THEN
        EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', p_role, p_pwd);
      END IF;
      EXECUTE format('GRANT data2pg TO %I', p_role);
    END IF;
--
    RETURN;
  END;
\$create_role\$;

SELECT public.create_role(:'data2pg_role', :'data2pg_pwd');
DROP FUNCTION public.create_role;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => The connection role is created"
fi

echo "Load the target databases list"
echo "------------------------------"

psql data2pg -U ${DATA2PG_ROLE} -c "\copy \"${DATA2PG_ADMIN_SCHEMA}\".target_database FROM $TARGET_DB_FILE CSV HEADER"

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => target databases successfuly loaded."
fi

echo ""
echo "The data2pg administration database is ready"
echo ""
