#!/usr/bin/bash
# data2pg_init_extension.sh
# This shell script initializes the data2pg extension in a target database.

echo "=========================================================================================="
echo "Create the data2pg role on the instance and the data2pg extension into the target database"
echo "=========================================================================================="

# Environment variables
## My values
export PGHOST=localhost
export PGDATABASE=test_dest
export DATA2PG_ROLE=data2pg_adm
export DATA2PG_PWD=secret
export DATA2PG_SCHEMA=data2pg0.7

## Default values
PGHOST_DEFAULT_VALUE=
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGDATABASE_DEFAULT_VALUE=postgres
DATA2PG_SCHEMA_DEFAULT_VALUE=data2pg
DATA2PG_ROLE_DEFAULT_VALUE=data2pg
DATA2PG_PWD_DEFAULT_VALUE=gp2atad

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

if [ -z ${DATA2PG_SCHEMA+x} ];
then
  echo "Setting environment variable DATA2PG_SCHEMA to its default value: ${DATA2PG_SCHEMA_DEFAULT_VALUE}"
  export DATA2PG_SCHEMA=${DATA2PG_SCHEMA_DEFAULT_VALUE}
else
  echo "The environment variable DATA2PG_SCHEMA is already defined: ${DATA2PG_SCHEMA}"
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

echo "Create the data2pg extension in the $PGDATABASE database"
echo "--------------------------------------------------------"

psql -v data2pg_schema=${DATA2PG_SCHEMA}<<EOF
\set ON_ERROR_STOP ON

CREATE OR REPLACE FUNCTION public.create_extension(p_schema TEXT) RETURNS void LANGUAGE plpgsql AS
\$create_extension\$

  BEGIN
-- If the extension is already installed, drop the existing migrations, if any, to avoid to generate orphan srcdb_XXX schemas.
    BEGIN
      EXECUTE format('PERFORM %I.drop_migration(mgr_name) FROM migration',p_schema);
    EXCEPTION
      WHEN OTHERS THEN  -- continue
    END;
-- Drop the extension and schema, if any.
    DROP EXTENSION IF EXISTS data2pg CASCADE;
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', p_schema);
-- Create the schema and extension.
    EXECUTE format('CREATE SCHEMA %I', p_schema);
    EXECUTE format('CREATE EXTENSION data2pg SCHEMA %I CASCADE', p_schema);
--
    RETURN;
  END;
\$create_extension\$;

SELECT public.create_extension(:'data2pg_schema');
DROP FUNCTION public.create_extension;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => The data2pg extension is successfuly created"
fi

echo "Load the data2pg addons, if any"
echo "-------------------------------"

if [ -f data2pg_addons.sql ]; then
  psql -f data2pg_addons.sql
  if [ $? -ne 0 ]; then
    echo "  => Problem encountered"
    exit 1
  else
    echo "  => The data2pg addons have been successfuly loaded"
  fi
else
  echo "Warning: no data2pg_addons.sql file found."
fi

echo "Create the role to be used by the scheduler, if needed"
echo "------------------------------------------------------"

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
