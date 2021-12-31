#!/usr/bin/bash
# data2pg_init_schema.sh
# This shell script initializes the data2pg schema in a target database.

echo "======================================================================================"
echo "Create the data2pg role on the instance and the schema on the target database"
echo "======================================================================================"

# Environment variables to setup
PGHOST_DEFAULT_VALUE=localhost
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGDATABASE_DEFAULT_VALUE=test_dest
DATA2PG_SCHEMA_DEFAULT_VALUE=data2pg

DATA2PG_SCHEMA='data2pg03'

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
  echo "  => Setting PGPORT to ${PGPORT_DEFAULT_VALUE}."
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

if [ -z ${PGDATABASE+x} ];
then
  echo "Environment variable PGDATABASE is not defined."
  echo "  => Setting PGDATABASE to ${PGDATABASE_DEFAULT_VALUE}."
  export PGDATABASE=${PGDATABASE_DEFAULT_VALUE}
else
  echo "Environment variable PGDATABASE is already defined to ${PGDATABASE}."
fi

if [ -z ${DATA2PG_SCHEMA+x} ];
then
  echo "Environment variable DATA2PG_SCHEMA is not defined."
  echo "  => Setting DATA2PG_SCHEMA to ${DATA2PG_SCHEMA_DEFAULT_VALUE}."
  export DATA2PG_SCHEMA=${DATA2PG_SCHEMA_DEFAULT_VALUE}
else
  echo "Environment variable DATA2PG_SCHEMA is already defined to ${DATA2PG_SCHEMA}."
fi

echo "Create the role, if needed"
echo "--------------------------"

psql postgres <<EOF
\set ON_ERROR_STOP ON

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

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => the data2pg role is created"
fi

echo "Create the data2pg extension in the $PGDATABASE database"
echo "--------------------------------------------------------"

psql -v data2pg_schema=${DATA2PG_SCHEMA}<<EOF
\set ON_ERROR_STOP ON

DROP EXTENSION IF EXISTS :data2pg_schema;
DROP SCHEMA IF EXISTS :data2pg_schema CASCADE;

CREATE SCHEMA :data2pg_schema;
CREATE EXTENSION data2pg SCHEMA :data2pg_schema;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => the data2pg extension is successfuly created"
fi
