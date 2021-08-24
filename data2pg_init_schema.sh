#!/usr/bin/bash
# data2pg_init_schema.sh
# This shell script initializes the data2pg schema in a target database.

# The 4 following constants must be adjusted before execution.
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGDATABASE_DEFAULT_VALUE=test_dest
PGHOST_DEFAULT_VALUE=localhost

if [ -z ${PGPORT+x} ];
then
  echo "Environment variable PGPORT is not defined."
  echo "Setting environment variable PGPORT to ${PGPORT_DEFAULT_VALUE}."
  export PGPORT=${PGPORT_DEFAULT_VALUE}
else
  echo "Environment variable PGPORT is already defined to ${PGPORT}."
fi

if [ -z ${PGUSER+x} ];
then
  echo "Environment variable PGUSER is not defined."
  echo "Setting environment variable PGUSER to ${PGUSER_DEFAULT_VALUE}."
  export PGUSER=${PGUSER_DEFAULT_VALUE}
else
  echo "Environment variable PGUSER is already defined to ${PGUSER}."
fi

if [ -z ${PGDATABASE+x} ];
then
  echo "Environment variable PGDATABASE is not defined."
  echo "Setting environment variable PGDATABASE to ${PGDATABASE_DEFAULT_VALUE}."
  export PGDATABASE=${PGDATABASE_DEFAULT_VALUE}
else
  echo "Environment variable PGDATABASE is already defined to ${PGDATABASE}."
fi

if [ -z ${PGHOST+x} ];
then
  echo "Environment variable PGHOST is not defined."
  echo "Setting environment variable PGHOST to ${PGHOST_DEFAULT_VALUE}"
  export PGHOST=${PGHOST_DEFAULT_VALUE}
else
  echo "Environment variable PGHOST is already defined to ${PGHOST}."
fi

echo "======================================================================================"
echo "Create the data2pg role on the instance and the database schema on the target database"
echo "======================================================================================"

echo "Create the role, if needed"
echo "--------------------------"

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

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => the data2pg role is created"
fi

echo "Create the data2pg schema in the $PGDATABASE database"
echo "-----------------------------------------------------"

psql $PGDATABASE -f sql/data2pg_init_schema.sql

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => data2pg schema successfuly created"
fi
