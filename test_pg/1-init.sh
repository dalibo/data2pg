#!/usr/bin/bash
# test_pg_init.sh
# This shell script initialize a test environment that will use 2 postgres databases and the postgres fdw
PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGHOST_DEFAULT_VALUE=localhost

if [ -z ${PGPORT+x} ];
then
  echo "Environment variable PGPORT is not defined."
  echo "Setting environment variable PGPORT to ${PGPORT_DEFAULT_VALUE}"
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

if [ -z ${PGHOST+x} ];
then
  echo "Environment variable PGHOST is not defined."
  echo "Setting environment variable PGHOST to ${PGHOST_DEFAULT_VALUE}"
  export PGHOST=${PGHOST_DEFAULT_VALUE}
else
  echo "Environment variable PGHOST is already defined to ${PGHOST}."
fi

echo "==================================================="
echo "Initialize the data2pg test with Postgres databases"
echo "==================================================="

echo "Create the role and both databases"
echo "----------------------------------"

psql postgres <<EOF

drop database if exists test_src;
drop database if exists test_dest;

drop role if exists data2pg;
create role data2pg login password 'xxx' superuser;		-- the role is only used for the migration project and will be dropped later
alter role data2pg set session_replication_role = 'replica';

create database test_src;
create database test_dest;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => Role and databases successfuly created"
fi

echo "Create the test_src schemas"
echo "---------------------------"

export PGDATABASE=test_src

psql <<EOF

SELECT current_database();

begin transaction;

\set ON_ERROR_STOP ON

\i test_pg/setup.sql

set search_path=myschema1;
\d

commit;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => Source database structure successfuly created"
fi

echo "Populate the test_src database"
echo "------------------------------"

psql <<EOF

SELECT current_database();

begin transaction;

\set ON_ERROR_STOP ON

\i test_pg/populate.sql

commit;

analyze;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => Source database successfuly populated"
fi

echo "Create the test_dest schemas"
echo "----------------------------"

export PGDATABASE=test_dest

psql <<EOF

SELECT current_database();

begin transaction;

\set ON_ERROR_STOP ON

\i test_pg/setup.sql

commit;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit
else
  echo "  => Destination database structure successfuly created"
fi
