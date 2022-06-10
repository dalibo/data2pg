#!/usr/bin/bash
# test_pg_init.sh
# This shell script initialize a test environment that will use 2 postgres databases and the postgres fdw
echo "==================================================="
echo "Initialize the data2pg test with Postgres databases"
echo "==================================================="

PGPORT_DEFAULT_VALUE=5432
PGUSER_DEFAULT_VALUE=postgres
PGHOST_DEFAULT_VALUE=localhost

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

echo "Create the role and both databases"
echo "----------------------------------"

psql <<EOF

\set ON_ERROR_STOP ON

DROP DATABASE IF EXISTS test_src;
DROP DATABASE IF EXISTS test_dest;

CREATE DATABASE test_src;
CREATE DATABASE test_dest;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => Role and databases successfuly created"
fi

echo "Create the test_src schemas"
echo "---------------------------"

export PGDATABASE=test_src

psql <<EOF

\set ON_ERROR_STOP ON

SELECT current_database();

BEGIN TRANSACTION;

\i test_pg/setup.sql

SET search_path=myschema1;
\d

COMMIT;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => Source database structure successfuly created"
fi

echo "Populate the test_src database"
echo "------------------------------"

psql <<EOF

\set ON_ERROR_STOP ON

SELECT current_database();

BEGIN TRANSACTION;

\i test_pg/populate.sql

COMMIT;

ANALYZE;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => Source database successfuly populated"
fi

echo "Create the test_dest schemas"
echo "----------------------------"

export PGDATABASE=test_dest

psql <<EOF

\set ON_ERROR_STOP ON

SELECT current_database();

BEGIN TRANSACTION;

\i test_pg/setup.sql

-- Rename a table and a sequence
ALTER TABLE myschema1.mytbl4 RENAME TO "MYTBL4";
ALTER SEQUENCE myschema2.mySeq2 RENAME TO "MYSEQ2";

COMMIT;
EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => Destination database structure successfuly created"
fi
