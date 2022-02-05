#!/usr/bin/bash
# 3-configure.sh
# This shell script prepares the data2pg components on the destination database

echo "==================================================="
echo "Prepare the data2pg test with Postgres databases"
echo "==================================================="

# Environment variables to setup
PGHOST_DEFAULT_VALUE=localhost
PGPORT_DEFAULT_VALUE=5432
PGDATABASE_DEFAULT_VALUE=test_dest

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

if [ -z ${PGDATABASE+x} ];
then
  echo "Environment variable PGDATABASE is not defined."
  echo "  => Setting PGDATABASE to ${PGDATABASE_DEFAULT_VALUE}."
  export PGDATABASE=${PGDATABASE_DEFAULT_VALUE}
else
  echo "Environment variable PGPORT is already defined to ${PGDATABASE}."
fi

psql -U data2pg -a<<EOF

\set ON_ERROR_STOP ON

-- set the search_path to the data2pg extension installation schema
SELECT set_config('search_path', nspname, false)
    FROM pg_extension JOIN pg_namespace ON (pg_namespace.oid = extnamespace)
    WHERE extname = 'data2pg';

BEGIN TRANSACTION;

--
-- Create the migration object and the FDW infrastructure
--

SELECT drop_migration('mig_all');
SELECT drop_migration('PG''s db');

select * from migration;
select * from batch;
\des
\dn

SELECT create_migration(
    p_migration            => 'PG''s db',
    p_sourceDbms           => 'PostgreSQL',
    p_extension            => 'postgres_fdw',
    p_serverOptions        => 'port ''5432'', dbname ''test_src'', fetch_size ''1000''',
    p_userMappingOptions   => 'user ''postgres'', password ''postgres''',
    p_importSchemaOptions  => 'import_default ''false'''
);


--
-- Register the tables and sequences
--

SELECT register_tables('PG''s db', 'myschema1', '.*', NULL);
SELECT register_tables('PG''s db', 'myschema2', '.*', NULL);
SELECT register_tables('PG''s db', 'phil''s schema3', '.*', NULL,
       p_ForeignTableOptions => 'OPTIONS(updatable ''false'')', p_createForeignTable => true);

SELECT register_sequences('PG''s db', 'myschema1', '.*', NULL);
SELECT register_sequences('PG''s db', 'myschema2', '.*', NULL);
SELECT register_sequences('PG''s db', 'phil''s schema3', '.*', NULL);

--
-- Register the columns transformation rules
--

SELECT register_column_transform_rule('myschema1','mytbl1','col11','col11_renamed');
SELECT register_column_transform_rule('myschema1','mytbl1','col11','col11');
SELECT register_column_transform_rule('myschema1','mytbl1','col13','substr(col13, 1, 10)');

--
-- Register the columns comparison rules
--

SELECT register_column_comparison_rule('myschema1','mytbl1','col11','col11');
SELECT register_column_comparison_rule('myschema1','myTbl3','col32',NULL);
SELECT register_column_comparison_rule('myschema1','myTbl3','col33','trunc(col33,1)','trunc(col33,0)');

--
-- Register the table parts
--

SELECT register_table_part('myschema2', 'mytbl1', 1, 'col11 < 50000', TRUE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', 2, 'col11 >= 50000 and col12 = ''ABC''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', 3, 'col11 >= 50000 and col12 = ''DEF''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', 4, 'col11 >= 50000 and col12 = ''GHI''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', 5, NULL, FALSE, TRUE);

--
-- Build the batches
--
SELECT drop_batch('BATCH0');
SELECT drop_batch('BATCH1');
SELECT drop_batch('COMPARE_ALL');
SELECT drop_batch('DISCOVER_ALL');

SELECT create_batch('BATCH0','PG''s db','COPY',true);
SELECT create_batch('BATCH1','PG''s db','COPY',false);
SELECT create_batch('COMPARE_ALL','PG''s db','COMPARE',null);
--SELECT create_batch('DISCOVER_ALL','PG''s db','DISCOVER',null);

--
-- Assign the tables and sequences to batches
--

SELECT assign_tables_to_batch('BATCH1', 'myschema1', '.*', NULL);
--select assign_tables_to_batch('BATCH1', 'myschema1', '.*', '^mytbl2b$');
SELECT assign_tables_to_batch('BATCH1', 'myschema2', '.*', '^mytbl1$');
SELECT assign_tables_to_batch('BATCH1', 'phil''s schema3', '.*', NULL);
--select assign_tables_to_batch('BATCH1', 'myschema4', '.*', NULL);

SELECT assign_sequences_to_batch('BATCH1', 'myschema1', '.*', NULL);
SELECT assign_sequences_to_batch('BATCH1', 'myschema2', '.*', NULL);
SELECT assign_sequences_to_batch('BATCH1', 'phil''s schema3', '.*', NULL);
--select assign_sequences_to_batch('BATCH1', 'myschema4', '.*', NULL);

SELECT assign_tables_to_batch('COMPARE_ALL', 'myschema1', '.*', NULL);
SELECT assign_tables_to_batch('COMPARE_ALL', 'myschema2', '.*', '^(mytbl1|mytbl5|mytbl6)$');  -- JSON or POINT types cannot be compared
SELECT assign_tables_to_batch('COMPARE_ALL', 'phil''s schema3', '.*', NULL);

SELECT assign_sequences_to_batch('COMPARE_ALL', 'myschema1', '.*', NULL);
SELECT assign_sequences_to_batch('COMPARE_ALL', 'myschema2', '.*', NULL);
SELECT assign_sequences_to_batch('COMPARE_ALL', 'phil''s schema3', '.*', NULL);

--SELECT assign_tables_to_batch('DISCOVER_ALL', 'myschema1', '.*', NULL);
--SELECT assign_tables_to_batch('DISCOVER_ALL', 'myschema2', '.*', NULL);
--SELECT assign_tables_to_batch('DISCOVER_ALL', 'phil''s schema3', '.*', NULL);

--
-- assign the table parts to batches
--

SELECT assign_table_part_to_batch('BATCH0', 'myschema2', 'mytbl1', 1);
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 2);
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 3);
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 4);
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 5);

SELECT assign_table_part_to_batch('COMPARE_ALL', 'myschema2', 'mytbl1', 1);
SELECT assign_table_part_to_batch('COMPARE_ALL', 'myschema2', 'mytbl1', 2);
SELECT assign_table_part_to_batch('COMPARE_ALL', 'myschema2', 'mytbl1', 3);
SELECT assign_table_part_to_batch('COMPARE_ALL', 'myschema2', 'mytbl1', 4);

--
-- Assign FK checks
--

SELECT assign_fkey_checks_to_batch('BATCH1', 'myschema2', 'mytbl1');
SELECT assign_fkey_checks_to_batch('BATCH1', 'myschema1', 'mytbl4');
SELECT assign_fkey_checks_to_batch('BATCH1', 'myschema2', 'mytbl4', 'mytbl4_col44_fkey');
SELECT assign_fkey_checks_to_batch('BATCH1', 'phil''s schema3', 'mytbl4', 'mytbl4_col44_fkey');

--
-- Add manual steps dependancies
--

SELECT add_step_parent('BATCH1', 'myschema1.mytbl1', 'myschema1.mytbl2');

--
-- Complete the migration configuration
--

SELECT complete_migration_configuration('PG''s db');

COMMIT;

select * from step;
select * from table_to_process;
select * from table_part;
select * from sequence_to_process;
select * from table_column order by tco_schema, tco_table, tco_number;
select * from table_index order by tic_schema, tic_table, tic_object;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
else
  echo "  => The data migration is ready to start"
fi
