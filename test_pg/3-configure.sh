#!/usr/bin/bash
# 3-configure.sh
# This shell script prepares the data2pg components on the destination database

export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=test_dest

echo "==================================================="
echo "Prepare the data2pg test with Postgres databases"
echo "==================================================="

psql -U data2pg -a <<EOF

\set ON_ERROR_STOP ON

BEGIN TRANSACTION;

--
-- Create the migration object and the FDW infrastructure
--

SELECT data2pg.drop_migration('mig_all');

select * from data2pg.migration;
select * from data2pg.batch;
\des
\dn

SELECT data2pg.create_migration(
    p_migration            => 'mig_all',
    p_sourceDbms           => 'PostgreSQL',
    p_extension            => 'postgres_fdw',
    p_serverOptions        => 'port ''5432'', dbname ''test_src'', fetch_size ''1000''',
    p_userMappingOptions   => 'user ''postgres'', password ''postgres'''
);


--
-- Register the tables and sequences
--

SELECT data2pg.register_tables('mig_all', 'myschema1', '.*', NULL);
SELECT data2pg.register_tables('mig_all', 'myschema2', '.*', NULL);
SELECT data2pg.register_tables('mig_all', 'phil''s schema3', '.*', NULL);

SELECT data2pg.register_sequences('mig_all', 'myschema1', '.*', NULL);
SELECT data2pg.register_sequences('mig_all', 'myschema2', '.*', NULL);
SELECT data2pg.register_sequences('mig_all', 'phil''s schema3', '.*', NULL);

--
-- Register the columns transformation rules
--

SELECT register_column_transform_rule('myschema1','mytbl1','col11','col11_renamed');
SELECT register_column_transform_rule('myschema1','mytbl1','col11_renamed','col11');
SELECT register_column_transform_rule('myschema1','mytbl1','col13','substr(col13, 1, 10)');

--
-- Register the table parts
--

SELECT data2pg.register_table_part('myschema2', 'mytbl1', 1, 'col11 < 50000', TRUE, FALSE);
SELECT data2pg.register_table_part('myschema2', 'mytbl1', 2, 'col11 >= 50000 and col12 = ''ABC''', FALSE, FALSE);
SELECT data2pg.register_table_part('myschema2', 'mytbl1', 3, 'col11 >= 50000 and col12 = ''DEF''', FALSE, FALSE);
SELECT data2pg.register_table_part('myschema2', 'mytbl1', 4, 'col11 >= 50000 and col12 = ''GHI''', FALSE, FALSE);
SELECT data2pg.register_table_part('myschema2', 'mytbl1', 5, NULL, FALSE, TRUE);

--
-- Build the batches
--
SELECT data2pg.drop_batch('BATCH0');
SELECT data2pg.drop_batch('BATCH1');
SELECT data2pg.drop_batch('COMPARE_ALL');

SELECT data2pg.create_batch('BATCH0','mig_all','COPY',true);
SELECT data2pg.create_batch('BATCH1','mig_all','COPY',false);
SELECT data2pg.create_batch('COMPARE_ALL','mig_all','COMPARE');

--
-- Assign the tables and sequences to batches
--

SELECT data2pg.assign_tables_to_batch('BATCH1', 'myschema1', '.*', NULL);
--select data2pg.assign_tables_to_batch('BATCH1', 'myschema1', '.*', '^mytbl2b$');
SELECT data2pg.assign_tables_to_batch('BATCH1', 'myschema2', '.*', '^mytbl1$');
SELECT data2pg.assign_tables_to_batch('BATCH1', 'phil''s schema3', '.*', NULL);
--select data2pg.assign_tables_to_batch('BATCH1', 'myschema4', '.*', NULL);

SELECT data2pg.assign_sequences_to_batch('BATCH1', 'myschema1', '.*', NULL);
SELECT data2pg.assign_sequences_to_batch('BATCH1', 'myschema2', '.*', NULL);
SELECT data2pg.assign_sequences_to_batch('BATCH1', 'phil''s schema3', '.*', NULL);
--select data2pg.assign_sequences_to_batch('BATCH1', 'myschema4', '.*', NULL);

SELECT data2pg.assign_tables_to_batch('COMPARE_ALL', 'myschema1', '.*', NULL);
SELECT data2pg.assign_tables_to_batch('COMPARE_ALL', 'myschema2', '.*', '^(mytbl6|mytbl5)$');  -- JSON or POINT types cannot be compared
SELECT data2pg.assign_tables_to_batch('COMPARE_ALL', 'phil''s schema3', '.*', NULL);

--
-- assign the table parts to batches
--

SELECT data2pg.assign_table_part_to_batch('BATCH0', 'myschema2', 'mytbl1', 1);
SELECT data2pg.assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 2);
SELECT data2pg.assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 3);
SELECT data2pg.assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 4);
SELECT data2pg.assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 5);

--
-- Assign FK checks
--

SELECT data2pg.assign_fkey_checks_to_batch('BATCH1', 'myschema2', 'mytbl1');
SELECT data2pg.assign_fkey_checks_to_batch('BATCH1', 'myschema1', 'mytbl4');
SELECT data2pg.assign_fkey_checks_to_batch('BATCH1', 'myschema2', 'mytbl4', 'mytbl4_col44_fkey');
SELECT data2pg.assign_fkey_checks_to_batch('BATCH1', 'phil''s schema3', 'mytbl4', 'mytbl4_col44_fkey');

--
-- Complete the migration configuration
--

SELECT complete_migration_configuration('mig_all');

COMMIT;

select * from step;
select * from table_to_process;
select * from table_part;
select * from sequence_to_process;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
else
  echo "  => The data migration is ready to start"
fi
