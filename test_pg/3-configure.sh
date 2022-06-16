#!/usr/bin/bash
# 3-configure.sh
# This shell script prepares the data2pg components on the destination database

# Environment variables to setup
export PGHOST=localhost
export PGDATABASE=test_dest

echo "==================================================="
echo "Prepare the data2pg test with Postgres databases"
echo "==================================================="

# Default values
PGHOST_DEFAULT_VALUE=
PGPORT_DEFAULT_VALUE=5432
PGDATABASE_DEFAULT_VALUE=postgres

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

SELECT drop_migration(
    p_migration                => 'PG''s db'
);

SELECT create_migration(
    p_migration                => 'PG''s db',
    p_sourceDbms               => 'PostgreSQL',
    p_extension                => 'postgres_fdw',
    p_serverOptions            => 'port ''5432'', dbname ''test_src'', fetch_size ''1000''',
    p_userMappingOptions       => 'user ''postgres'', password ''postgres''',
    p_importSchemaOptions      => 'import_default ''false'''
);

--
-- Create a custom function to convert table names
--
CREATE FUNCTION tables_renaming_rules(TEXT) RETURNS TEXT LANGUAGE SQL AS
\$\$
    SELECT CASE WHEN \$1 = 'MYTBL4' THEN 'mytbl4' ELSE \$1 END;
\$\$;

--
-- Register the tables
--

SELECT register_tables(
    p_migration                => 'PG''s db',
    p_schema                   => 'myschema1',
    p_tablesToInclude          => '^MYTBL4$',
    p_tablesToExclude          => NULL,                               -- Default
    p_sourceSchema             => NULL,                               -- Default
    p_sourceTableNamesFnct     => 'data2pg03.tables_renaming_rules',
    p_sourceTableStatLoc       => 'source_table_stat',                -- Default
    p_createForeignTable       => TRUE,                               -- Default
    p_ForeignTableOptions      => NULL,                               -- Default
    p_sortByClusterIdx         => TRUE                                -- Default
);
SELECT register_table(
    p_migration                => 'PG''s db',
    p_schema                   => 'myschema2',
    p_table                    => 'myTbl3',
    p_sourceSchema             => NULL,                               -- Default
    p_sourceTableNamesFnct     => NULL,                               -- Default
    p_sourceTableStatLoc       => 'source_table_stat',                -- Default
    p_createForeignTable       => TRUE,                               -- Default
    p_ForeignTableOptions      => NULL,                               -- Default
    p_sortByClusterIdx         => TRUE                                -- Default
);
SELECT register_tables('PG''s db', 'myschema1', '.*', NULL);
SELECT register_tables('PG''s db', 'myschema2', '.*');
SELECT register_tables('PG''s db', 'phil''s schema3',
       p_ForeignTableOptions => 'OPTIONS(updatable ''false'')', p_createForeignTable => true);

--
-- Register the columns transformation rules
--

SELECT register_column_transform_rule(
    p_schema                   => 'myschema1',
    p_table                    => 'mytbl1',
    p_column                   => 'col11',
    p_expression               => 'col11_renamed'
);
SELECT register_column_transform_rule('myschema1','mytbl1','col11','col11');
SELECT register_column_transform_rule('myschema1','mytbl1','col13','substr(col13, 1, 10)');

--
-- Register the columns comparison rules
--

SELECT register_column_comparison_rule(
    p_schema                   => 'myschema1',
    p_table                    => 'mytbl1',
    p_column                   => 'col11',
    p_sourceExpression         => 'col11',
    p_targetExpression         => NULL                                -- Default
);
SELECT register_column_comparison_rule('myschema1','myTbl3','col32',NULL);
SELECT register_column_comparison_rule('myschema1','myTbl3','col33','trunc(col33,1)','trunc(col33,0)');

--
-- Register the table parts
--

SELECT register_table_part(
    p_schema                   => 'myschema2',
    p_table                    => 'mytbl1',
    p_partId                   => 'pre',
    p_condition                => NULL,                               -- Default
    p_isFirstPart              => TRUE,
    p_isLastPart               => FALSE                               -- Default
);
SELECT register_table_part('myschema2', 'mytbl1', '1', 'col11 < 50000', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', '2', 'col11 >= 50000 and col12 = ''ABC''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', '3', 'col11 >= 50000 and col12 = ''DEF''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', '4', 'col11 >= 50000 and col12 = ''GHI''', FALSE, FALSE);
SELECT register_table_part('myschema2', 'mytbl1', 'post', NULL, FALSE, TRUE);

SELECT register_table_part('myschema2', 'myTbl3', '1', 'TRUE', TRUE, FALSE);  -- copy all rows in a single step
SELECT register_table_part('myschema2', 'myTbl3', '2', NULL, FALSE, TRUE);    -- but separate the post-processing to separate index creations

--
-- Register the sequences
--

SELECT register_sequences(
    p_migration                => 'PG''s db',
    p_schema                   => 'myschema1',
    p_sequencesToInclude       => '.*',                               -- Default
    p_sequencesToExclude       => NULL,                               -- Default
    p_sourceSchema             => NULL,                               -- Default
    p_sourceSequenceNamesFnct  => NULL                                -- Default
);
SELECT register_sequence(
    p_migration                => 'PG''s db',
    p_schema                   => 'myschema2',
    p_sequence                 => 'MYSEQ2',
    p_sourceSchema             => NULL,                               -- Default
    p_sourceSequenceNamesFnct  => 'lower'
);
SELECT register_sequences('PG''s db', 'myschema2', '.*');
SELECT register_sequences('PG''s db', 'phil''s schema3');


--
-- Create the batches
--
SELECT drop_batch(
    p_batchName                => 'BATCH0'
);
SELECT drop_batch('BATCH1');
SELECT drop_batch('COMPARE_ALL');
SELECT drop_batch('DISCOVER_ALL');

SELECT create_batch(
    p_batchName                => 'BATCH0',
    p_migration                => 'PG''s db',
    p_batchType                => 'COPY',
    p_withInitStep             => true,
    p_withEndStep              => false
);
SELECT create_batch('BATCH1', 'PG''s db', 'COPY', false, true);
SELECT create_batch('COMPARE_ALL', 'PG''s db', 'COMPARE', true, true);
SELECT create_batch('CHECK_TABLES', 'PG''s db', 'COPY', false, false);
--SELECT create_batch('DISCOVER_ALL', 'PG''s db', 'DISCOVER', true, true);

--
-- Assign the tables to batches
--

SELECT assign_tables_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema1',
    p_tablesToInclude          => '.*',                               -- Default
    p_tablesToExclude          => NULL                                -- Default
);
SELECT assign_table_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_table                    => 'mytbl2'
);
SELECT assign_tables_to_batch('BATCH1', 'myschema2', '.*', '^(mytbl1|mytbl2|myTbl3)$');
SELECT assign_tables_to_batch('BATCH1', 'phil''s schema3');

SELECT assign_tables_to_batch('COMPARE_ALL', 'myschema1', '.*', NULL);
SELECT assign_tables_to_batch('COMPARE_ALL', 'myschema2', '.*', '^(mytbl1|mytbl5|mytbl6)$');  -- JSON or POINT types cannot be compared
SELECT assign_tables_to_batch('COMPARE_ALL', 'phil''s schema3', '.*', NULL);

--SELECT assign_tables_to_batch('DISCOVER_ALL', 'myschema1', '.*', NULL);
--SELECT assign_tables_to_batch('DISCOVER_ALL', 'myschema2', '.*', NULL);
--SELECT assign_tables_to_batch('DISCOVER_ALL', 'phil''s schema3', '.*', NULL);

--
-- assign the table parts to batches
--

SELECT assign_table_part_to_batch(
    p_batchName                => 'BATCH0',
    p_schema                   => 'myschema2',
    p_table                    => 'mytbl1',
    p_partId                   => 'pre'
);
SELECT assign_table_part_to_batch('BATCH0', 'myschema2', 'mytbl1', '1');
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', '2');
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', '3');
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', '4');
SELECT assign_table_part_to_batch('BATCH1', 'myschema2', 'mytbl1', 'post');

SELECT assign_table_parts_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_table                    => 'myTbl3',
    p_partsToInclude           => '.*',
    p_partsToExclude           => NULL
);

SELECT assign_table_parts_to_batch('COMPARE_ALL', 'myschema2', 'mytbl1', '.*', '^(pre|post)$');

--
-- Assign the index creations
--

SELECT assign_index_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_table                    => 'myTbl3',
    p_object                   => 'myTbl3_pkey'
);
SELECT assign_indexes_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_table                    => 'myTbl3',
    p_objectsToInclude         => '.*',                               -- Default
    p_objectsToExclude         => NULL                                -- Default
);

--
-- Assign table checks
--
SELECT assign_table_checks_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema1',
    p_table                    => 'mytbl1'
);
SELECT assign_tables_checks_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_tablesToInclude          => '.*',                               -- Default
    p_tablesToExclude          => NULL                                -- Default
);

SELECT assign_tables_checks_to_batch('CHECK_TABLES', 'myschema1', '.*', NULL);
SELECT assign_tables_checks_to_batch('CHECK_TABLES', 'myschema2', '.*', NULL);
SELECT assign_tables_checks_to_batch('CHECK_TABLES', 'phil''s schema3');

--
-- Assign FK checks
--

SELECT assign_fkey_checks_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_table                    => 'mytbl1',
    p_fkey                     => NULL                                -- Default
);
SELECT assign_fkey_checks_to_batch('BATCH1', 'myschema1', 'MYTBL4');
SELECT assign_fkey_checks_to_batch('BATCH1', 'myschema2', 'mytbl4', 'mytbl4_col44_fkey');
SELECT assign_fkey_checks_to_batch('BATCH1', 'phil''s schema3', 'mytbl4', 'mytbl4_col44_fkey');

--
-- Assign the sequences to batches
--

SELECT assign_sequences_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema1',
    p_sequencesToInclude       => '.*',                               -- Default
    p_sequencesToExclude       => NULL                                -- Default
);
SELECT assign_sequence_to_batch(
    p_batchName                => 'BATCH1',
    p_schema                   => 'myschema2',
    p_sequence                 => 'myseq2'
);
SELECT assign_sequences_to_batch('BATCH1', 'myschema2', '.*', '^myseq2$');
SELECT assign_sequences_to_batch('BATCH1', 'phil''s schema3');

SELECT assign_sequences_to_batch('COMPARE_ALL', 'myschema1', '.*', NULL);
SELECT assign_sequences_to_batch('COMPARE_ALL', 'myschema2', '.*', NULL);
SELECT assign_sequences_to_batch('COMPARE_ALL', 'phil''s schema3', '.*', NULL);

--
-- Add custom steps
--
SELECT assign_custom_step_to_batch(
    p_batchName                => 'BATCH1',
    p_stepName                 => 'custom_step.1',
    p_function                 => '_do_nothing',
    p_schema                   => NULL,
    p_object                   => NULL,
    p_subObject                => NULL,
    p_cost                     => 1
);

--
-- Add manual steps dependancies
--

SELECT add_step_parent(
    p_batchName                => 'BATCH1',
    p_step                     => 'myschema1.mytbl1',
    p_parent_step              => 'myschema1.mytbl2'
);

--
-- Adjust the steps cost
--

SELECT set_step_cost_multiplier(
    p_batchName                => 'BATCH1',
    p_step                     => 'myschema1.mytbl1',
    p_factor                   => 2.5
    );

--
-- Complete the migration configuration
--

SELECT complete_migration_configuration(
    p_migration                => 'PG''s db'
);

COMMIT;

SELECT stp_batch_name as "Batch", count(*) AS "#steps" FROM step GROUP BY 1 ORDER BY stp_batch_name;

EOF

if [ $? -ne 0 ]; then
  echo "  => Problem encountered"
  exit 1
else
  echo "  => The scheduler can use this migration configuration"
fi
