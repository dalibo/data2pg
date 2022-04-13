-- This file belongs to Data2Pg, the framework that helps migrating data to PostgreSQL databases from various sources.
-- This script defines the data2pg extension content.

-- Complain if the script is sourced in psql, rather than a CREATE EXTENSION statement.
\echo Use "CREATE EXTENSION data2pg" to load this file. \quit
---
--- Preliminary checks.
---
DO LANGUAGE plpgsql
$$
BEGIN
    IF current_setting('server_version_num')::int < 90600 THEN
        RAISE EXCEPTION 'The current postgres version (%) is too old. It should be at least 9.6', current_setting('server_version');
    END IF;
END
$$;

--
-- Create specific types.
--
-- The batch_id_type coposite type is used as output record for the __get_batch_ids() function called by the Data2Pg scheduler.
CREATE TYPE batch_id_type AS (
    bi_batch_name              TEXT,                    -- The name of the batch
    bi_batch_type              TEXT,                    -- The batch type
    bi_mgr_name                TEXT,                    -- A name of the migration the batch belongs to
    bi_mgr_config_completed    BOOLEAN                  -- Boolean indicating whether the migration configuration is completed or not
);

-- The working_plan_type composite type is used as output record for the __get_working_plan() function called by the Data2Pg scheduler.
CREATE TYPE working_plan_type AS (
    wp_name                    TEXT,                    -- The name of the step
    wp_sql_function            TEXT,                    -- The sql function to execute (for common cases)
    wp_shell_script            TEXT,                    -- A shell script to execute (for specific purpose only)
    wp_cost                    BIGINT,                  -- A relative cost indication used to plan the run (a table size for instance)
    wp_parents                 TEXT[]                   -- A set of parent steps that need to be completed to allow the step to start
);

-- The step_report_type is used as output record for the elementary step functions called by the Data2Pg scheduler.
CREATE TYPE step_report_type AS (
    sr_indicator               TEXT,                    -- The indicator, depending on the called function
                                                        --   ('COPIED_ROWS'/'COPIED_SEQUENCES'/'COMPARED_ROWS'/...)
    sr_value                   BIGINT,                  -- The numeric value associated to the message, if any
    sr_rank                    SMALLINT,                -- The rank of the indicator when displayed by the scheduler
    sr_is_main_indicator       BOOLEAN                  -- Boolean indicating whether the indicator is the main indicator to display by the monitoring clients
);

--
-- Create tables.
--

-- The migration table contains a row per migration, i.e. a connection to a remote source database.
CREATE TABLE migration (
    mgr_name                   TEXT NOT NULL,           -- The migration name
    mgr_source_dbms            TEXT NOT NULL            -- The RDBMS as source database
                               CHECK (mgr_source_dbms IN ('Oracle', 'SQLServer', 'Sybase_ASA', 'PostgreSQL')),
    mgr_extension              TEXT NOT NULL,           -- Extension name
    mgr_server_name            TEXT NOT NULL,           -- The FDW server name
    mgr_server_options         TEXT NOT NULL,           -- The options for the server, like 'host ''localhost'', port ''5432'', dbname ''test1'''
    mgr_user_mapping_options   TEXT NOT NULL,           -- The user mapping options used by the data2pg role to reach the source database
                                                        --   like 'user ''postgres'', password ''pwd''', but the password is masked
    mgr_import_schema_options  TEXT,                    -- The options added to the IMPORT FOREIGN SCHEMA statement when registering tables
    mgr_config_completed       BOOLEAN,                 -- Boolean indicating whether the migration configuration is completed or not
    PRIMARY KEY (mgr_name)
);

-- The batch table contains a row per batch, i.e. a set of tables and sequences processed by a single data2pg run.
CREATE TABLE batch (
    bat_name                   TEXT NOT NULL,           -- The batch name
    bat_migration              TEXT NOT NULL,           -- The migration the batch belongs to
    bat_type                   TEXT NOT NULL            -- The batch type, i.e. the type of action to perform
                               CHECK (bat_type IN ('COPY', 'CHECK', 'COMPARE', 'DISCOVER')),
    bat_with_init_step         BOOLEAN,                 -- Boolean indicating whether a INIT step has to be included in the batch working plan
    bat_with_end_step          BOOLEAN,                 -- Boolean indicating whether a END step has to be included in the batch working plan
    PRIMARY KEY (bat_name),
    FOREIGN KEY (bat_migration) REFERENCES migration (mgr_name)
);

-- The step table contains a row for each elementary step of batches executions.
CREATE TABLE step (
    stp_batch_name             TEXT NOT NULL,           -- Batch the step belongs to
    stp_name                   TEXT NOT NULL,           -- The name of the step
                                                        --   By construction it contains the schema, the table or sequence names
                                                        --   and the table part number if any
    stp_type                   TEXT  NOT NULL           -- step type
                               CHECK (stp_type IN ('INIT', 'TABLE', 'SEQUENCE', 'TABLE_PART', 'INDEX', 'CHECK', 'FOREIGN_KEY', 'CUSTOM', 'END')),
    stp_schema                 TEXT,                    -- Schema name of the sequence or table to process,
                                                        --   NULL if the step is not related to a sequence or a table
    stp_object                 TEXT,                    -- Object name, typically the sequence or table to process,
                                                        --   NULL if the step is not related to a sequence or a table
    stp_part_num               INTEGER,                 -- table part number for tables processed by several steps, NULL if NA
    stp_sub_object             TEXT,                    -- Sub-object, typically a foreign key to process, NULL if NA
    stp_sql_function           TEXT,                    -- The sql function to execute (for common cases)
    stp_shell_script           TEXT,                    -- A shell script to execute (for specific purpose only)
    stp_cost                   BIGINT,                  -- A relative cost indication used to plan the run
                                                        --   (generally built with the row count and the table size)
    stp_added_parents          TEXT[],                  -- Some additional parent steps created by add_step_parent() before building the final stp_parents array
    stp_parents                TEXT[],                  -- The set of parent steps that need to be completed to allow the step to start
    PRIMARY KEY (stp_batch_name, stp_name),
    FOREIGN KEY (stp_batch_name) REFERENCES batch (bat_name)
);

-- The table_to_process table contains a row for each table to process, with some useful data to migrate it.
CREATE TABLE table_to_process (
    tbl_schema                 TEXT NOT NULL,           -- The schema of the target table
    tbl_name                   TEXT NOT NULL,           -- The name of the target table
    tbl_migration              TEXT NOT NULL,           -- The migration the table is linked to
    tbl_foreign_schema         TEXT NOT NULL,           -- The schema containing the foreign table representing the source table
    tbl_foreign_name           TEXT NOT NULL,           -- The name of the foreign table representing the source table
    tbl_source_schema          TEXT NOT NULL,           -- The schema name in the source database
    tbl_source_name            TEXT NOT NULL,           -- The table name in the source table
    tbl_rows                   BIGINT,                  -- The approximative number of rows of the source table
    tbl_kbytes                 FLOAT,                   -- The size in K-Bytes of the source table
    tbl_copy_sort_order        TEXT,                    -- The ORDER BY clause, if needed, for the INSERT SELECT copy statement (NULL if no sort)
    tbl_some_gen_alw_id_col    BOOLEAN,                 -- TRUE when there are some "generated always as identity columns" in the table's definition
    tbl_referencing_tables     TEXT[],                  -- The other tables that are referenced by FKeys on this table
    tbl_referenced_tables      TEXT[],                  -- The other tables whose FKeys references this table
    PRIMARY KEY (tbl_schema, tbl_name),
    FOREIGN KEY (tbl_migration) REFERENCES migration (mgr_name)
);

-- The table_column table contains a row for each column of the tables to process.
CREATE TABLE table_column (
    tco_schema                 TEXT NOT NULL,           -- The schema of the target table
    tco_table                  TEXT NOT NULL,           -- The name of the target table
    tco_name                   TEXT NOT NULL,           -- The name of the column in the target table
    tco_number                 INTEGER NOT NULL,        -- The column rank in the table
    tco_type                   TEXT NOT NULL,           -- The column type in the target table as described into the catalog
    tco_copy_source_expr       TEXT,                    -- For a COPY batch, the expression to select (generaly the source column name)
                                                        --   (NULL if the column is generated always as an expression)
    tco_copy_dest_col          TEXT,                    -- For a COPY batch, the column to insert into (generally the source column name)
                                                        --   (NULL if the column is generated always as an expression)
    tco_compare_source_expr    TEXT,                    -- For a COMPARE batch, the columns/expressions to select from the foreign table
                                                        --   (NULL if the column is masked)
    tco_compare_dest_expr      TEXT,                    -- For a COMPARE batch, the columns/expressions to select from the local postgres table
                                                        --   (NULL if the column is masked)
    tco_compare_sort_rank      INTEGER,                 -- For a COMPARE batch, the column rank in the table sort, NULL if the column is not used for the sort
    PRIMARY KEY (tco_schema, tco_table, tco_name),
    FOREIGN KEY (tco_schema, tco_table) REFERENCES table_to_process(tbl_schema, tbl_name)
);

-- The table_index table contains a row for each index or constraint.
CREATE TABLE table_index (
    tic_schema                 TEXT NOT NULL,           -- The schema of the target table
    tic_table                  TEXT NOT NULL,           -- The table name
    tic_object                 TEXT NOT NULL,           -- The name of the index or constraint
    tic_type                   VARCHAR(2) NOT NULL      -- The object type (I for index / Cp pour PKEY, Cu for UNIQUE constraint
                                                        --   or Cx for EXCLUDE constraint)
                               CHECK (tic_type IN ('I', 'Cp', 'Cu', 'Cx')),
    tic_definition             TEXT NOT NULL,           -- The index or constraint definition
    tic_drop_for_copy          BOOLEAN,                 -- A boolean indicating whether the index or constraint must be dropped at bulk insert time
    tic_separate_creation_step BOOLEAN DEFAULT FALSE,   -- A boolean indicating whether the index has to be created by a separate step
    tic_last_drop_ts           TIMESTAMPTZ,             -- The timestamp of the latest drop of this index or constraint
    tic_last_create_ts         TIMESTAMPTZ,             -- The timestamp of the latest re-creation of this index or constraint
    tic_last_create_duration   INTERVAL,                -- The last recreation duration
    PRIMARY KEY (tic_schema, tic_table, tic_object),
    FOREIGN KEY (tic_schema, tic_table) REFERENCES table_to_process (tbl_schema, tbl_name)
);

-- The table_part table contains a row for each part of table to migrate. A row in the table_to_process table must exist.
CREATE TABLE table_part (
    prt_schema                 TEXT NOT NULL,           -- The schema of the target table
    prt_table                  TEXT NOT NULL,           -- The name of the target table
    prt_number                 INTEGER,                 -- The part number
    prt_condition              TEXT,                    -- The condition to be set as a WHERE clause in the copy statement.
                                                        -- Set to  NULL if no table copy for this part (ie, only processing of first_step or last_step).
    prt_is_first_step          BOOLEAN,                 -- TRUE when this is the first step for the table (to truncate the table and drop the secondary indexes)
    prt_is_last_step           BOOLEAN,                 -- TRUE when this is the last step for the table (to recreate the secondary indexes and analyze the table)
    PRIMARY KEY (prt_schema, prt_table, prt_number),
    FOREIGN KEY (prt_schema, prt_table) REFERENCES table_to_process (tbl_schema, tbl_name)
);

-- The sequence_to_process table contains a row for sequence to process.
CREATE TABLE sequence_to_process (
    seq_name                   TEXT NOT NULL,           -- The name of the target sequence
    seq_schema                 TEXT NOT NULL,           -- The schema of the target sequence
    seq_migration              TEXT NOT NULL,           -- The migration the sequence is linked to
    seq_foreign_schema         TEXT NOT NULL,           -- The schema of the schema containing the foreign table representing the source sequence
    seq_foreign_name           TEXT NOT NULL,           -- The name of the foreign table representing the source sequence
    seq_source_schema          TEXT NOT NULL,           -- The schema or user owning the sequence in the source database
    seq_source_name            TEXT NOT NULL,           -- The sequence name in the source database
    PRIMARY KEY (seq_schema, seq_name),
    FOREIGN KEY (seq_migration) REFERENCES migration (mgr_name)
);

-- The source_table_stat table materializes the tables statistics from the source database in a generic way for all DBMS.
-- These statistics are physically stored into the local database because some FDW are not able to properly handle row selection on foreign tables.
CREATE TABLE source_table_stat (
    stat_migration           TEXT NOT NULL,              -- The migration name, identifying the source database the statistics come from
    stat_schema              TEXT NOT NULL,              -- The name of the source schema
    stat_table               TEXT NOT NULL,              -- The schema of the source table
    stat_rows                BIGINT,                     -- The number of rows as reported by the source catalog
    stat_kbytes              FLOAT,                      -- The table data volume in k-bytes as reported by the source catalog
    PRIMARY KEY (stat_migration, stat_schema, stat_table)
);

-- The counter table is populated with the result of checks.
CREATE TABLE counter (
    cnt_schema               TEXT NOT NULL,              -- The schema of the target table
    cnt_table                TEXT NOT NULL,              -- The target table name
    cnt_database             CHAR NOT NULL               -- The database the rows comes from ; either Source or Destination
                             CHECK (cnt_database IN ('S', 'D')),
    cnt_counter              TEXT,                       -- The counter id
    cnt_value                BIGINT,                     -- The counter value
    cnt_timestamp            TIMESTAMPTZ                 -- The transaction timestamp of the table check
                             DEFAULT clock_timestamp(),
    PRIMARY KEY (cnt_schema, cnt_table, cnt_database, cnt_counter)
);

-- The content_diff table is populated with the result of elementary compare_table and compare_sequence steps.
-- The table is just a report and has no pkey.
CREATE TABLE content_diff (
    diff_timestamp           TIMESTAMPTZ                 -- The transaction timestamp of the relation comparison
                             DEFAULT current_timestamp,
    diff_schema              TEXT NOT NULL,              -- The name of the destination schema
    diff_relation            TEXT NOT NULL,              -- The name of the destination table used for the comparison
    diff_rank                BIGINT,                     -- A difference number
    diff_database            CHAR NOT NULL               -- The database the rows comes from ; either Source or Destination
                             CHECK (diff_database IN ('S', 'D')),
    diff_key_cols            JSON,                       -- The JSON representation of the table key, for rows that are different between both databases
    diff_other_cols          JSON                        -- The JSON representation of the table columns not in key, for rows that are different between
                                                         --   both databases or the sequence characteristics that are different between both databases
);

-- The discovery_table table is populated with the result of elementary discover_table steps.
CREATE TABLE discovery_table (
    dscv_schema              TEXT NOT NULL,              -- The name of the schema holding the table on the source database
    dscv_table               TEXT NOT NULL,              -- The analyzed table name on the source database
    dscv_timestamp           TIMESTAMPTZ                 -- The transaction timestamp of the table analysis
                             DEFAULT current_timestamp,
    dscv_max_row             BIGINT,                     -- The DISCOVER_MAX_ROWS parameter, if set at _discover_table() time.
    dscv_nb_row              BIGINT,                     -- The number of rows in the analyzed table
    PRIMARY KEY (dscv_schema, dscv_table)
);

-- The discovery_column table is populated with the result of elementary discover_table steps.
CREATE TABLE discovery_column (
    dscv_schema              TEXT NOT NULL,              -- The name of the schema holding the table on the source database
    dscv_table               TEXT NOT NULL,              -- The analyzed table name on the source database
    dscv_column              TEXT NOT NULL,              -- The analyzed column name
    dscv_column_num          INTEGER,                    -- The column rank in the pg_attribute table (i.e. its attnum)
    dscv_type                TEXT,                       -- The column data type in the foreign table
    dscv_max_size            INTEGER,                    -- The column max size when declared as character varying
    dscv_nb_not_null         BIGINT,                     -- The number of NOT NULL values
    dscv_num_min             NUMERIC,                    -- The minimum value for a numeric column
    dscv_num_max             NUMERIC,                    -- The maximum value for a numeric column
    dscv_num_max_precision   SMALLINT,                   -- The maximum number of digits needed to represent the column
    dscv_num_max_integ_part  SMALLINT,                   -- The maximum number of digits for the integer part of the column
    dscv_num_max_fract_part  SMALLINT,                   -- The maximum number of digits for the fractional part of the column
    dscv_str_avg_length      INTEGER,                    -- The average length for a string or bytea column
    dscv_str_max_length      INTEGER,                    -- The maximum length for a string or bytea column
    dscv_str_nul_char        INTEGER,                    -- The number of fields containing \x00 characters
    dscv_ts_min              TEXT,                       -- The minimum value for a date/time column
    dscv_ts_max              TEXT,                       -- The maximum value for a date/time column
    dscv_ts_nb_date          BIGINT,                     -- The number of pure dates for a timestamp column (i.e. with time part set to 00:00:00)
    dscv_stats_ndistinct     REAL,                       -- The ndistinct value from the pg_stats view after an ANALYZE on the foreign table
    dscv_has_2_values        BOOLEAN,                    -- True if the column is a hidden boolean
    PRIMARY KEY (dscv_schema, dscv_table, dscv_column),
    FOREIGN KEY (dscv_schema, dscv_table) REFERENCES discovery_table (dscv_schema, dscv_table)
);
-- The discovery_advice table contains pieces of advice deducted from the discovery_table and discovery_column content.
CREATE TABLE discovery_advice (
    dscv_schema              TEXT NOT NULL,              -- The name of the schema holding the foreign table
    dscv_table               TEXT NOT NULL,              -- The analyzed foreign table name
    dscv_column              TEXT,                       -- The column associated to the advice, NULL for table level piece of advice
    dscv_column_num          INTEGER,                    -- The column rank in the pg_attribute table (i.e. its attnum)
    dscv_advice_type         TEXT NOT NULL               -- The advice type (W = Warning, A = Advice)
                             CHECK (dscv_advice_type IN ('W', 'A')),
    dscv_advice_code         TEXT,                       -- A coded representation of the advice, like 'INTEGER' for the message "column ... could be an INTEGER"
    dscv_advice_msg          TEXT,                       -- The textual representation of the piece of advice that can be infered from collected data
    FOREIGN KEY (dscv_schema, dscv_table) REFERENCES discovery_table (dscv_schema, dscv_table)
);

--
-- Create views.
--

CREATE VIEW counter_diff AS
    SELECT d.cnt_schema, d.cnt_table, d.cnt_counter,
           s.cnt_value as cnt_source_value, d.cnt_value as cnt_dest_value,
           s.cnt_timestamp as cnt_source_ts, d.cnt_timestamp as cnt_dest_ts
        FROM counter s
             JOIN counter d ON (d.cnt_schema = s.cnt_schema AND d.cnt_table = s.cnt_table AND
                                        d.cnt_database = 'D' AND d.cnt_counter = s.cnt_counter)
        WHERE s.cnt_database = 'S'
          AND s.cnt_value <> d.cnt_value;

--
-- Create functions.
--

--
-- Functions used to describe the migrations.
--

-- The create_migration() function registers a new migration and creates:
--   * the FDW extension if it does not already exist
--   * the Foreign Server, named data2pg_<migration_name>_server
--   * and the User Mapping to use to reach the source database.
-- It returns the number of created migration, i.e. 1.
CREATE FUNCTION create_migration(
    p_migration              TEXT,
    p_sourceDbms             TEXT,
    p_extension              TEXT,
    p_serverOptions          TEXT,
    p_userMappingOptions     TEXT,
    p_userHasPrivileges      BOOLEAN DEFAULT false,
    p_importSchemaOptions    TEXT DEFAULT NULL
    )
    RETURNS INTEGER LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$create_migration$
DECLARE
    v_serverName             TEXT;
BEGIN
-- Check that no input parameter is NULL.
    IF p_migration IS NULL OR p_sourceDbms IS NULL OR p_extension IS NULL OR p_serverOptions IS NULL OR p_userMappingOptions IS NULL THEN
        RAISE EXCEPTION 'create_migration: No input parameter may be NULL.';
    END IF;
-- Check that the migration does not exist yet.
    PERFORM 0
        FROM @extschema@.migration
        WHERE mgr_name = p_migration;
    IF FOUND THEN
        RAISE EXCEPTION 'create_migration: The migration "%" already exists. Call the drop_migration() function to drop it, if needed.', p_migration;
    END IF;
-- Record the supplied parameters into the migration table, masking the password part of the user mapping options.
    v_serverName = 'data2pg_' || p_migration || '_server';
    INSERT INTO @extschema@.migration
        VALUES (p_migration, p_sourceDbms, p_extension, v_serverName, p_serverOptions,
                regexp_replace(p_userMappingOptions, '(password\s+'').*?('')', '\1########\2'),
                p_importSchemaOptions);
-- Create the FDW extension.
    EXECUTE format(
        'CREATE EXTENSION IF NOT EXISTS %s',
        p_extension);
-- Create the Server used to reach the source database.
    EXECUTE format(
        'CREATE SERVER IF NOT EXISTS %I'
        '    FOREIGN DATA WRAPPER %s'
        '    OPTIONS (%s)',
        v_serverName, p_extension, p_serverOptions);
    EXECUTE format(
        'GRANT USAGE ON FOREIGN SERVER %I TO data2pg',
        v_serverName);
-- Create the User Mapping to let the data2pg role or the current superuser installing the function log on the source database.
    EXECUTE format(
        'CREATE USER MAPPING IF NOT EXISTS FOR %s'
        '    SERVER %I'
        '    OPTIONS (%s)',
        current_user, v_serverName, p_userMappingOptions);
-- Load additional objects depending on the selected DBMS.
    PERFORM @extschema@.load_dbms_specific_objects(p_migration, p_sourceDbms, v_serverName, p_userHasPrivileges);
--
    RETURN 1;
END;
$create_migration$;

-- The load_dbms_specific_objects() function is called by the create_migration() function.
-- It creates additional objects, depending on the RDBMS of the source database.
-- In particular, it collects the volume statistics from the source catalog.
CREATE FUNCTION load_dbms_specific_objects(
    p_migration              TEXT,
    p_sourceDbms             TEXT,
    p_serverName             TEXT,
    p_userHasPrivileges      BOOLEAN DEFAULT false
    )
    RETURNS VOID LANGUAGE plpgsql AS
$load_dbms_specific_objects$
DECLARE
    v_queryTables            TEXT;
    v_querySequences         TEXT;
BEGIN
    -- perform DBMS specific tasks.
    IF p_sourceDbms = 'Oracle' THEN
        -- Create an image for ora_tables and ora_sequences tables.
        -- Depends of DBA privileges
        IF p_userHasPrivileges THEN
            v_queryTables := '('
                'SELECT t.owner, t.table_name, num_rows, bytes'
                '  FROM dba_tables t JOIN dba_segments s ON t.table_name = s.segment_name'
            ')';
            v_querySequences := '('
                'SELECT sequence_owner, sequence_name, last_number'
                '  FROM dba_sequences'
            ')';
        ELSE
            v_queryTables := '('
                'SELECT user, t.table_name, num_rows, bytes'
                '  FROM user_tables t JOIN user_segments s ON t.table_name = s.segment_name'
            ')';
            v_querySequences := '('
                'SELECT sequence_owner, sequence_name, last_number'
                '  FROM all_sequences'
            ')';
        END IF;
        EXECUTE format(
            'CREATE FOREIGN TABLE @extschema@.ora_tables ('
            '   owner VARCHAR(128),'
            '   table_name VARCHAR(128),'
            '   num_rows BIGINT,'
            '   bytes BIGINT'
            ') SERVER %I OPTIONS ( table ''%s'')',
            p_serverName, v_queryTables
        );
        EXECUTE format(
            'CREATE FOREIGN TABLE @extschema@.ora_sequences ('
            '   sequence_owner VARCHAR(128),'
            '   sequence_name VARCHAR(128),'
            '   last_number bigint'
            ') SERVER %I OPTIONS ( table ''%s'')',
            p_serverName, v_querySequences
        );

        -- Populate the source_table_stat table.
        INSERT INTO @extschema@.source_table_stat
            SELECT p_migration, owner, table_name, num_rows, sum(bytes) / 1024
              FROM @extschema@.ora_tables GROUP BY 1,2,3,4;

        -- Drop the now useless foreign tables, but keep the ora_sequences
        -- that will be used by the _copy_sequence() function.
        DROP FOREIGN TABLE @extschema@.ora_tables;

    ELSIF p_sourceDbms = 'PostgreSQL' THEN
        -- Create an image of the pg_class table.
        EXECUTE format(
            'CREATE FOREIGN TABLE @extschema@.pg_foreign_pg_class ('
            '    relname TEXT,'
            '    relnamespace OID,'
            '    relkind TEXT,'
            '    reltuples BIGINT,'
            '    relpages BIGINT'
            ') SERVER %I OPTIONS (schema_name ''pg_catalog'', table_name ''pg_class'')',
            p_serverName);
        -- Create an image of the pg_namespace table.
        EXECUTE format(
            'CREATE FOREIGN TABLE @extschema@.pg_foreign_pg_namespace ('
            '    oid OID,'
            '    nspname TEXT'
            ') SERVER %I OPTIONS (schema_name ''pg_catalog'', table_name ''pg_namespace'')',
            p_serverName);
        -- Populate the source_table_stat table.
        INSERT INTO @extschema@.source_table_stat
            SELECT p_migration, nspname, relname, CASE WHEN reltuples = -1 THEN NULL ELSE reltuples::bigint END, relpages * 8
                FROM @extschema@.pg_foreign_pg_class
                     JOIN @extschema@.pg_foreign_pg_namespace ON (relnamespace = pg_foreign_pg_namespace.oid)
                WHERE relkind = 'r'
                  AND nspname NOT IN ('pg_catalog', 'information_schema');
        -- Drop the now useless foreign tables.
        DROP FOREIGN TABLE @extschema@.pg_foreign_pg_class, @extschema@.pg_foreign_pg_namespace;
    ELSIF p_sourceDbms = 'Sybase_ASA' THEN
        -- Create an image of the systab table.
        EXECUTE format(
            'CREATE FOREIGN TABLE @extschema@.asa_foreign_systab('
            '    user_name TEXT,'
            '    table_name TEXT,'
            '    row_count BIGINT,'
            '    size_kb FLOAT'
            ') SERVER %I OPTIONS (query '
            '    ''SELECT user_name,'
            '            table_name,'
            '            count AS row_count,'
            '            ISNULL((DB_PROPERTY(''''PageSize'''') * count_set_bits(tab_page_list))/1024, 0) +'
            '              ISNULL((DB_PROPERTY(''''PageSize'''') * count_set_bits(ext_page_list))/1024, 0) as size_kb'
            '        FROM sys.systab'
            '             JOIN SYSUSER ON (creator = user_id)'
            '        WHERE creator <> 0'
            '          AND user_name <> ''''dbo'''' '''
            ')',
            p_serverName);
        -- Populate the source_table_stat table.
        INSERT INTO @extschema@.source_table_stat
            SELECT p_migration, user_name, table_name, row_count, size_kb
                FROM @extschema@.asa_foreign_systab;
        -- Drop the now useless foreign tables.
        DROP FOREIGN TABLE @extschema@.asa_foreign_systab;
    ELSE
        RAISE EXCEPTION 'load_dbms_specific_objects: The DBMS % is not yet implemented (internal error).', p_sourceDbms;
    END IF;
END;
$load_dbms_specific_objects$;

-- The drop_migration() function drops objects created by the create_migration() function and
-- by all subsequent functions that assigned tables, sequences to batches.
-- It returns the number of dropped foreign tables.
CREATE FUNCTION drop_migration(
    p_migration               TEXT
    )
    RETURNS INTEGER LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$drop_migration$
DECLARE
    v_serverName             TEXT;
    v_schemaList             TEXT;
    v_nbForeignTables        INT;
    r_tbl                    RECORD;
BEGIN
-- Check that the migration exists and get its characteristics.
    SELECT mgr_server_name INTO v_serverName
        FROM @extschema@.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
-- Drop the batches linked to the migration.
    PERFORM @extschema@.drop_batch(bat_name)
        FROM @extschema@.batch
        WHERE bat_migration = p_migration;
-- Drop foreign tables linked to this migration.
    v_nbForeignTables = 0;
    FOR r_tbl IN
        SELECT tbl_foreign_schema AS foreign_schema, tbl_foreign_name AS foreign_table
            FROM @extschema@.table_to_process
            WHERE tbl_migration = p_migration
        UNION
        SELECT seq_foreign_schema, seq_foreign_name
            FROM @extschema@.sequence_to_process
            WHERE seq_migration = p_migration
    LOOP
        v_nbForeignTables = v_nbForeignTables + 1;
        EXECUTE format(
            'DROP FOREIGN TABLE IF EXISTS %I.%I',
            r_tbl.foreign_schema, r_tbl.foreign_table);
    END LOOP;
-- Drop the schemas that contained the foreign objects and that are not used by any other migration.
    SELECT string_agg(quote_ident(foreign_schema), ',') INTO v_schemaList
        FROM (
          (  SELECT DISTINCT tbl_foreign_schema AS foreign_schema
                 FROM @extschema@.table_to_process
                 WHERE tbl_migration = p_migration
           UNION
             SELECT DISTINCT seq_foreign_schema
                 FROM @extschema@.sequence_to_process
                 WHERE seq_migration = p_migration
          ) EXCEPT (
             SELECT DISTINCT tbl_foreign_schema AS foreign_schema
                 FROM @extschema@.table_to_process
                 WHERE tbl_migration <> p_migration
          ) EXCEPT (
             SELECT DISTINCT seq_foreign_schema
                 FROM @extschema@.sequence_to_process
                 WHERE seq_migration <> p_migration
          )
            ) AS t;
    IF v_schemaList IS NOT NULL THEN
        EXECUTE 'DROP SCHEMA IF EXISTS ' || v_schemaList;
    END IF;
-- Remove rows from the discovery tables for tables belonging to the migration
    DELETE FROM @extschema@.discovery_advice
        USING @extschema@.table_to_process
        WHERE dscv_schema = tbl_schema AND dscv_table = tbl_name
          AND tbl_migration = p_migration;
    DELETE FROM @extschema@.discovery_column
        USING @extschema@.table_to_process
        WHERE dscv_schema = tbl_schema AND dscv_table = tbl_name
          AND tbl_migration = p_migration;
    DELETE FROM @extschema@.discovery_table
        USING @extschema@.table_to_process
        WHERE dscv_schema = tbl_schema AND dscv_table = tbl_name
          AND tbl_migration = p_migration;
-- Remove table columns associated to tables belonging to the migration.
    DELETE FROM @extschema@.table_column
        USING @extschema@.table_to_process
        WHERE tco_schema = tbl_schema AND tco_table = tbl_name
          AND tbl_migration = p_migration;
-- Remove indexes and constraints associated to tables belonging to the migration.
    DELETE FROM @extschema@.table_index
        USING @extschema@.table_to_process
        WHERE tic_schema = tbl_schema AND tic_table = tbl_name
          AND tbl_migration = p_migration;
-- Remove table parts associated to tables belonging to the migration.
    DELETE FROM @extschema@.table_part
        USING @extschema@.table_to_process
        WHERE prt_schema = tbl_schema AND prt_table = tbl_name
          AND tbl_migration = p_migration;
-- Remove tables from the table_to_process table.
    DELETE FROM @extschema@.table_to_process
        WHERE tbl_migration = p_migration;
-- Remove sequences from the sequence_to_process table.
    DELETE FROM @extschema@.sequence_to_process
        WHERE seq_migration = p_migration;
-- Remove the source table statistics from the source_table_stat table.
    DELETE FROM @extschema@.source_table_stat
        WHERE stat_migration = p_migration;
-- The FDW is left because it can be used for other purposes.
-- Drop the server, if it exists.
    EXECUTE format(
        'DROP SERVER IF EXISTS %I CASCADE',
        v_serverName);
-- Delete the row from the migration table.
    DELETE FROM @extschema@.migration where mgr_name = p_migration;
--
    RETURN v_nbForeignTables;
END;
$drop_migration$;

-- The create_batch() function registers a new batch for an existing migration.
-- It returns the number of created batch, i.e. 1.
CREATE FUNCTION create_batch(
    p_batchName              TEXT,                    -- Batch name
    p_migration              TEXT,                    -- Migration name
    p_batchType              TEXT,                    -- Batch type (either 'COPY', 'CHECK', 'COMPARE' or 'DISCOVER')
    p_withInitStep           BOOLEAN,                 -- Boolean indicating whether an INIT step will need to be added to the batch working plan
    p_withEndStep            BOOLEAN                  -- Boolean indicating whether an END step will need to be added to the batch working plan
    )
    RETURNS INTEGER LANGUAGE plpgsql AS
$create_batch$
DECLARE
    v_dbms                   TEXT;
    v_firstBatch             TEXT;
BEGIN
-- Check that no input parameter is NULL.
    IF p_batchName IS NULL OR p_migration IS NULL OR p_batchType IS NULL THEN
        RAISE EXCEPTION 'create_batch: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check that the batch does not exist yet.
    PERFORM 0
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF FOUND THEN
        RAISE EXCEPTION 'create_batch: The batch "%" already exists.', p_batchName;
    END IF;
-- Check the supplied migration and get its DBMS.
    SELECT p_sourceDbms INTO v_dbms FROM @extschema@.check_migration(p_migration);
-- Check the batch type.
    IF p_batchType = 'DISCOVER' AND v_dbms NOT IN ('Oracle', 'Postgres') THEN
        RAISE EXCEPTION 'create_batch: Batch of type DISCOVER are not allowed for % databases.', v_dbms;
    END IF;
    IF p_batchType <> 'COPY' AND p_batchType <> 'CHECK' AND p_batchType <> 'COMPARE' AND p_batchType <> 'DISCOVER' THEN
        RAISE EXCEPTION 'create_batch: Illegal batch type (%). It must be eiter COPY, CHECK, COMPARE or DISCOVER.', p_batchType;
    END IF;
-- Checks are OK.
-- Record the batch into the batch table.
    INSERT INTO @extschema@.batch (bat_name, bat_migration, bat_type, bat_with_init_step, bat_with_end_step)
        VALUES (p_batchName, p_migration, p_batchType, coalesce(p_withInitStep, FALSE), coalesce(p_withEndStep, FALSE));
-- If the batch needs an INIT and/or END steps, add them into the step table.
    IF p_withInitStep THEN
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_sql_function, stp_cost)
            VALUES ('INIT_' || p_migration, p_batchName, 'INIT',
                    CASE p_batchType
                        WHEN 'COPY' THEN '_copy_init'
--                        WHEN 'CHECK' THEN '_check_init'
                        WHEN 'COMPARE' THEN '_compare_init'
                        WHEN 'DISCOVER' THEN '_discover_init'
                    END,
                    1);
    END IF;
    IF p_withEndStep THEN
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_sql_function, stp_cost)
            VALUES ('END_' || p_migration, p_batchName, 'END',
                    CASE p_batchType
                        WHEN 'COPY' THEN '_copy_end'
--                        WHEN 'CHECK' THEN '_check_end'
                        WHEN 'COMPARE' THEN '_compare_end'
                        WHEN 'DISCOVER' THEN '_discover_end'
                    END,
                    1);
    END IF;
--
    RETURN 1;
END;
$create_batch$;

-- The drop_batch() function removes all components linked to a given batch.
-- It returns the number of removed steps.
CREATE FUNCTION drop_batch(
    p_batchName              TEXT
    )
    RETURNS INTEGER LANGUAGE plpgsql AS
$drop_batch$
DECLARE
    v_nbStep                 INT;
BEGIN
-- Delete rows from the step table.
    DELETE FROM @extschema@.step
        WHERE stp_batch_name = p_batchName;
    GET DIAGNOSTICS v_nbStep = ROW_COUNT;
-- Delete the row from the batch table.
    DELETE FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE WARNING 'drop_batch: No batch found with name "%".', p_batchName;
    END IF;
--
    RETURN v_nbStep;
END;
$drop_batch$;

-- The register_tables() function links a set of tables from a single schema to a migration.
-- Two regexp filter tables to include and exclude.
-- A foreign table is created for each table if requested.
-- The schema to hold foreign objects is created if needed.
-- Some characteristics of the table are recorded into the table_to_process, table_index and table_column tables.
CREATE FUNCTION register_tables(
    p_migration              TEXT,               -- The migration linked to the tables
    p_schema                 TEXT,               -- The schema which tables have to be registered into the migration
    p_tablesToInclude        TEXT,               -- Regexp defining the tables to register for the schema
    p_tablesToExclude        TEXT,               -- Regexp defining the tables to exclude (NULL to exclude no table)
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL,
    p_sourceTableNamesFnct   TEXT                -- A function name to use to compute the source table name using the target table name
                             DEFAULT NULL,       -- NULL means both names are equals. May be 'upper', 'lower' or any schema qualified custom function
    p_sourceTableStatLoc     TEXT                -- The data2pg table that contains statistics about these target tables
                             DEFAULT 'source_table_stat',
    p_createForeignTable     BOOLEAN             -- Boolean indicating whether the FOREIGN TABLE have to be created
                             DEFAULT TRUE,       --   (if FALSE, an external operation must create them before launching a migration)
    p_ForeignTableOptions    TEXT                -- A specific directive to apply to the created foreign tables
                             DEFAULT NULL,       --   (it will be appended as is to an ALTER FOREIGN TABLE statement,
                                                 --    it may be "OTPIONS (<key> 'value', ...)" for options at table level,
                                                 --    or "ALTER COLUMN <column> (ADD OPTIONS <key> 'value', ...), ...' for column level options)
    p_separateCreateIndex    BOOLEAN             -- Boolean indicating whether the indexes of these tables have to be created by
                             DEFAULT FALSE,      --   separate steps (to speed-up index rebuild for large tables with a lof of indexes)
    p_sortByPKey             BOOLEAN             -- Boolean indicating whether the source data must be sorted on PKey at migration time
                             DEFAULT FALSE       --   (they are sorted anyway if a clustered index exists)
    )
    RETURNS INTEGER LANGUAGE plpgsql             -- Returns the number of effectively assigned tables
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$register_tables$
DECLARE
    v_serverName             TEXT;
    v_sourceDbms             TEXT;
    v_importSchemaOptClause  TEXT;
    v_sourceSchema           TEXT;
    v_foreignSchema          TEXT;
    v_sourceTable            TEXT;
    v_pgVersion              INT;
    v_tablesArray            TEXT[];
    v_sourceTablesArray      TEXT[];
    v_tablesList             TEXT;
    v_sourceTablesList       TEXT;
    v_tableName              TEXT;
    v_missingTablesList      TEXT;
    v_prevMigration          TEXT;
    v_copySortOrder          TEXT;
    v_anyGenAlwaysIdentCol   BOOLEAN;
    v_referencingTables      TEXT[];
    v_referencedTables       TEXT[];
    v_sourceRows             BIGINT;
    v_sourceKBytes           FLOAT;
    v_pkOid                  OID;
    v_uniqueOid              OID;
    v_copyKeyColNb           SMALLINT[];
    v_compareKeyColNb        SMALLINT[];
    v_nbColInIdx             SMALLINT;
    v_nbColInKey             SMALLINT;
    v_rowsThreshold          CONSTANT BIGINT = 10000;      -- The estimated number of rows limit to drop and recreate indexes and constraints
    r_tbl                    RECORD;
    r_col                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_tables: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check the supplied migration and get its server name and source DBMS.
    SELECT p_serverName, p_sourceDbms, coalesce('OPTIONS (' || p_importSchemaOptions || ')', '')
       INTO v_serverName, v_sourceDbms, v_importSchemaOptClause
       FROM @extschema@.check_migration(p_migration);
-- Check that the schema exists.
    PERFORM @extschema@.check_schema(p_schema);
-- Check the p_ForeignTableOptions parameter.
    IF p_ForeignTableOptions IS NOT NULL THEN
-- Check a simple profile
        IF p_ForeignTableOptions !~* 'OPTIONS\s*\(' THEN
            RAISE EXCEPTION 'register_tables: The p_ForeignTableOptions parameter must be a valid ALTER FOREIGN TABLE clause, with an OPTIONS() clause.';
        END IF;
-- Check the p_ForeignTableOptions and p_createForeignTable consistency.
        IF p_ForeignTableOptions IS NOT NULL AND NOT p_createForeignTable THEN
            RAISE EXCEPTION 'register_tables: Foreign table options cannot be specified if the foreign tables are not to be created.';
        END IF;
    END IF;
-- Create the foreign schema if it does not exist.
    v_foreignSchema = 'srcdb_' || p_schema;
    EXECUTE format(
        'CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION data2pg',
        v_foreignSchema);
-- Compute the schema or user name in the source database.
    v_sourceSchema = coalesce(p_sourceSchema, p_schema);
-- Get the postgres version.
    SELECT current_setting('server_version_num')::INT INTO v_pgVersion;
-- Get the array of tables to process.
    SELECT array_agg(relname ORDER BY relname) INTO v_tablesArray
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE nspname = p_schema
          AND relkind = 'r'
          AND relname ~ p_tablesToInclude
          AND (p_tablesToExclude IS NULL OR relname !~ p_tablesToExclude);
-- Exclude and notice the tables that are already registered
    FOR r_tbl IN
       SELECT tbl_name
           FROM @extschema@.table_to_process
           WHERE tbl_schema = p_schema
             AND tbl_name = ANY(v_tablesArray)
    LOOP
        v_tablesArray = array_remove(v_tablesArray, r_tbl.tbl_name);
        RAISE NOTICE 'register_tables: The table % is already registered to a migration.', r_tbl.tbl_name;
    END LOOP;
-- Stop if no tables are selected.
    IF v_tablesArray IS NULL OR v_tablesArray = '{}' THEN
        RAISE EXCEPTION 'register_tables: No table has been found in the schema "%" using the provided selection criteria.', p_schema;
    END IF;
--
-- Process the selected tables.
--
-- Unless it is not requested, create the foreign tables mapped on the tables in the source database, using an IMPORT FOREIGN SCHEMA statement.
    IF p_createForeignTable THEN
-- Build the source tables list, using the name transformation function, if any.
        IF p_sourceTableNamesFnct IS NULL THEN
            v_sourceTablesArray = v_tablesArray;
        ELSE
            v_sourceTablesArray = '{}';
            FOREACH v_tableName IN ARRAY v_tablesArray
            LOOP
-- There is no previous check on the supplied table names conversion function.
--   If it does not exist, or has not the right parameters set, the next statement fails with a clear enough message.
                EXECUTE format('SELECT %s(%L)', p_sourceTableNamesFnct, v_tableName)
                    INTO v_tableName;
                v_sourceTablesArray = array_append(v_sourceTablesArray, v_tableName);
            END LOOP;
        END IF;
        SELECT string_agg(quote_ident(relname), ',') INTO v_sourceTablesList
            FROM (SELECT unnest(v_sourceTablesArray) AS relname) AS t;
-- Create all foreign tables at once.
        EXECUTE format(
            'IMPORT FOREIGN SCHEMA %s LIMIT TO (%s) FROM SERVER %I INTO %I %s',
            quote_ident(v_sourceSchema), v_sourceTablesList, v_serverName, v_foreignSchema, v_importSchemaOptClause);
-- Check that all expected tables have been found.
        SELECT string_agg(relname, ', ') INTO v_missingTablesList
            FROM (
                SELECT unnest(v_sourceTablesArray) AS relname
                  EXCEPT
                SELECT relname
                    FROM pg_catalog.pg_class
                         JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                    WHERE nspname = v_foreignSchema
                      AND relkind = 'f'
                  ) AS t;
        IF v_missingTablesList IS NOT NULL THEN
            RAISE EXCEPTION 'register_tables: Some tables have not been found in the source database : %.', v_missingTablesList;
        END IF;
    END IF;
-- Next step of the tables processing. Tables are processed one by one.
    FOR r_tbl IN
        SELECT relname, pg_class.oid AS table_oid, pg_namespace.oid AS schema_oid
            FROM pg_catalog.pg_class
                 JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = p_schema
              AND relname = ANY(v_tablesArray)
            ORDER BY relname
    LOOP
-- If the source and target table names are not equal, rename the foreign table to match the target table name
        v_sourceTable = v_sourceTablesArray[array_position(v_tablesArray, r_tbl.relname::text)];
        IF v_sourceTable <> r_tbl.relname THEN
            EXECUTE format(
                'ALTER FOREIGN TABLE %I.%I RENAME TO %I',
                v_foreignSchema, v_sourceTable, r_tbl.relname);
        END IF;
-- Alter the foreign table with the provided options.
        IF p_ForeignTableOptions IS NOT NULL THEN
            EXECUTE format(
                'ALTER FOREIGN TABLE %I.%I %s',
                v_foreignSchema, r_tbl.relname, p_ForeignTableOptions);
        END IF;
-- Get the clustered index columns list, if it exists.
-- This list will be used in an ORDER BY clause of the table copy function.
        SELECT substring(pg_get_indexdef(indexrelid) FROM ' USING .*\((.+)\)') INTO v_copySortOrder
            FROM pg_catalog.pg_index
            WHERE indrelid = r_tbl.table_oid
              AND indisclustered;
-- Are there any columns declared GENERATED ALWAYS AS IDENTITY in the target table?
        IF v_pgVersion < 100000 THEN
            v_anyGenAlwaysIdentCol = FALSE;
        ELSE
            v_anyGenAlwaysIdentCol = EXISTS(
                SELECT 1 FROM pg_catalog.pg_attribute
                    WHERE attrelid = r_tbl.table_oid
                      AND attnum > 0 AND NOT attisdropped
                      AND attidentity = 'a');
        END IF;
-- Build both arrays of tables linked by foreign keys.
--    The tables that are referenced by FK from this table.
        SELECT array_agg(DISTINCT nf.nspname || '.' || tf.relname ORDER BY nf.nspname || '.' || tf.relname) INTO v_referencingTables
          FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace nf, pg_catalog.pg_class tf
          WHERE contype = 'f'                                           -- FK constraints only
            AND c.confrelid = tf.oid AND tf.relnamespace = nf.oid       -- join for referenced table and namespace
            AND c.conrelid  = r_tbl.table_oid;                          -- select the current table
--    The tables that reference this table by their FK.
        SELECT array_agg(DISTINCT n.nspname || '.' || t.relname ORDER BY n.nspname || '.' || t.relname) INTO v_referencedTables
          FROM pg_catalog.pg_constraint c, pg_catalog.pg_namespace n, pg_catalog.pg_class t
          WHERE contype = 'f'                                           -- FK constraints only
            AND c.conrelid  = t.oid  AND t.relnamespace  = n.oid        -- join for table and namespace
            AND c.confrelid = r_tbl.table_oid;                          -- select the current table
-- Get statistics for the table.
-- For Oracle tables, the FDW (with the default "case 'smart'" option of the import foreign schema statement) generates lower case table names
-- while the source tables names are in upper case. So no table name conversion function is required but the source statistics have upper case table names.
        EXECUTE format (
            'SELECT stat_rows, stat_kbytes
                FROM @extschema@.%I
                WHERE stat_schema = %L AND stat_table = %L',
            p_sourceTableStatLoc, v_sourceSchema,
            CASE WHEN v_sourceDbms = 'Oracle' THEN UPPER(v_sourceTable) ELSE v_sourceTable END )
            INTO v_sourceRows, v_sourceKBytes;
-- Register the table into the table_to_process table.
        INSERT INTO @extschema@.table_to_process (
                tbl_schema, tbl_name, tbl_migration, tbl_foreign_schema, tbl_foreign_name, tbl_source_schema, tbl_source_name,
                tbl_rows, tbl_kbytes,
                tbl_copy_sort_order, tbl_some_gen_alw_id_col,
                tbl_referencing_tables, tbl_referenced_tables
            ) VALUES (
                p_schema, r_tbl.relname, p_migration, v_foreignSchema, r_tbl.relname, v_sourceSchema, v_sourceTable,
                coalesce(v_sourceRows, 0), coalesce(v_sourceKBytes, 0),
                v_copySortOrder, v_anyGenAlwaysIdentCol,
                v_referencingTables, v_referencedTables
            );
-- Register the columns of the target table.
--   Look at the indexes associated to the table.
        v_pkOid = NULL;
        v_uniqueOid = NULL;
--   Get the PK index id to be used to sort for a COMPARE step, if it exists.
        SELECT indexrelid INTO v_pkOid
            FROM pg_catalog.pg_index
            WHERE indrelid = r_tbl.table_oid
              AND indisprimary;
--   If there is no PK, get a UNIQUE index id to be used to sort for a COMPARE step, if one exists.
--     If there are several UNIQUE index, the choosen index will be the non partial index with the
--     least number of columns and then the index defined first (i.e. having the lowest oid).
        IF v_pkOid IS NULL THEN
            SELECT indexrelid INTO v_uniqueOid
                FROM pg_catalog.pg_index
                WHERE indrelid = r_tbl.table_oid
                  AND indisunique
                  AND indpred IS NULL
                ORDER BY indnatts, indexrelid
                LIMIT 1;
        END IF;
--   Get the array of columns numbers composing the sort key for COMPARE steps (included columns being ignored).
        IF coalesce(v_pkOid,v_uniqueOid) IS NOT NULL THEN
            SELECT indkey, indnatts, indnkeyatts
                INTO v_compareKeyColNb, v_nbColInIdx, v_nbColInKey
                FROM pg_catalog.pg_index
                WHERE indexrelid = coalesce(v_pkOid, v_uniqueOid);
            IF v_nbColInIdx > v_nbColInKey THEN
--   Remove the included columns, if any (they are at the array end and do not belong to the key).
                FOR i IN v_nbColInKey + 1 .. v_nbColInIdx LOOP
                    v_compareKeyColNb = array_remove(v_compareKeyColNb, v_compareKeyColNb[i]);
                END LOOP;
            END IF;
        ELSE
--   Without PK or UNIQUE index, use all columns.
            SELECT array_agg(attnum ORDER BY attnum)
                INTO v_compareKeyColNb
                FROM pg_catalog.pg_attribute
                WHERE attrelid = r_tbl.table_oid
                  AND attnum > 0 AND NOT attisdropped;
        END IF;
--   Insert into the table_column table
        EXECUTE format(
                 'INSERT INTO @extschema@.table_column'
                 '         (tco_schema, tco_table, tco_name, tco_number, tco_type,'
                 '          tco_copy_source_expr, tco_copy_dest_col,'
                 '          tco_compare_source_expr, tco_compare_dest_expr, tco_compare_sort_rank)'
                 '    SELECT %L, %L, attname, attnum,'
                 '           CASE WHEN typname = ''bpchar'' OR typname = ''varchar'''
                 '                  THEN typname || ''('' || atttypmod::text || '')'''
                 '                WHEN typname = ''numeric'' AND atttypmod <> -1 '
                 '                  THEN typname || ''('' || (((atttypmod - 4) >> 16) & 65535)::text || '','' || ((atttypmod - 4) & 65535)::text || '')'''
                 '                ELSE typname'
                 '           END,'                                                            -- type format
                 '           CASE WHEN %s = '''' THEN quote_ident(attname) ELSE NULL END,'    -- copy source expression
                 '           CASE WHEN %s = '''' THEN quote_ident(attname) ELSE NULL END,'    -- copy destination column
                 '           quote_ident(attname),'                                           -- compare source expression
                 '           quote_ident(attname),'                                           -- compare destination column
                 '           array_position(%L, attnum) + 1'                                  -- compare sort rank
                 '        FROM pg_catalog.pg_attribute'
                 '             JOIN pg_type ON (atttypid = pg_type.oid)'
                 '        WHERE attrelid = %s'
                 '          AND attnum > 0 AND NOT attisdropped'
                 '        ORDER BY attnum',
                 p_schema, r_tbl.relname,
                 CASE WHEN v_pgVersion >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                 CASE WHEN v_pgVersion >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                 v_compareKeyColNb, r_tbl.table_oid);
-- Insert the indexes and constraints into the table_index table.
-- Start with PKEY, UNIQUE or EXCLUDE constraints.
-- Constraints will be dropped and recreated only if there are enough rows to justify it and if they are not linked to another constraint (like a FK).
        INSERT INTO @extschema@.table_index
                (tic_schema, tic_table, tic_object, tic_type, tic_definition,
                 tic_drop_for_copy)
            SELECT p_schema, r_tbl.relname, c1.conname, 'C' || c1.contype, pg_get_constraintdef(c1.oid),
                   coalesce(v_sourceRows >= v_rowsThreshold, TRUE)
                       AND NOT EXISTS(
                                       SELECT 0 FROM pg_catalog.pg_constraint c2
                                       WHERE c2.conindid = c1.conindid AND c2.oid <> c1.oid
                                      )
                FROM pg_catalog.pg_constraint c1
                WHERE c1.conrelid = r_tbl.table_oid
                  AND c1.contype IN ('p', 'u', 'x');
-- Continue with indexes
-- These indexes are not clustered indexes that are not linked to any constraint (pkey or others).
-- Indexes will be dropped and recreated only if there are enough rows to justify it, if it is not a cluster index
--   and if they are not linked to another constraint (like a FK).
        INSERT INTO @extschema@.table_index
                (tic_schema, tic_table, tic_object, tic_type, tic_definition,
                 tic_drop_for_copy)
            SELECT p_schema, r_tbl.relname, relname, 'I', pg_get_indexdef(pg_class.oid),
                   coalesce(v_sourceRows > v_rowsThreshold, TRUE)
                       AND NOT indisclustered
                       AND NOT EXISTS(
                                       SELECT 0 FROM pg_catalog.pg_constraint
                                       WHERE pg_constraint.conindid = pg_class.oid
                                      )
                FROM pg_catalog.pg_index
                     JOIN pg_catalog.pg_class ON (pg_class.oid = indexrelid)
                WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
            ON CONFLICT DO NOTHING;
-- For dropable indexes/constraints, set the tic_separate_creation_step to TRUE if requested.
        IF p_separateCreateIndex THEN
            UPDATE @extschema@.table_index
                SET tic_separate_creation_step = TRUE
                WHERE tic_schema = p_schema
                  AND tic_table = r_tbl.relname
                  AND tic_drop_for_copy;
        END IF;
--    Update the global statistics on pg_class (reltuples and relpages) for the just created foreign table.
--    This will let the optimizer choose a proper plan for the _compare_table() function, without the cost of an ANALYZE.
        IF v_sourceRows IS NOT NULL AND v_sourceKBytes IS NOT NULL THEN
            UPDATE pg_catalog.pg_class
                SET reltuples = coalesce(v_sourceRows, 0), relpages = coalesce(v_sourceKBytes / 8, 0)
                WHERE oid = (quote_ident(v_foreignSchema) || '.' || quote_ident(r_tbl.relname))::regclass;
        END IF;
    END LOOP;
--
    RETURN cardinality(v_tablesArray);
END;
$register_tables$;

-- The register_table() function links a single table to a migration.
-- It is just a wrapper over the register_tables() function.
CREATE FUNCTION register_table(
    p_migration              TEXT,               -- The migration linked to the tables
    p_schema                 TEXT,               -- The schema holding the table to register
    p_table                  TEXT,               -- The table name to register
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL,
    p_sourceTableNamesFnct   TEXT                -- A function name to use to compute the source table name using the target table name
                             DEFAULT NULL,       -- NULL means both names are equals. May be 'upper', 'lower' or any schema qualified custom function
    p_sourceTableStatLoc     TEXT                -- The data2pg table that contains statistics about these target tables
                             DEFAULT 'source_table_stat',
    p_createForeignTable     BOOLEAN             -- Boolean indicating whether the FOREIGN TABLE have to be created
                             DEFAULT TRUE,       --   (if FALSE, an external operation must create them before launching a migration)
    p_ForeignTableOptions    TEXT                -- A specific directive to apply to the created foreign tables
                             DEFAULT NULL,       --   (it will be appended as is to an ALTER FOREIGN TABLE statement,
                                                 --    it may be "OTPIONS (<key> 'value', ...)" for options at table level,
                                                 --    or "ALTER COLUMN <column> (ADD OPTIONS <key> 'value', ...), ...' for column level options)
    p_separateCreateIndex    BOOLEAN             -- Boolean indicating whether the indexes of these tables have to be created by
                             DEFAULT FALSE,      --   separate steps (to speed-up index rebuild for large tables with a lof of indexes)
    p_sortByPKey             BOOLEAN             -- Boolean indicating whether the source data must be sorted on PKey at migration time
                             DEFAULT FALSE       --   (they are sorted anyway if a clustered index exists)
    )
    RETURNS INTEGER LANGUAGE sql                 -- Returns the number of effectively assigned tables
    AS
$register_table$
    SELECT @extschema@.register_tables(p_migration, p_schema, '^' || p_table || '$', NULL, p_sourceSchema, p_sourceTableNamesFnct,
                                       p_sourceTableStatLoc, p_createForeignTable, p_ForeignTableOptions, p_separateCreateIndex, p_sortByPKey);
$register_table$;

-- The register_column_transform_rule() functions defines a column change from the source table to the destination table.
-- It allows to manage columns with different names, with different types and or with specific computation rule.
-- The target column is defined with the schema, table and column name.
-- It returns the number of registered column transformations, i.e. 1.
-- Several transformation rule may be applied for the same column. In this case, the last one defines the real transformation that will be applied.
CREATE FUNCTION register_column_transform_rule(
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_column                 TEXT,               -- The column name as it would appear in the INSERT SELECT statement of the copy processing
    p_expression             TEXT                -- The column name as it will appear in the INSERT SELECT statement of the copy processing. It may be
                                                 --   another column name if the column is renamed or an expression if the column content requires a
                                                 --   transformation rule. The column name may need to be double-quoted.
    )
    RETURNS INT LANGUAGE plpgsql AS
$register_column_transform_rule$
DECLARE
    v_migrationName          TEXT;
BEGIN
-- Check that no parameter is not NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_column IS NULL OR p_expression IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: None of the input parameters can be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name.
    SELECT tbl_migration INTO v_migrationName
        FROM @extschema@.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_transform_rule: Table %.% not found.', p_schema, p_table;
    END IF;
-- Check that a comparison rule has not already been registered for the column.
    PERFORM 0
        FROM @extschema@.table_column
        WHERE tco_schema = p_schema AND tco_table = p_table AND tco_name = p_column
          AND (tco_compare_source_expr IS NULL OR tco_copy_source_expr <> tco_compare_source_expr);
    IF FOUND THEN
        RAISE EXCEPTION 'register_column_transform_rule: It looks like a comparison rule has already been registerd for the column %.%.%.', p_schema, p_table, p_column;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE @extschema@.migration
        SET mgr_config_completed = FALSE
        WHERE mgr_name = v_migrationName
          AND mgr_config_completed;
-- Record the change into the table_column table.
    UPDATE @extschema@.table_column
        SET tco_copy_source_expr = p_expression,
            tco_compare_source_expr = p_expression
        WHERE tco_schema = p_schema AND tco_table = p_table AND tco_name = p_column;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_transform_rule: The column % is not found in the list of columns to copy.', p_column;
    END IF;
    RETURN 1;
END;
$register_column_transform_rule$;

-- The register_column_comparison_rule() functions defines a specific rule in comparing a column between the source and the target databases.
-- It allows to either simply mask the column for the COMPARE operation, or to compare the result of an expression on both source and tardet tables.
-- The target column is defined with the schema, table and column name.
-- The expression on the source and the target databases may be different.
-- It returns the number of registered column transformations, i.e. 1.
-- Several comparison rules may be applied for the same column. In this case, the last one defines the real way to compare the column.
CREATE FUNCTION register_column_comparison_rule(
    p_schema                 TEXT,               -- The schema name of the target table
    p_table                  TEXT,               -- The target table name
    p_column                 TEXT,               -- The target column name
    p_sourceExpression       TEXT DEFAULT NULL,  -- The expression to use on the source table for the comparison operation ; a NULL simply masks the column
    p_targetExpression       TEXT DEFAULT NULL   -- The expression to use on the target table for the comparison operation ; equals p_sourceExpression when NULL
    )
    RETURNS INT LANGUAGE plpgsql AS
$register_column_comparison_rule$
DECLARE
    v_migrationName          TEXT;
    v_sortRank               INT;
BEGIN
-- Check that no required parameter is NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_column IS NULL THEN
        RAISE EXCEPTION 'register_column_comparison_rule: None of the 3 first input parameters can be NULL.';
    END IF;
-- Check that the source expression is not NULL when the destination expression is also NOT NULL.
    IF p_sourceExpression IS NULL AND p_targetExpression IS NOT NULL THEN
        RAISE EXCEPTION 'register_column_comparison_rule: If p_sourceExpression is NULL, p_targetExpression must also be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name.
    SELECT tbl_migration INTO v_migrationName
        FROM @extschema@.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_comparison_rule: Table %.% not found.', p_schema, p_table;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE @extschema@.migration
        SET mgr_config_completed = FALSE
        WHERE mgr_name = v_migrationName
          AND mgr_config_completed;
-- Record the change into the table_column table.
    UPDATE @extschema@.table_column
       SET tco_compare_source_expr = p_sourceExpression,
           tco_compare_dest_expr = coalesce(p_targetExpression, p_sourceExpression)
       WHERE tco_schema = p_schema AND tco_table = p_table AND tco_name = p_column
       RETURNING tco_compare_sort_rank INTO v_sortRank;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_comparison_rule: The column % is not found in the list of columns to compare.', p_column;
    END IF;
    IF v_sortRank IS NOT NULL THEN
        RAISE WARNING 'register_column_comparison_rule: The column % is used in the sorts at tables comparison time. Applying a comparison rule may have a side effect.', p_column;
    END IF;
    RETURN 1;
END;
$register_column_comparison_rule$;

-- The register_table_part() function defines a table's subset that will be processed by its own migration step.
-- A table part step includes 1 or 2 of the usual 3 elementary actions of a table copy:
--   - pre-processing: dropping indexes, truncating tables, depending on the context
--   - copy processing
--   - post-processing: creating indexes, ...
-- Splitting the work like this allows to either parallelize a single table copy, and/or anticipate the copy of a table subset, and/or limit the rows to copy.
-- The related table must have been registered yet.
-- Some characteristics of the table part are recorded into the table_part table.
CREATE FUNCTION register_table_part(
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_partNum                INTEGER,            -- The part number, which is unique for a table. But part numbers are not necessarily in sequence
    p_condition              TEXT,               -- The condition that will filter the rows to copy at migration time. NULL if no row to copy
    p_isFirstPart            BOOLEAN             -- Boolean indicating that the part is the first one for the table
                             DEFAULT FALSE,      --   (if TRUE, the pre-processing action is performed)
    p_isLastPart             BOOLEAN             -- Boolean indicating that the part is the last one for the table
                             DEFAULT FALSE       --   (if TRUE, the post-processing action is performed)
    )
    RETURNS INT LANGUAGE plpgsql AS              -- Returns the number of effectively assigned table part, i.e. 1
$register_table_part$
DECLARE
    v_migrationName          TEXT;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_partNum IS NULL THEN
        RAISE EXCEPTION 'register_table_part: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name.
    SELECT tbl_migration INTO v_migrationName
        FROM @extschema@.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_table_part: Table %.% not found.', p_schema, p_table;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE @extschema@.migration
        SET mgr_config_completed = FALSE
        WHERE mgr_name = v_migrationName
          AND mgr_config_completed;
-- Check that the table part doesn't exist yet.
    PERFORM 0
        FROM @extschema@.table_part
        WHERE prt_schema = p_schema AND prt_table = p_table AND prt_number = p_partNum;
    IF FOUND THEN
        RAISE EXCEPTION 'register_table_part: A part % already exists for the table %.%.',
                        p_partNum, p_schema, p_table;
    END IF;
-- Check that at least an action will be executed for this part.
    IF p_condition IS NULL AND NOT p_isFirstPart AND NOT p_isLastPart THEN
        RAISE EXCEPTION 'register_table_part: these parameters combination would lead to nothing to do for this table part.';
    END IF;
-- Register the table part into the ... table_part table.
    INSERT INTO @extschema@.table_part (
            prt_schema, prt_table, prt_number, prt_condition, prt_is_first_step, prt_is_last_step
        ) VALUES (
            p_schema, p_table, p_partNum, p_condition, p_isFirstPart, p_isLastPart
        );
--
    RETURN 1;
END;
$register_table_part$;

-- The register_sequences() function links a set of sequences from a single schema to a migration.
-- Two regexp filter sequences to include and exclude.
-- A foreign table is created for each sequence. It will contain the useful characteristics of the sequence.
-- A schema to hold foreign objects is created if needed.
CREATE FUNCTION register_sequences(
    p_migration              TEXT,               -- The migration linked to the sequences
    p_schema                 TEXT,               -- The schema which sequences have to be registered to the migration
    p_sequencesToInclude     TEXT,               -- Regexp defining the sequences to register for the schema
    p_sequencesToExclude     TEXT,               -- Regexp defining the sequences to exclude (NULL to exclude no sequence)
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL,
    p_sourceSequenceNamesFnct TEXT               -- The sequence name transfomation function in the source database.
                             DEFAULT NULL        -- NULL means both names are equals. May be 'upper', 'lower' or any schema qualified custom function
    )
    RETURNS INTEGER LANGUAGE plpgsql             -- Returns the number of effectively registered sequences
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$register_sequences$
DECLARE
    v_serverName             TEXT;
    v_sourceDbms             TEXT;
    v_sourceSchema           TEXT;
    v_sourceSequenceName     TEXT;
    v_sequencesArray         TEXT[];
    v_sequencesList          TEXT;
    v_foreignSchema          TEXT;
    v_prevMigration          TEXT;
    r_seq                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_sequencesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_sequences: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check the supplied migration and get its server name and source DBMS.
    SELECT p_serverName, p_sourceDbms INTO v_serverName, v_sourceDbms FROM @extschema@.check_migration(p_migration);
-- Check that the schema exists.
    PERFORM @extschema@.check_schema(p_schema);
-- Create the foreign schema if it does not exist.
    v_foreignSchema = 'srcdb_' || p_schema;
    EXECUTE format(
        'CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION data2pg',
        v_foreignSchema);
-- Get the array of sequences to process.
    SELECT array_agg(relname ORDER BY relname) INTO v_sequencesArray
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE nspname = p_schema
              AND relkind = 'S'
              AND relname ~ p_sequencesToInclude
              AND (p_sequencesToExclude IS NULL OR relname !~ p_sequencesToExclude);
-- Exclude and notice the sequences that are already registered
    FOR r_seq IN
       SELECT seq_name
           FROM @extschema@.sequence_to_process
           WHERE seq_schema = p_schema
             AND seq_name = ANY(v_sequencesArray)
    LOOP
        v_sequencesArray = array_remove(v_sequencesArray, r_seq.seq_name);
        RAISE NOTICE 'register_sequences: The sequence % is already assigned to a migration.', r_seq.seq_name;
    END LOOP;
    IF v_sequencesArray IS NULL OR v_sequencesArray = '{}' THEN
        RAISE EXCEPTION 'register_sequences: No sequence has been found in the schema "%" using the provided selection criteria.', p_schema;
    END IF;
-- Process the selected sequences.
    v_sourceSchema = coalesce(p_sourceSchema, p_schema);
-- Get each selected sequence.
    FOR r_seq IN
        SELECT relname
            FROM pg_catalog.pg_class
                 JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = p_schema
              AND relname = ANY(v_sequencesArray)
            ORDER BY relname
    LOOP
-- Compute the sequence name on the source database.
        v_sourceSequenceName = CASE WHEN v_sourceDbms = 'Oracle' THEN upper(r_seq.relname) ELSE r_seq.relname END;
        IF p_sourceSequenceNamesFnct IS NOT NULL THEN
-- There is no previous check on the supplied table names conversion function.
--   If it does not exist, or has not the right parameters set, the next statement fails with a clear enough message.
            EXECUTE format('SELECT %s(%L)', p_sourceSequenceNamesFnct, v_sourceSequenceName)
                INTO v_sourceSequenceName;
        END IF;
-- Check that the sequence exists in the source database.
        IF v_sourceDbms = 'Oracle' THEN
            PERFORM 0
                FROM @extschema@.ora_sequences
                WHERE sequence_owner = v_sourceSchema
                  AND sequence_name = v_sourceSequenceName;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'register_sequences: The sequence "%.%" has not been found in the source database.', p_schema, v_sourceSequenceName;
            END IF;
        ELSIF v_sourceDbms = 'PostgreSQL' THEN
-- For PostgreSQL source database only,
-- Create the foreign table mapped on the sequence in the source database to get its current value.
            EXECUTE format(
                'CREATE FOREIGN TABLE %I.%I (last_value BIGINT, is_called BOOLEAN)'
                '    SERVER %I OPTIONS (schema_name %L, table_name %L)',
                v_foreignSchema, r_seq.relname, v_serverName, v_sourceSchema, v_sourceSequenceName);
            EXECUTE format(
                'ALTER FOREIGN TABLE %I.%I OWNER TO data2pg',
                v_foreignSchema, r_seq.relname);
-- Check that the sequence exists, by accessing the foreign table.
            BEGIN
                EXECUTE format(
                    'SELECT count(*) FROM %I.%I',
                    v_foreignSchema, r_seq.relname);
            EXCEPTION WHEN OTHERS THEN
                RAISE EXCEPTION 'register_sequences: The sequence "%.%" has not been found in the source database.', p_schema, v_sourceSequenceName;
            END;
        END IF;
-- Register the sequence into the sequence_to_process table.
        INSERT INTO @extschema@.sequence_to_process (
                seq_schema, seq_name, seq_migration, seq_foreign_schema, seq_foreign_name, seq_source_schema, seq_source_name
            ) VALUES (
                p_schema, r_seq.relname, p_migration, v_foreignSchema, r_seq.relname, v_sourceSchema, v_sourceSequenceName
            );
    END LOOP;
--
    RETURN cardinality(v_sequencesArray);
END;
$register_sequences$;

-- The register_sequence() function links a single sequence to a migration.
-- It is just a wrapper over the register_sequences() function.
CREATE FUNCTION register_sequence(
    p_migration              TEXT,               -- The migration linked to the sequences
    p_schema                 TEXT,               -- The schema holding the sequence to register
    p_sequence               TEXT,               -- The sequence name to register for the schema
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL,
    p_sourceSequenceNamesFnct TEXT               -- The sequence name transfomation function in the source database.
                             DEFAULT NULL        -- NULL means both names are equals. May be 'upper', 'lower' or any schema qualified custom function
    )
    RETURNS INTEGER LANGUAGE sql                 -- Returns the number of effectively registered sequences
    AS
$register_sequence$
    SELECT @extschema@.register_sequences(p_migration, p_schema, '^' || p_sequence || '$', NULL, p_sourceSchema, p_sourceSequenceNamesFnct);
$register_sequence$;

-- The assign_tables_to_batch() function assigns a set of tables of a single schema to a batch.
-- Two regexp filter tables already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_tables_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema which tables have to be assigned to the batch
    p_tablesToInclude        TEXT,               -- Regexp defining the tables to assign for the schema
    p_tablesToExclude        TEXT                -- Regexp defining the tables to exclude (NULL to exclude no table)
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- Returns the number of effectively assigned tables
$assign_tables_to_batch$
DECLARE
    v_migrationName          TEXT;
    v_batchType              TEXT;
    v_nbTables               INT;
    v_prevBatchName          TEXT;
    r_tbl                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'assign_tables_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists, get its migration name and set the migration as in-progress.
    SELECT p_migrationName, p_batchType INTO v_migrationName, v_batchType FROM @extschema@.check_batch(p_batchName);
-- Get the selected tables.
    v_nbTables = 0;
    FOR r_tbl IN
        SELECT tbl_name, tbl_rows, tbl_kbytes, tbl_referencing_tables
            FROM @extschema@.table_to_process
            WHERE tbl_migration = v_migrationName
              AND tbl_schema = p_schema
              AND tbl_name ~ p_tablesToInclude
              AND (p_tablesToExclude IS NULL OR tbl_name !~ p_tablesToExclude)
    LOOP
        v_nbTables = v_nbTables + 1;
        IF v_batchType = 'COPY' THEN
-- For batches of type COPY, check that the table is not already fully assigned to another batch of type COPY.
            SELECT stp_batch_name INTO v_prevBatchName
                FROM @extschema@.step
                     JOIN @extschema@.batch ON (bat_name = stp_batch_name)
                WHERE stp_name = p_schema || '.' || r_tbl.tbl_name
                  AND bat_type = 'COPY';
            IF FOUND THEN
                RAISE EXCEPTION 'assign_tables_to_batch: The table %.% is already assigned to the batch "%".',
                                p_schema, r_tbl.tbl_name, v_prevBatchName;
            END IF;
-- ... And check that the table has no table part already assigned to any batch of the same type.
            PERFORM 0
                FROM @extschema@.step
                     JOIN @extschema@.batch ON (bat_name = stp_batch_name)
                WHERE stp_name LIKE p_schema || '.' || r_tbl.tbl_name || '.%'
                  AND bat_type = 'COPY';
            IF FOUND THEN
                RAISE EXCEPTION 'assign_tables_to_batch: The table %.% has at least 1 part already assigned to a batch of type COPY.',
                                p_schema, r_tbl.tbl_name;
            END IF;
        ELSIF v_batchType = 'COMPARE' THEN
-- For batches of type COMPARE, raise a warning if the table has neither PK nor unique index.
            PERFORM 0
                FROM @extschema@.table_index
                WHERE tic_schema = p_schema
                  AND tic_table = r_tbl.tbl_name
                  AND tic_type IN ('Cp', 'Cu');
            IF NOT FOUND THEN
                RAISE WARNING 'assign_tables_to_batch: The table %.% has neither PK nor UNIQUE index.'
                              ' All columns are used as sort key, without being sure that there will not be duplicates',
                              p_schema, r_tbl.tbl_name;
            END IF;
        END IF;
-- Record the table into the step table.
        INSERT INTO @extschema@.step (
                stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                stp_sql_function, stp_cost
            ) VALUES (
                p_schema || '.' || r_tbl.tbl_name, p_batchName, 'TABLE', p_schema, r_tbl.tbl_name,
                CASE v_batchType
                    WHEN 'COPY' THEN '_copy_table'
                    WHEN 'CHECK' THEN '_check_table'
                    WHEN 'COMPARE' THEN '_compare_table'
                    WHEN 'DISCOVER' THEN '_discover_table'
                END,
                r_tbl.tbl_kbytes
            );
    END LOOP;
-- Check that at least 1 table has been assigned.
    IF v_nbTables = 0 THEN
        RAISE EXCEPTION 'assign_tables_to_batch: No tables have been selected.';
    END IF;
--
    RETURN v_nbTables;
END;
$assign_tables_to_batch$;

-- The assign_table_to_batch() function assigns a single table to a batch.
-- It is a simple wrapper over the assign_tables_to_batch() function.
CREATE FUNCTION assign_table_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema holding the table to assign
    p_table                  TEXT                -- The table to assign
    )
    RETURNS INTEGER LANGUAGE sql AS              -- Returns the number of effectively assigned tables.
$assign_table_to_batch$
    SELECT @extschema@.assign_tables_to_batch(p_batchName, p_schema, '^' || p_table || '$', NULL);
$assign_table_to_batch$;

-- The assign_table_part_to_batch() function assigns a table's part to a batch.
-- Two regexp filter tables already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_table_part_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_partNum                INTEGER             -- The part number to assign
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- Returns the number of effectively assigned table part, ie. 1
$assign_table_part_to_batch$
DECLARE
    v_batchType              TEXT;
    v_rows                   BIGINT;
    v_kbytes                 FLOAT;
    v_prevBatchName          TEXT;
BEGIN
-- Check that all parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_table IS NULL OR p_partNum IS NULL THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: No input parameter can be NULL.';
    END IF;
-- Check that the batch exists, get its type and set its migration as in-progress.
    SELECT p_batchType INTO v_batchType FROM @extschema@.check_batch(p_batchName);
-- Check that the batch type allows table parts assignments.
    IF v_batchType = 'DISCOVER' THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: a table part cannot be assigned to a batch of type %.', v_batchType;
    END IF;
-- Check that the table part has been registered and get the table statistics.
    SELECT tbl_rows, tbl_kbytes INTO v_rows, v_kbytes
        FROM @extschema@.table_part
             JOIN @extschema@.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
        WHERE prt_schema = p_schema AND prt_table = p_table AND prt_number = p_partNum;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% has not been registered.',
                        p_partNum, p_schema, p_table;
    END IF;
    IF v_batchType = 'COPY' THEN
-- For batches of type COPY, check if the table part has been already assigned.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM @extschema@.step
                 JOIN @extschema@.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || p_table || '.' || p_partNum
              AND bat_type = 'COPY';
        IF FOUND THEN
            IF v_prevBatchName <> p_batchName THEN
                RAISE WARNING 'assign_table_part_to_batch: The part % of the table %.% is already assigned to the batch %.',
                                p_partNum, p_schema, p_table, v_prevBatchName;
            ELSE
                RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% is already assigned to this batch.',
                                p_partNum, p_schema, p_table;
            END IF;
        END IF;
-- ... and check if the table is already fully assigned to a batch of the same type.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM @extschema@.step
                 JOIN @extschema@.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || p_table
              AND bat_type = 'COPY';
        IF FOUND THEN
            IF v_prevBatchName <> p_batchName THEN
                RAISE WARNING 'assign_table_part_to_batch: The table %.% is already assigned to the batch "%".',
                                p_schema, p_table, v_prevBatchName;
            ELSE
                RAISE EXCEPTION 'assign_table_part_to_batch: The table %.% is already assigned to this batch.',
                                p_schema, p_table;
            END IF;
        END IF;
    END IF;
-- Record the table part into the step table.
    INSERT INTO @extschema@.step (
            stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_part_num,
            stp_sql_function, stp_cost
        ) VALUES (
            p_schema || '.' || p_table || '.' || p_partNum, p_batchName, 'TABLE_PART', p_schema, p_table, p_partNum,
            CASE v_batchType
                WHEN 'COPY' THEN '_copy_table'
                WHEN 'CHECK' THEN '_check_table'
                WHEN 'COMPARE' THEN '_compare_table'
            END,
            v_kbytes
        );
--
    RETURN 1;
END;
$assign_table_part_to_batch$;

-- The assign_index_to_batch() function assigns an index re-creation to a batch.
CREATE FUNCTION assign_index_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_object                 TEXT                -- The index or constraint to assign
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- returns the number of effectively assigned indexes, ie 1
$assign_index_to_batch$
DECLARE
    v_batchType              TEXT;
    v_separateCreateIndex    BOOLEAN;
    v_kbytes                 FLOAT;
    v_prevBatchName          TEXT;
BEGIN
-- Check that all parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_table IS NULL OR p_object IS NULL THEN
        RAISE EXCEPTION 'assign_index_to_batch: No input parameter can be NULL.';
    END IF;
-- Check that the batch exists, get its type and set its migration as in-progress.
    SELECT p_batchType INTO v_batchType FROM @extschema@.check_batch(p_batchName);
-- Check that the batch type allows index creations.
    IF v_batchType <> 'COPY' THEN
        RAISE EXCEPTION 'assign_index_to_batch: an index creation cannot be assigned to a batch of type %.', v_batchType;
    END IF;
-- Check that the index belongs to a registered table and has been set as to be created in a separate step and get the table statistics.
    SELECT tic_separate_creation_step, tbl_kbytes INTO v_separateCreateIndex, v_kbytes
        FROM @extschema@.table_index
             JOIN @extschema@.table_to_process ON (tbl_schema = tic_schema AND tbl_name = tic_table)
        WHERE tic_schema = p_schema AND tic_table = p_table AND tic_object = p_object;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_index_to_batch: The index % of the table %.% is not known or the table is not registered.',
                        p_object, p_schema, p_table;
    END IF;
    IF NOT v_separateCreateIndex THEN
        RAISE EXCEPTION 'assign_index_to_batch: The index % of the table %.% has not been set as to be created by a separate step.',
                        p_object, p_schema, p_table;
    END IF;
-- Warn if the index has been already assigned to another batch.
    SELECT stp_batch_name INTO v_prevBatchName
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_name = p_schema || '.' || p_table || '.' || p_object
          AND bat_type = 'COPY';
    IF FOUND THEN
        RAISE WARNING 'assign_index_to_batch: The index % creation of the table %.% is already assigned to the batch %.',
                        p_object, p_schema, p_table, v_prevBatchName;
    END IF;
-- Record the index creation into the step table.
    INSERT INTO @extschema@.step (
            stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_sub_object,
            stp_sql_function, stp_cost
        ) VALUES (
            p_schema || '.' || p_table || '.' || p_object, p_batchName, 'INDEX', p_schema, p_table, p_object,
            '_create_index', v_kbytes
        );
--
    RETURN 1;
END;
$assign_index_to_batch$;

-- The assign_tables_checks_to_batch() function assigns a set of table checks of a single schema to a batch.
-- Two regexp filter tables already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_tables_checks_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema which tables have to be assigned to the batch
    p_tablesToInclude        TEXT,               -- Regexp defining the tables to assign for the schema
    p_tablesToExclude        TEXT                -- Regexp defining the tables to exclude (NULL to exclude no table)
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- Returns the number of effectively assigned table checks
$assign_tables_checks_to_batch$
DECLARE
    v_migrationName          TEXT;
    v_batchType              TEXT;
    v_nbTables               INT;
    v_prevBatchName          TEXT;
    r_tbl                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'assign_tables_checks_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists, get its migration name and set the migration as in-progress.
    SELECT p_migrationName, p_batchType INTO v_migrationName, v_batchType FROM @extschema@.check_batch(p_batchName);
-- Check that the batch is of type COPY
    IF v_batchType <> 'COPY' THEN
        RAISE EXCEPTION 'assign_tables_checks_to_batch: The batch % is not of type COPY.', p_batchName;
    END IF;
-- Record the tables into the step table.
    INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_sql_function, stp_cost)
        SELECT p_schema || '.' || tbl_name || '.' || '_check', p_batchName, 'CHECK', p_schema, tbl_name, '_check_table', tbl_kbytes
            FROM @extschema@.table_to_process
            WHERE tbl_migration = v_migrationName
              AND tbl_schema = p_schema
              AND tbl_name ~ p_tablesToInclude
              AND (p_tablesToExclude IS NULL OR tbl_name !~ p_tablesToExclude);
    GET DIAGNOSTICS v_nbTables = ROW_COUNT;
-- Check that at least 1 table has been assigned.
    IF v_nbTables = 0 THEN
        RAISE EXCEPTION 'assign_tables_checks_to_batch: No tables have been selected.';
    END IF;
--
    RETURN v_nbTables;
END;
$assign_tables_checks_to_batch$;

-- The assign_table_checks_to_batch() function assigns checks for a single table to a batch.
-- It is a simple wrapper over the assign_tables_checks_to_batch() function.
CREATE FUNCTION assign_table_checks_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema holding the table to assign
    p_table                  TEXT                -- The table to assign
    )
    RETURNS INTEGER LANGUAGE sql AS              -- Returns the number of effectively assigned tables.
$assign_table_to_batch$
    SELECT @extschema@.assign_tables_checks_to_batch(p_batchName, p_schema, '^' || p_table || '$', NULL);
$assign_table_to_batch$;

-- The assign_sequences_to_batch() function assigns a set of sequences of a single schema to a batch
-- Two regexp filter sequences already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_sequences_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema which sequences have to be assigned to the batch
    p_sequencesToInclude     TEXT,               -- Regexp defining the sequences to assign for the schema
    p_sequencesToExclude     TEXT                -- Regexp defining the sequences to exclude (NULL to exclude no sequence)
    )
    RETURNS INT LANGUAGE plpgsql AS              -- returns the number of effectively assigned sequences
$assign_sequences_to_batch$
DECLARE
    v_migrationName          TEXT;
    v_batchType              TEXT;
    v_nbSequences            INT;
    v_prevBatchName          TEXT;
    r_seq                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_sequencesToInclude IS NULL THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists, get its migration name and set the migration as in-progress.
    SELECT p_migrationName, p_batchType INTO v_migrationName, v_batchType FROM @extschema@.check_batch(p_batchName);
-- Check that the batch is not of type 'CHECK' OR 'DISCOVER'.
    IF v_batchType = 'CHECK' OR v_batchType = 'DISCOVER' THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: sequences cannot be assigned to a batch of type %.', v_batchType;
    END IF;
-- Get the selected sequences.
    v_nbSequences = 0;
    FOR r_seq IN
        SELECT seq_name
            FROM @extschema@.sequence_to_process
            WHERE seq_migration = v_migrationName
              AND seq_schema = p_schema
              AND seq_name ~ p_sequencesToInclude
              AND (p_sequencesToExclude IS NULL OR seq_name !~ p_sequencesToExclude)
    LOOP
        v_nbSequences = v_nbSequences + 1;
        IF v_batchType = 'COPY' THEN
-- For batches of type COPY, check that the sequence is not already assigned to a batch of the same type.
            SELECT stp_batch_name INTO v_prevBatchName
                FROM @extschema@.step
                     JOIN @extschema@.batch ON (bat_name = stp_batch_name)
                WHERE stp_name = p_schema || '.' || r_seq.seq_name
                  AND bat_type = 'COPY';
            IF FOUND THEN
                RAISE EXCEPTION 'assign_sequences_to_batch: The sequence %.% is already assigned to the batch %.',
                                p_schema, r_seq.seq_name, v_prevBatchName;
            END IF;
        END IF;
-- Record the sequence into the step table.
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_sql_function, stp_cost)
            VALUES (
                p_schema || '.' || r_seq.seq_name, p_batchName, 'SEQUENCE', p_schema, r_seq.seq_name,
                CASE v_batchType
                    WHEN 'COPY' THEN '_copy_sequence'
                    WHEN 'COMPARE' THEN '_compare_sequence'
                END,
                10);
    END LOOP;
--
    RETURN v_nbSequences;
END;
$assign_sequences_to_batch$;

-- The assign_sequence_to_batch() function assigns a single sequence to a batch.
-- It is a simple wrapper over the assign_sequences_to_batch() function.
CREATE FUNCTION assign_sequence_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema holding the sequence to assign
    p_sequence               TEXT                -- The sequence name
    )
    RETURNS INTEGER LANGUAGE sql AS              -- Returns the number of effectively assigned sequences.
$assign_sequence_to_batch$
    SELECT @extschema@.assign_sequences_to_batch(p_batchName, p_schema, '^' || p_sequence || '$', NULL);
$assign_sequence_to_batch$;

-- The assign_fkey_checks_to_batch() function assigns checks on one or all foreign keys of a table to a batch.
CREATE FUNCTION assign_fkey_checks_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema of the table with fkeys to check
    p_table                  TEXT,               -- The table with fkeys to check
    p_fkey                   TEXT                -- Foreign key name, NULL to check all fkeys of the table
                             DEFAULT NULL
    )
    RETURNS INT LANGUAGE plpgsql AS              -- returns the number of effectively assigned fkey check steps
$assign_fkey_checks_to_batch$
DECLARE
    v_batchType              TEXT;
    v_tableKbytes            FLOAT;
    v_refTableKbytes         FLOAT;
    v_nbFKey                 INT;
    v_prevBatchName          TEXT;
    v_cost                   BIGINT;
    r_fk                     RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_table IS NULL THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists and set its migration as in-progress.
    SELECT p_batchType INTO v_batchType FROM @extschema@.check_batch(p_batchName);
-- Check that the batch is of type COPY.
    IF v_batchType <> 'COPY' THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: batch "%" is of type %. FK ckecks can only be assigned to batches of type COPY.', p_batchName, v_batchType;
    END IF;
-- Check that the table exists (It may not been registered into the migration).
    PERFORM 0
        FROM pg_catalog.pg_class
             JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
        WHERE nspname = p_schema
          AND relname = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: table %.% not found.', p_schema, p_table;
    END IF;
-- Get the table size, if it has been registered.
    v_tableKbytes = 0;
    SELECT tbl_kbytes INTO v_tableKbytes
        FROM @extschema@.table_to_process
        WHERE tbl_schema = p_schema
          AND tbl_name = p_table;
-- Get the fkeys.
    v_nbFKey = 0;
    FOR r_fk IN
        SELECT conname, n2.nspname, c2.relname
            FROM pg_catalog.pg_constraint
                 JOIN pg_catalog.pg_class c ON (c.oid = conrelid)
                 JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
                 JOIN pg_catalog.pg_class c2 ON (c2.oid = confrelid)
                 JOIN pg_catalog.pg_namespace n2 ON (n2.oid = c2.relnamespace)
            WHERE n.nspname = p_schema
              AND c.relname = p_table
              AND contype = 'f'
              AND (p_fkey IS NULL OR conname = p_fkey)
    LOOP
        v_nbFKey = v_nbFKey + 1;
-- Check that the fkey is not already assigned.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM @extschema@.step
            WHERE stp_name = p_schema || '.' || p_table || '.' || r_fk.conname;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_fkey_checks_to_batch: The fkey % of table %.% is already assigned to the batch %.',
                            r_fk.conname, p_schema, p_table, v_prevBatchName;
        END IF;
-- Get the table size for the referenced table, if registered.
        v_refTableKbytes = 0;
        SELECT tbl_kbytes INTO v_refTableKbytes
            FROM @extschema@.table_to_process
            WHERE tbl_schema = r_fk.nspname
              AND tbl_name = r_fk.relname;
-- Compute the global cost and check that at least one of both tables are registered.
        v_cost = (v_tableKbytes + v_refTableKbytes);
        IF v_cost = 0 THEN
            RAISE EXCEPTION 'assign_fkey_checks_to_batch: none of both tables linked by the fkey %.%.% are registered.',
                            p_schema, p_table, r_fk.conname;
        END IF;
-- Record the FK check into the step table.
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                                  stp_sub_object, stp_sql_function, stp_cost)
            VALUES (p_schema || '.' || p_table || '.' || r_fk.conname, p_batchName, 'FOREIGN_KEY', p_schema, p_table,
                    r_fk.conname, '_check_fkey', v_cost);
    END LOOP;
-- Check that fkeys have been found.
    IF v_nbFKey = 0 AND p_fkey IS NOT NULL THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: No fkey % has been found for the table %.%.',
                        p_fkey, p_schema, p_table;
    END IF;
    IF v_nbFKey = 0 AND p_fkey IS NULL THEN
        RAISE WARNING 'assign_fkey_checks_to_batch: The table %.% has no foreign key.',
                        p_schema, p_table;
    END IF;
--
    RETURN v_nbFKey;
END;
$assign_fkey_checks_to_batch$;

-- The add_step_parent() function creates an additional dependancy between 2 steps.
-- It is the user's responsability to set appropriate dependancy and avoid create loops between steps.
CREATE FUNCTION add_step_parent(
    p_batchName              TEXT,
    p_step                   TEXT,
    p_parent_step            TEXT
    )
    RETURNS INT LANGUAGE plpgsql AS          -- returns the number of effectively assigned parent, ie. 1
$add_step_parent$
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_batchName IS NULL OR p_step IS NULL OR p_parent_step IS NULL THEN
        RAISE EXCEPTION 'add_step_parent: The input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists and set the migration as in-progress.
    PERFORM @extschema@.check_batch(p_batchName);
-- Check that both steps exist and are different.
    IF p_step = p_parent_step THEN
        RAISE EXCEPTION 'add_step_parent: both step and parent step must be different';
    END IF;
    PERFORM 0
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'add_step_parent: step % in batch % not found.', p_step, p_batchName;
    END IF;
    PERFORM 0
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_parent_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'add_step_parent: step % in batch % not found.', p_parent_step, p_batchName;
    END IF;
-- Register the new parent.
    UPDATE @extschema@.step
        SET stp_added_parents = array_append(stp_added_parents, p_parent_step)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
--
    RETURN 1;
END;
$add_step_parent$;

-- The complete_migration_configuration() function is the final function in migration's configuration.
-- It checks that all registered and assigned data are consistent and builds the chaining constraints between steps.
CREATE FUNCTION complete_migration_configuration(
    p_migration              TEXT
    )
    RETURNS VOID LANGUAGE plpgsql AS
$complete_migration_configuration$
DECLARE
    v_batchArray             TEXT[];
    v_countBatchWithInitStep INT;
    v_countBatchWithEndStep  INT;
    v_parents                TEXT[];
    v_refSchema              TEXT;
    v_refTable               TEXT;
    v_parentsToAdd           TEXT[];
    r_tbl                    RECORD;
    r_function               RECORD;
    r_step                   RECORD;
BEGIN
-- Check that the migration exist and set it config_completed flag as true.
    UPDATE @extschema@.migration
        SET mgr_config_completed = TRUE
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'complete_migration_configuration: Migration "%" not found.', p_migration;
    END IF;
-- Get the list of related batches.
    SELECT array_agg(bat_name) INTO v_batchArray
        FROM @extschema@.batch
        WHERE bat_migration = p_migration;
-- Raise a warning if none or several batches of type COPY have an INIT step or an END step.
    SELECT count(bat_name) FILTER (WHERE bat_with_init_step), count(bat_name) FILTER (WHERE bat_with_end_step)
        INTO v_countBatchWithInitStep, v_countBatchWithEndstep
        FROM @extschema@.batch
        WHERE bat_migration = p_migration
          AND bat_type = 'COPY';
    IF v_countBatchWithInitStep <> 1 THEN
        RAISE WARNING 'complete_migration_configuration: % batches of type COPY are declared with an initial step. It is usualy 1',
                      v_countBatchWithInitStep;
    END IF;
    IF v_countBatchWithEndStep <> 1 THEN
        RAISE WARNING 'complete_migration_configuration: % batches of type COPY are declared with an end step. It is usualy 1',
                      v_countBatchWithEndStep;
    END IF;
-- Check that all tables registered into the migration have a unique part set as the first one and a unique part as the last one.
    FOR r_tbl IN
        SELECT prt_schema, prt_table, nb_first, nb_last
            FROM (
                SELECT prt_schema, prt_table,
                       count(*) FILTER (WHERE prt_is_first_step) AS nb_first,
                       count(*) FILTER (WHERE prt_is_last_step) AS nb_last
                    FROM @extschema@.table_part
                         JOIN @extschema@.table_to_process ON (prt_schema = tbl_schema AND prt_table = tbl_name)
                    WHERE tbl_migration = p_migration
                    GROUP BY 1,2
                 ) AS t
            WHERE nb_first <> 1 OR nb_last <> 1
    LOOP
        IF r_tbl.nb_first = 0 THEN
            RAISE WARNING 'complete_migration_configuration: The table %.% has no first part.', r_tbl.prt_schema, r_tbl.prt_table;
        END IF;
        IF r_tbl.nb_last = 0 THEN
            RAISE WARNING 'complete_migration_configuration: The table %.% has no last part.', r_tbl.prt_schema, r_tbl.prt_table;
        END IF;
        IF r_tbl.nb_first > 1 THEN
            RAISE WARNING 'complete_migration_configuration: The table %.% has % first parts.', r_tbl.prt_schema, r_tbl.prt_table, r_tbl.nb_first;
        END IF;
        IF r_tbl.nb_last > 1 THEN
            RAISE WARNING 'complete_migration_configuration: The table %.% has % last parts.', r_tbl.prt_schema, r_tbl.prt_table, r_tbl.nb_last;
        END IF;
    END LOOP;
    IF FOUND THEN
        RAISE EXCEPTION 'complete_migration_configuration: Fatal errors encountered';
    END IF;
-- Check that tables having separetely created indexes have also separate first and last parts to enclose the indexes creation.
    FOR r_tbl IN
        SELECT tic_schema, tic_table
            FROM (
                SELECT tic_schema, tic_table, count(prt_number) AS nb_part
                    FROM @extschema@.table_index
                         JOIN @extschema@.table_to_process ON (tic_schema = tbl_schema AND tic_table = tbl_name)
                         LEFT OUTER JOIN @extschema@.table_part ON (tic_schema = prt_schema AND tic_table = prt_table)
                    WHERE tic_separate_creation_step
                      AND tbl_migration = p_migration
                    GROUP BY 1,2
                 ) AS t
            WHERE nb_part < 2
    LOOP
        RAISE EXCEPTION 'complete_migration_configuration: The table %.% has separately created indexes but no table parts have been registered.', r_tbl.tic_schema, r_tbl.tic_table;
    END LOOP;
-- Check that all functions referenced in the step table for the migration exist and will be callable by the scheduler.
    FOR r_function IN
        SELECT DISTINCT '@extschema@.' || stp_sql_function || '(TEXT, TEXT, JSONB)' AS function_prototype
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
    LOOP
-- If the function (or the data2pg role) does not exist, trying to check the privilege will raise a standart postgres exception.
        IF NOT has_function_privilege('data2pg', r_function.function_prototype, 'execute') THEN
            RAISE EXCEPTION 'complete_migration_configuration: The function % will not be callable by the Data2Pg scheduler.', r_function.function_prototype;
        END IF;
    END LOOP;
--
-- Build the chaining constraints between steps.
--
-- Reset the stp_parents column for the all steps of the migration.
    UPDATE @extschema@.step
        SET stp_parents = NULL
        WHERE stp_batch_name = ANY (v_batchArray)
          AND stp_parents IS NOT NULL;
-- Create the links between table parts.
-- The table parts set as the first step for their table must be the parents of all the others parts of the same batch.
    FOR r_step IN
        SELECT stp_batch_name, stp_name, stp_schema, stp_object, stp_part_num
            FROM @extschema@.step
                 JOIN @extschema@.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
                 JOIN @extschema@.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
            WHERE stp_type = 'TABLE_PART'
              AND tbl_migration = p_migration
              AND prt_is_first_step
    LOOP
        UPDATE @extschema@.step
            SET stp_parents = array_append(stp_parents, r_step.stp_name)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_part_num <> r_step.stp_part_num;
    END LOOP;
-- Process the table parts set as the last step for their table.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_part_num
            FROM @extschema@.step
                 JOIN @extschema@.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'TABLE_PART'
              AND prt_is_last_step
    LOOP
-- They must have all the other parts of the same batch as parents.
        SELECT array_agg(stp_name) INTO v_parents
            FROM @extschema@.step
                 JOIN @extschema@.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
                 JOIN @extschema@.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
            WHERE stp_type = 'TABLE_PART'
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_part_num <> r_step.stp_part_num
              AND stp_batch_name = r_step.stp_batch_name;
        UPDATE @extschema@.step
            SET stp_parents = array_cat(stp_parents, v_parents)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_name = r_step.stp_name;
-- They must have all the create index steps of the same batch, if any, as parents also.
        SELECT array_agg(stp_name) INTO v_parents
            FROM @extschema@.step
            WHERE stp_type = 'INDEX'
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_batch_name = r_step.stp_batch_name;
        UPDATE @extschema@.step
            SET stp_parents = array_cat(stp_parents, v_parents)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_name = r_step.stp_name;
    END LOOP;
-- Create the links for index creation steps.
-- All the related table parts of the same batch, except the last one, are parents of each index creation step.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_sub_object
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'INDEX'
    LOOP
        SELECT array_agg(stp_name) INTO v_parents
            FROM @extschema@.step
                 JOIN @extschema@.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
            WHERE stp_type = 'TABLE_PART'
              AND NOT prt_is_last_step
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_batch_name = r_step.stp_batch_name;
        UPDATE @extschema@.step
            SET stp_parents = array_cat(stp_parents, v_parents)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_name = r_step.stp_name;
    END LOOP;
-- Create the links for table checks steps.
-- All the related table or table parts of the same batch are parents of each table check step.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_sub_object
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'CHECK'
    LOOP
        SELECT array_agg(stp_name) INTO v_parents
            FROM @extschema@.step
            WHERE stp_type IN ('TABLE', 'TABLE_PART')
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_batch_name = r_step.stp_batch_name;
        IF v_parents IS NOT NULL THEN
            UPDATE @extschema@.step
                SET stp_parents = array_cat(stp_parents, v_parents)
                WHERE stp_batch_name = r_step.stp_batch_name
                  AND stp_name = r_step.stp_name;
        END IF;
    END LOOP;
-- Add chaining constraints for foreign keys checks.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_sub_object
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'FOREIGN_KEY'
    LOOP
-- Add the owner table and the referenced table, or related table parts if any, as parents, if they are in the same batch as the fkey check.
--      Identify the referenced table.
        SELECT n2.nspname, c2.relname INTO v_refSchema, v_refTable
            FROM pg_catalog.pg_constraint
                 JOIN pg_catalog.pg_class c ON (c.oid = conrelid)
                 JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
                 JOIN pg_catalog.pg_class c2 ON (c2.oid = confrelid)
                 JOIN pg_catalog.pg_namespace n2 ON (n2.oid = c2.relnamespace)
            WHERE n.nspname = r_step.stp_schema
              AND c.relname = r_step.stp_object
              AND conname = r_step.stp_sub_object;
--      Build the parents to add list.
        SELECT array_agg(stp_name ORDER BY stp_name) INTO v_parentsToAdd
            FROM @extschema@.step
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_type IN ('TABLE', 'TABLE_PART')
              AND ((    stp_schema = r_step.stp_schema
                    AND stp_object = r_step.stp_object
                   ) OR (
                        stp_schema = v_refSchema
                    AND stp_object = v_refTable
                   ));
        UPDATE @extschema@.step
            SET stp_parents = array_cat(stp_parents, v_parentsToAdd)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_name = r_step.stp_name;
    END LOOP;
-- Process the chaining constraints related to the INIT steps.
    UPDATE @extschema@.step
        SET stp_parents = ARRAY['INIT_' || p_migration]
        FROM @extschema@.batch
        WHERE stp_batch_name = bat_name
          AND bat_with_init_step
          AND stp_type <> 'INIT'
          AND stp_parents IS NULL;
-- Process the chaining constraints related to the END steps.
    FOR r_step IN
        SELECT stp_name, stp_batch_name
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'END'
    LOOP
        SELECT array_agg(stp_name) INTO v_parents
            FROM @extschema@.step
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_type <> 'END';
        UPDATE @extschema@.step
            SET stp_parents = v_parents
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_name = r_step.stp_name;
    END LOOP;
-- Move the parents registered by add_step_parent() function calls.
    UPDATE @extschema@.step
        SET stp_parents = array_cat(stp_parents, stp_added_parents)
        FROM @extschema@.batch
        WHERE stp_batch_name = bat_name
          AND stp_added_parents IS NOT NULL;
-- Remove duplicate steps in the all parents array of the migration.
    WITH parent_rebuild AS (
        SELECT step_batch, step_name, array_agg(step_parent ORDER BY step_parent) AS unique_parents
            FROM (
               SELECT DISTINCT stp_batch_name AS step_batch, stp_name AS step_name, unnest(stp_parents) AS step_parent
                   FROM @extschema@.step
                   WHERE stp_batch_name = ANY (v_batchArray)
                 ) AS t
            GROUP BY step_batch, step_name)
    UPDATE @extschema@.step
        SET stp_parents = parent_rebuild.unique_parents
        FROM parent_rebuild
        WHERE stp_batch_name = ANY (v_batchArray)
          AND step.stp_batch_name = parent_rebuild.step_batch
          AND step.stp_name = parent_rebuild.step_name
          AND step.stp_parents <> parent_rebuild.unique_parents;
--
    RETURN;
END;
$complete_migration_configuration$;

-- The check_migration() function checks that a given migration name exists, set it as in-progress
--   and returns its server name, RDBMS and import schema options.
CREATE FUNCTION check_migration(
    p_migration               TEXT,
    OUT p_serverName          TEXT,
    OUT p_sourceDbms          TEXT,
    OUT p_importSchemaOptions TEXT
    )
    LANGUAGE plpgsql AS
$check_migration$
BEGIN
-- Check that the migration exists and get its server name and source DBMS.
    SELECT mgr_server_name, mgr_source_dbms, mgr_import_schema_options
        INTO p_serverName, p_sourceDbms, p_importSchemaOptions
        FROM @extschema@.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'check_migration: The migration "%" is unknown.', p_migration;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
        SET mgr_config_completed = FALSE
        WHERE mgr_name = p_migration
          AND mgr_config_completed;
--
    RETURN;
END;
$check_migration$;

-- The check_schema() function checks that a given schema exists.
CREATE FUNCTION check_schema(
    p_schema                  TEXT
    )
    RETURNS void LANGUAGE plpgsql AS
$check_schema$
BEGIN
-- Check that the supplied schema exists.
    PERFORM 0 FROM pg_catalog.pg_namespace
        WHERE nspname = p_schema;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'check_schema: The schema % is unknown.', p_schema;
    END IF;
--
    RETURN;
END;
$check_schema$;

-- The check_batch() function checks that a given migration name exists, set it as in-progress and returns its server name and RDBMS.
CREATE FUNCTION check_batch(
    p_batchName               TEXT,
    OUT p_migrationName       TEXT,
    OUT p_batchType           TEXT
    )
    LANGUAGE plpgsql AS
$check_batch$
BEGIN
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO p_migrationName, p_batchType
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'check_batch: the batch "%" is unknown.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
        SET mgr_config_completed = FALSE
        WHERE mgr_name = p_migrationName
          AND mgr_config_completed;
--
    RETURN;
END;
$check_batch$;

--
--
-- Functions called by the Data2Pg scheduler.
--
--

-- The _copy_init() function is the initial step of a batch of type COPY. It checks that all expected component are present and truncate tables.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report including the number of truncated tables.
CREATE FUNCTION _copy_init(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_copy_init$
DECLARE
    v_batchName                TEXT;
    v_migration                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name, bat_migration INTO v_batchName, v_migration
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_copy_init: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Check the components. It raises an exception in case of trouble.
    PERFORM @extschema@._verify_objects(v_migration);
-- Truncate all application tables linked to the migration.
-- Build the tables list.
    SELECT string_agg('ONLY ' || quote_ident(tbl_schema) || '.' || quote_ident(tbl_name), ', ' ORDER BY tbl_schema, tbl_name), count(*)
        INTO v_tablesList, v_nbTables
        FROM @extschema@.table_to_process
        WHERE tbl_migration = v_migration;
-- ... and truncate them within a single statement.
    EXECUTE format(
          'TRUNCATE %s CASCADE',
          v_tablesList
          );
-- Return the step report.
    r_output.sr_indicator = 'INITIAL_CHECKS_OK';
    r_output.sr_value = 1;
    r_output.sr_rank = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'TRUNCATED_TABLES';
    r_output.sr_value = v_nbTables;
    r_output.sr_rank = 2;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_copy_init$;

-- The _copy_end() function is the final step of a single batch or a set of batches of type COPY.
-- It checks that all expected component are present.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report including the number of truncated tables.
CREATE FUNCTION _copy_end(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_copy_end$
DECLARE
    v_batchName                TEXT;
    v_migration                TEXT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name, bat_migration INTO v_batchName, v_migration
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_copy_init: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Check the components. It raises an exception in case of trouble.
    PERFORM @extschema@._verify_objects(v_migration);
-- Return the step report.
    r_output.sr_indicator = 'FINAL_CHECKS_OK';
    r_output.sr_value = 1;
    r_output.sr_rank = 99;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_copy_end$;

-- The _verify_objects() function verify that all objects involved in a migration really exists.
-- It is called by the _copy_init() and _copy_end() functions
-- Input parameters: migration name.
-- It raises exceptions in case of detected problems, like missing schemas, tables, index, constraints, sequences.
-- The expected objects lists are built at migration configuration time.
CREATE FUNCTION _verify_objects(
    p_migration                TEXT
    )
    RETURNS VOID LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_verify_objects$
DECLARE
    v_list                     TEXT;
BEGIN
-- Check target and foreign schemas.
    SELECT string_agg(tbl_schema, ', ' ORDER BY tbl_schema) INTO v_list
        FROM (
            (SELECT DISTINCT tbl_schema
                FROM @extschema@.table_to_process
                WHERE tbl_migration = p_migration
             UNION
             SELECT DISTINCT tbl_foreign_schema
                FROM @extschema@.table_to_process
                WHERE tbl_migration = p_migration)
            EXCEPT
            SELECT nspname
                FROM pg_catalog.pg_namespace
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing schemas detected (%).', v_list;
    END IF;
-- Check target tables.
    SELECT string_agg(tbl_schema || '.' || tbl_name, ', ' ORDER BY tbl_schema || '.' || tbl_name) INTO v_list
        FROM (
            SELECT DISTINCT tbl_schema, tbl_name
                FROM @extschema@.table_to_process
                WHERE tbl_migration = p_migration
            EXCEPT
            SELECT nspname, relname
                FROM pg_class
                     JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE relkind = 'r'
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing target tables detected (%).', v_list;
    END IF;
-- Check foreign tables.
    SELECT string_agg(tbl_foreign_schema  || '.' || tbl_foreign_name, ', ' ORDER BY tbl_foreign_schema  || '.' || tbl_foreign_name) INTO v_list
        FROM (
            SELECT DISTINCT tbl_foreign_schema, tbl_foreign_name
                FROM @extschema@.table_to_process
                WHERE tbl_migration = p_migration
            EXCEPT
            SELECT nspname, relname
                FROM pg_class
                     JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE relkind = 'f'
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing foreign tables detected (%).', v_list;
    END IF;
-- Check indexes.
    SELECT string_agg(tic_schema || '.' || tic_table || '.' || tic_object, ', ' ORDER BY tic_schema || '.' || tic_table || '.' || tic_object)
        INTO v_list
        FROM (
            SELECT DISTINCT tic_schema, tic_table, tic_object
                FROM @extschema@.table_index
                     JOIN @extschema@.table_to_process ON (tic_schema = tbl_schema AND tic_table = tbl_name)
                WHERE tbl_migration = p_migration
                  AND tic_type = 'I'
            EXCEPT
            SELECT nspname, r.relname, i.relname
                FROM pg_index
                     JOIN pg_class i ON (i.oid = indexrelid)
                     JOIN pg_class r ON (r.oid = indrelid)
                     JOIN pg_catalog.pg_namespace ON (r.relnamespace = pg_namespace.oid)
                WHERE i.relkind = 'i'
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing index detected (%).', v_list;
    END IF;
-- Check PK, UNIQUE and EXCLUDE constraints.
    SELECT string_agg(tic_schema || '.' || tic_table || '.' || tic_object, ', ' ORDER BY tic_schema || '.' || tic_table || '.' || tic_object)
        INTO v_list
        FROM (
            SELECT DISTINCT tic_schema, tic_table, tic_object
                FROM @extschema@.table_index
                     JOIN @extschema@.table_to_process ON (tic_schema = tbl_schema AND tic_table = tbl_name)
                WHERE tbl_migration = p_migration
                  AND tic_type LIKE 'C%'
            EXCEPT
            SELECT nspname, r.relname, conname
                FROM pg_constraint
                     JOIN pg_class r ON (r.oid = conrelid)
                     JOIN pg_catalog.pg_namespace ON (r.relnamespace = pg_namespace.oid)
                WHERE contype IN ('p', 'u', 'x')
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing constraint detected (%).', v_list;
    END IF;
-- Check target sequences.
    SELECT string_agg(seq_schema || '.' || seq_name, ', ' ORDER BY seq_schema || '.' || seq_name) INTO v_list
        FROM (
            SELECT DISTINCT seq_schema, seq_name
                FROM @extschema@.sequence_to_process
                WHERE seq_migration = p_migration
            EXCEPT
            SELECT nspname, relname
                FROM pg_class
                     JOIN pg_catalog.pg_namespace ON (pg_class.relnamespace = pg_namespace.oid)
                WHERE relkind = 'S'
             ) AS t;
    IF v_list IS NOT NULL THEN
        RAISE EXCEPTION '_verify_objects: Missing target sequences detected (%).', v_list;
    END IF;
--
    RETURN;
END;
$_verify_objects$;

-- The _compare_init() function the initial step of a batch of type COMPARE.
-- It requested, it truncates the content diff table that collects detected tables content differences in batches of type COMPARE.
-- The truncation is only performed when the COMPARE_TRUNCATE_DIFF step option is set to true.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report including the number of truncated tables, i.e. 1 or 0 depending on the step option parameter.
CREATE FUNCTION _compare_init(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql AS
$_compare_init$
DECLARE
    v_compareTruncateDiff      BOOLEAN;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Analyze the step options.
    v_compareTruncateDiff = CAST(p_stepOptions->>'COMPARE_TRUNCATE_DIFF' AS BOOLEAN);
-- Truncate the table, if requested.
    IF v_compareTruncateDiff THEN
        TRUNCATE @extschema@.content_diff;
        r_output.sr_value = 1;
    ELSE
        r_output.sr_value = 0;
    END IF;
-- Return the step report.
    r_output.sr_indicator = 'TRUNCATED_TABLES';
    r_output.sr_rank = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_compare_init$;

-- The _compare_end() function is the final step of a single batch or a set of batches of type COMPARE.
-- It does not perform any task, right now.
-- Input parameters: batch name, step name and execution options.
-- It returns an empty step report.
CREATE FUNCTION _compare_end(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_compare_end$
DECLARE
    v_batchName                TEXT;
    v_migration                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name, bat_migration INTO v_batchName, v_migration
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_compare_end: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Return the step report.
    RETURN;
END;
$_compare_end$;

-- The _discover_init() function is the initial step of a single batch or a set of batches of type DISCOVER.
-- It does not perform any task, right now.
-- Input parameters: batch name, step name and execution options.
-- It returns an empty step report.
CREATE FUNCTION _discover_init(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_discover_init$
DECLARE
    v_batchName                TEXT;
    v_migration                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name, bat_migration INTO v_batchName, v_migration
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_discover_init: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Return the step report.
    RETURN;
END;
$_discover_init$;

-- The _discover_end() function is the final step of a single batch or a set of batches of type DISCOVER.
-- It does not perform any task, right now.
-- Input parameters: batch name, step name and execution options.
-- It returns an empty step report.
CREATE FUNCTION _discover_end(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_discover_end$
DECLARE
    v_batchName                TEXT;
    v_migration                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name, bat_migration INTO v_batchName, v_migration
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_discover_end: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Return the step report.
    RETURN;
END;
$_discover_end$;

-- The _copy_table() function is the generic copy function that is used to process tables.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report including the number of copied rows.
-- It is set as session_replication_role = 'replica', so that no check are performed on foreign keys and no regular trigger are executed.
CREATE FUNCTION _copy_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SET session_replication_role = 'replica'
    SET synchronous_commit = 'off'
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_copy_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_partCondition            TEXT;
    v_isFirstStep              BOOLEAN = TRUE;
    v_isLastStep               BOOLEAN = TRUE;
    v_copyMaxRows              BIGINT;
    v_copyPctRows              REAL;
    v_copySlowDown             BIGINT;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_estimatedNbRows          BIGINT;
    v_copySortOrder            TEXT;
    v_insertColList            TEXT;
    v_selectExprList           TEXT;
    v_someGenAlwaysIdentCol    BOOLEAN;
    v_constraint               TEXT;
    v_i                        INT;
    v_index                    TEXT;
    v_indexDef                 TEXT;
    v_stmt                     TEXT;
    v_nbRows                   BIGINT = 0;
    v_nbDroppedIndex           SMALLINT = 0;
    v_nbCreatedIndex           SMALLINT = 0;
    v_nbMissingIndex           SMALLINT = 0;
    r_obj                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the table.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_copy_table: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Read the table_to_process table to get table related details.
    SELECT tbl_foreign_schema, tbl_foreign_name, tbl_rows,
           tbl_copy_sort_order, tbl_some_gen_alw_id_col
        INTO v_foreignSchema, v_foreignTable, v_estimatedNbRows,
             v_copySortOrder, v_someGenAlwaysIdentCol
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- Read the table_column table to get details about columns
    SELECT string_agg(tco_copy_source_expr, ','),
           string_agg(tco_copy_dest_col, ',')
        INTO v_selectExprList, v_insertColList
        FROM (
           SELECT tco_name, tco_copy_source_expr, tco_copy_dest_col
               FROM @extschema@.table_column
               WHERE tco_schema = v_schema AND tco_table = v_table
               ORDER BY tco_number
             ) AS t;
-- If the step concerns a table part, get additional details about the part.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition, prt_is_first_step, prt_is_last_step
            INTO v_partCondition, v_isFirstStep, v_isLastStep
            FROM @extschema@.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
    END IF;
-- Analyze the step options.
    v_copyMaxRows = p_stepOptions->>'COPY_MAX_ROWS';
    v_copyPctRows = p_stepOptions->>'COPY_PCT_ROWS';
    v_copySlowDown = p_stepOptions->>'COPY_SLOW_DOWN';
-- Compute the maximum number of rows to copy, depending on both COPY_MAX_ROWS and COPY_PCT_ROWS option values.
-- When the COPY_PCT_ROWS is set, apply this percentage to the estimated number of rows from the table statistics, rounded to the next integer.
-- If both COPY_MAX_ROWS and COPY_PCT_ROWS options are set, keep the least computed number of rows.
    IF v_copyPctRows IS NOT NULL AND
      (v_copyMaxRows IS NULL OR v_copyMaxRows > v_estimatedNbRows * v_copyPctRows / 100) THEN
       v_copyMaxRows = 1 + v_estimatedNbRows * v_copyPctRows / 100;
    END IF;
--
-- Pre-processing.
--
    IF v_isFirstStep THEN
-- Drop the constraints known as 'to be dropped' (pk, unique, exclude) and record the drop timestamp.
        FOR r_obj IN
            SELECT tic_object
                FROM @extschema@.table_index
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_type LIKE 'C%'
                  AND tic_drop_for_copy
        LOOP
            EXECUTE format(
                'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                v_schema, v_table, r_obj.tic_object
            );
            UPDATE @extschema@.table_index
                SET tic_last_drop_ts = clock_timestamp(),
                    tic_last_create_ts = NULL,
                    tic_last_create_duration = NULL
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_object = r_obj.tic_object;
            v_nbDroppedIndex = v_nbDroppedIndex + 1;
        END LOOP;
-- Drop the indexes known as "to be dropped" during the copy processing and record the drop timestamp.
        FOR r_obj IN
            SELECT tic_object
                FROM @extschema@.table_index
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_type = 'I'
                  AND tic_drop_for_copy
        LOOP
            EXECUTE format(
                'DROP INDEX %I.%I',
                v_schema, r_obj.tic_object
            );
            UPDATE @extschema@.table_index
                SET tic_last_drop_ts = clock_timestamp(),
                    tic_last_create_ts = NULL,
                    tic_last_create_duration = NULL
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_object = r_obj.tic_object;
            v_nbDroppedIndex = v_nbDroppedIndex + 1;
        END LOOP;
    END IF;
--
-- Copy processing.
--
-- The copy processing is not performed for a 'TABLE_PART' step without condition.
    IF v_partNum IS NULL OR v_partCondition IS NOT NULL THEN
-- Do not sort the source data when the table is processed with a WHERE clause or a LIMIT clause.
        IF v_partCondition IS NOT NULL OR v_copyMaxRows IS NOT NULL THEN
            v_copySortOrder = NULL;
        END IF;
-- Copy the foreign table to the destination table.
        v_stmt = format(
            'INSERT INTO %I.%I (%s) %s
               SELECT %s
               FROM ONLY %I.%I
               %s
               %s
               %s',
            v_schema, v_table, v_InsertColList,
            CASE WHEN v_someGenAlwaysIdentCol THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
            v_selectExprList, v_foreignSchema, v_foreignTable,
            coalesce('WHERE ' || v_partCondition, ''),
            coalesce('ORDER BY ' || v_copySortOrder, ''),
            coalesce('LIMIT ' || v_copyMaxRows, '')
            );
--raise warning '%',v_stmt;
        EXECUTE v_stmt;
        GET DIAGNOSTICS v_nbRows = ROW_COUNT;
    END IF;
--
-- Post processing.
--
    IF v_isLastStep THEN
-- Recreate the constraints that have been previously dropped.
        FOR r_obj IN
            SELECT tic_object, tic_type, tic_definition, tic_separate_creation_step
                FROM @extschema@.table_index
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_type LIKE 'C%'
                  AND tic_drop_for_copy
        LOOP
-- Look at the catalog to know whether the related index exists.
            PERFORM 0 
                FROM pg_catalog.pg_class
                     JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
                WHERE relkind = 'i'
                  AND nspname = v_schema
                  AND relname = r_obj.tic_object;
            IF NOT FOUND THEN 
-- The index related to the constraint does not exist (this is the usual case), so just recreate the constraint with the common syntax.
                v_nbCreatedIndex = v_nbCreatedIndex + 1;
                IF r_obj.tic_separate_creation_step THEN
-- The index related to the constraint should have been recreated. This is probably an error in the migration conofiguration.
                    v_nbMissingIndex = v_nbMissingIndex + 1;
                    RAISE WARNING '_copy_table: For the constraint %.%.%, the index should have been already recreated. Some steps are probably missing in the batch. However, recrete the constraint.',
                        v_schema, v_table, r_obj.tic_object;
                END IF;
-- Record the recreation start timestamp.
                UPDATE @extschema@.table_index
                    SET tic_last_create_ts = clock_timestamp()
                    WHERE tic_schema = v_schema
                      AND tic_table = v_table
                      AND tic_object = r_obj.tic_object;
-- Recreate the constraint with its initial definition.
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                    v_schema, v_table, r_obj.tic_object, r_obj.tic_definition
                );
-- Record the recreation duration.
                UPDATE @extschema@.table_index
                    SET tic_last_create_duration = clock_timestamp() - tic_last_create_ts
                    WHERE tic_schema = v_schema
                      AND tic_table = v_table
                      AND tic_object = r_obj.tic_object;
            ELSE
-- The index already exists. So recreate the constraint using it.
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD CONSTRAINT %I %s USING INDEX %I',
                    v_schema, v_table, r_obj.tic_object,
                    CASE r_obj.tic_type
                        WHEN 'Cp' THEN 'PRIMARY KEY'
                        WHEN 'Cu' THEN 'UNIQUE'
                        ELSE NULL                        -- should never happen
                    END, r_obj.tic_object
                );
            END IF;
        END LOOP;
-- Recreate the indexes that have been previously dropped and that are not recreated by a separate step.
        FOR r_obj IN
            SELECT tic_object, tic_definition
                FROM @extschema@.table_index
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_type = 'I'
                  AND tic_drop_for_copy
                  AND NOT tic_separate_creation_step
        LOOP
            v_nbCreatedIndex = v_nbCreatedIndex + 1;
-- Record the recreation start timestamp
            UPDATE @extschema@.table_index
                SET tic_last_create_ts = clock_timestamp()
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_object = r_obj.tic_object;
-- Create the index.
            EXECUTE r_obj.tic_definition;
-- Record the recreation duration
            UPDATE @extschema@.table_index
                SET tic_last_create_duration = clock_timestamp() - tic_last_create_ts
                WHERE tic_schema = v_schema
                  AND tic_table = v_table
                  AND tic_object = r_obj.tic_object;
        END LOOP;
-- Get the statistics (and let the autovacuum do its job).
        EXECUTE format(
            'ANALYZE %I.%I',
            v_schema, v_table);
    END IF;
-- Slowdown (for testing purpose only)
    IF v_copySlowDown IS NOT NULL THEN
        PERFORM pg_sleep(v_nbRows * v_copySlowDown / 1000000);
    END IF;
-- Return the step report.
    IF v_isLastStep THEN
        r_output.sr_indicator = 'COPIED_TABLES';
        r_output.sr_value = 1;
        r_output.sr_rank = 10;
        r_output.sr_is_main_indicator = FALSE;
        RETURN NEXT r_output;
    END IF;
    r_output.sr_indicator = 'COPIED_ROWS';
    r_output.sr_value = v_nbRows;
    r_output.sr_rank = 11;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
    IF v_nbDroppedIndex > 0 THEN
        r_output.sr_indicator = 'DROPPED_INDEXES';
        r_output.sr_value = v_nbDroppedIndex;
        r_output.sr_rank = 12;
        r_output.sr_is_main_indicator = FALSE;
        RETURN NEXT r_output;
    END IF;
    IF v_nbCreatedIndex > 0 THEN
        r_output.sr_indicator = 'RECREATED_INDEXES';
        r_output.sr_value = v_nbCreatedIndex;
        r_output.sr_rank = 13;
        r_output.sr_is_main_indicator = FALSE;
        RETURN NEXT r_output;
    END IF;
    IF v_nbMissingIndex > 0 THEN
        r_output.sr_indicator = 'MISSING_INDEXES';
        r_output.sr_value = v_nbMissingIndex;
        r_output.sr_rank = 14;
        r_output.sr_is_main_indicator = FALSE;
        RETURN NEXT r_output;
    END IF;
--
    RETURN;
END;
$_copy_table$;

-- The _copy_sequence() function is a generic sequence adjustment function that is used to process individual sequences.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report.
CREATE FUNCTION _copy_sequence(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SET synchronous_commit = 'off'
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$_copy_sequence$
DECLARE
    v_schema                   TEXT;
    v_sequence                 TEXT;
    v_foreignSchema            TEXT;
    v_sourceSchema             TEXT;
    v_sourceSequenceName       TEXT;
    v_sourceDbms               TEXT;
    v_lastValue                BIGINT;
    v_isCalled                 TEXT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the sequence.
    SELECT stp_schema, stp_object, 'srcdb_' || stp_schema, mgr_source_dbms, seq_source_schema, seq_source_name
      INTO v_schema, v_sequence, v_foreignSchema, v_sourceDbms, v_sourceSchema, v_sourceSequenceName
        FROM @extschema@.step
             JOIN @extschema@.sequence_to_process ON (seq_schema = stp_schema AND seq_name = stp_object)
             JOIN @extschema@.migration ON (seq_migration = mgr_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_copy_sequence: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Depending on the source DBMS, get the sequence's characteristics.
    SELECT p_lastValue, p_isCalled INTO v_lastValue, v_isCalled
       FROM @extschema@._get_source_sequence(v_sourceDbms, v_sourceSchema, v_sourceSequenceName, v_foreignSchema, v_sequence);
-- Set the sequence's characteristics.
    EXECUTE format(
        'SELECT setval(%L, %s, %s)',
        quote_ident(v_schema) || '.' || quote_ident(v_sequence), v_lastValue, v_isCalled
        );
-- Return the step report.
    r_output.sr_indicator = 'COPIED_SEQUENCES';
    r_output.sr_value = 1;
    r_output.sr_rank = 20;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_copy_sequence$;

-- The _get_source_sequence() function get the properties of a sequence on the source database, depending on the RDBMS.
-- It is called by functions processing sequences copy or comparison.
-- Input parameters: source DBSM and the sequence id.
-- The output parameters: last value and is_called properties.
CREATE FUNCTION _get_source_sequence(
    p_sourceDbms               TEXT,
    p_sourceSchema             TEXT,
    p_sourceSequenceName       TEXT,
    p_foreignSchema            TEXT,
    p_foreignSequence          TEXT,
    OUT p_lastValue            BIGINT,
    OUT p_isCalled             TEXT
    )
    RETURNS RECORD LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$_get_source_sequence$
BEGIN
-- Depending on the source DBMS, get the sequence's characteristics.
    IF p_sourceDbms = 'Oracle' THEN
        SELECT last_number, 'true' INTO p_lastValue, p_isCalled
           FROM @extschema@.ora_sequences
           WHERE sequence_owner = p_sourceSchema
             AND sequence_name = p_sourceSequenceName;
    ELSIF p_sourceDbms = 'PostgreSQL' THEN
        EXECUTE format(
            'SELECT last_value, CASE WHEN is_called THEN ''true'' ELSE ''false'' END FROM %I.%I',
            p_foreignSchema, p_foreignSequence
            ) INTO p_lastValue, p_isCalled;
    ELSE
        RAISE EXCEPTION '_get_source_sequence: The DBMS % is not yet implemented (internal error).', p_sourceDbms;
    END IF;
--
    RETURN;
END;
$_get_source_sequence$;

-- The _create_index() function is a generic function recreate a index that has been marked as
-- 'not to be created in the copy post-processing phase'. It may be useful to create several indexes in parallel for a large table.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report.
CREATE FUNCTION _create_index(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SET synchronous_commit = 'off'
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_create_index$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_index                    TEXT;
    v_type                     TEXT;
    v_definition               TEXT;
    v_separateCreationStep     BOOLEAN;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the index/constraint.
    SELECT stp_schema, stp_object, stp_sub_object, tic_type, tic_definition, tic_separate_creation_step
        INTO v_schema, v_table, v_index, v_type, v_definition, v_separateCreationStep
        FROM @extschema@.step
             JOIN @extschema@.table_index ON (tic_schema = stp_schema AND tic_table = stp_object AND tic_object = stp_sub_object)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_create_index: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Check that the requested index/constraint creation is valid.
    IF NOT v_separateCreationStep THEN
        RAISE EXCEPTION '_create_index: internal error (the index %.% is not marked as to be created by a separate step).', v_schema, v_index;
    END IF;
-- Record the recreation start timestamp.
    UPDATE @extschema@.table_index
        SET tic_last_create_ts = clock_timestamp()
        WHERE tic_schema = v_schema
          AND tic_table = v_table
          AND tic_object = v_index;
-- Create the index of the constraint depending on its registered type.
    CASE
        WHEN v_type = 'I' THEN
-- It is an index.
            EXECUTE v_definition;
        WHEN v_type IN ('Cp', 'Cu') THEN
-- It is a constraint.
-- Create the related index in advance. The constraint will be created later in the table copy post-processing, referencing this index.
-- To get the index definition, just remove the PRIMARY KEY or UNIQUE keywords from the constraint definition.
            EXECUTE format(
                'CREATE UNIQUE INDEX %I ON %I.%I %s',
                v_index, v_schema, v_table,
                regexp_replace(v_definition, 'PRIMARY KEY |UNIQUE ', ''));
        ELSE
            RAISE EXCEPTION '_create_index: internal error (the index type for %.% is %, should be "I" or "Cp" or "Cu").', v_schema, v_index, v_type;
    END CASE;
-- Record the recreation duration
    UPDATE @extschema@.table_index
        SET tic_last_create_duration = clock_timestamp() - tic_last_create_ts
        WHERE tic_schema = v_schema
          AND tic_table = v_table
          AND tic_object = v_index;
-- Return the step report.
    r_output.sr_indicator = 'RECREATED_INDEXES';
    r_output.sr_value = 1;
    r_output.sr_rank = 13;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_create_index$;

-- The _check_table() function computes some aggregates, at least the number of rows, on both source and target tables.
-- Results can be compared to be sure that the data content has been correctly migrated.
-- Input parameters: batch name, step name and execution options.
-- It stores the result into the counter table.
-- It returns a step report including the number of recorded aggregates.
CREATE OR REPLACE FUNCTION _check_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_check_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_serverName               TEXT;
    v_sourceDbms               TEXT;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_aggForeignTable          TEXT;
    v_sourceSchema             TEXT;
    v_sourceTable              TEXT;
    v_nbColumns                SMALLINT = 0;
    v_colDefList               TEXT;
    v_stmt                     TEXT;
    v_foreignTableQuery        TEXT;
    v_foreignTableOptions      TEXT;
    v_nbRows                   BIGINT;
    v_nbAggregates             INT;
    v_nbDiff                   INT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the table to examine.
    SELECT stp_schema, stp_object
        INTO v_schema, v_table
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_check_table: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Read the migration table to get the FDW server name and source RDBMS.
    SELECT mgr_server_name, mgr_source_dbms
        INTO v_serverName, v_sourceDbms
        FROM @extschema@.migration
             JOIN @extschema@.batch ON (bat_migration = mgr_name)
        WHERE bat_name = p_batchName;
-- Read the table_to_process table to get the foreign schema and build the foreign table name.
    SELECT tbl_foreign_schema, tbl_foreign_name, tbl_source_schema, tbl_source_name, tbl_foreign_name || '_agg'
        INTO v_foreignSchema, v_foreignTable, v_sourceSchema, v_sourceTable, v_aggForeignTable
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- Delete previous aggregates in the counter table.
    DELETE FROM @extschema@.counter
        WHERE cnt_schema = v_schema AND cnt_table = v_table;
--
-- Process the target table.
    v_nbAggregates = 1;
-- Analyze the source table content and record the resulting aggregates into the counter table.
    v_stmt = 'WITH aggregates AS ('
          || '  SELECT count(*) AS nb_rows FROM ' || quote_ident(v_schema) || '.' || quote_ident(v_table)
          || '  ) '
          || 'INSERT INTO @extschema@.counter (cnt_schema, cnt_table, cnt_database, cnt_counter, cnt_value)'
          || '  SELECT ' || quote_literal(v_schema) || ', ' || quote_literal(v_table) || ', ''D'', ''nb_rows'', nb_rows'
          || '      FROM aggregates';
    EXECUTE v_stmt;
--
-- Process the source table.
    IF v_sourceDbms IN ('Oracle', 'SQLServer', 'Sybase_ASA') THEN
-- With FDW allowing to create a foreign table defined as query, use this technic to let the source DBMS compute the aggregates.
        v_colDefList = 'nb_rows BIGINT';
-- Create the aggregate foreign table.
        v_foreignTableQuery := format(
            'SELECT count(*) AS nb_rows FROM %I.%I',
            v_sourceSchema, v_sourceTable
        );
        IF v_sourceDbms = 'Oracle' THEN
            v_foreignTableOptions = format(
                'table %L',
                '(' || v_foreignTableQuery || ')'
                );
        ELSE
            v_foreignTableOptions = format(
                'query %L',
                v_foreignTableQuery
                );
        END IF;
        v_stmt = format(
            'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS (%s)',
            v_foreignSchema, v_aggForeignTable, v_colDefList, v_serverName, v_foreignTableOptions
        );
        EXECUTE v_stmt;
-- Analyze the source table content by querying the aggregate foreign table and record the resulting aggregates into the counter table.
        v_stmt = 'WITH aggregates AS ('
              || '  SELECT * FROM ' || quote_ident(v_foreignSchema) || '.' || quote_ident(v_aggForeignTable)
              || '  ) '
              || 'INSERT INTO @extschema@.counter (cnt_schema, cnt_table, cnt_database, cnt_counter, cnt_value)'
              || '  SELECT ' || quote_literal(v_schema) || ', ' || quote_literal(v_table) || ', ''S'', ''nb_rows'', nb_rows'
              || '      FROM aggregates';
        EXECUTE v_stmt;
-- Drop the aggregate foreign table, that is now useless.
        EXECUTE format(
            'DROP FOREIGN TABLE %I.%I',
            v_foreignSchema, v_aggForeignTable
        );
    ELSE
-- Otherwise directly compute the aggregates on the foreign table.
-- Analyze the source table content by querying the foreign table and record the resulting aggregates into the counter table.
        v_stmt = 'WITH aggregates AS ('
              || '  SELECT count(*) AS nb_rows FROM ' || quote_ident(v_foreignSchema) || '.' || quote_ident(v_foreignTable)
              || '  ) '
              || 'INSERT INTO @extschema@.counter (cnt_schema, cnt_table, cnt_database, cnt_counter, cnt_value)'
              || '  SELECT ' || quote_literal(v_schema) || ', ' || quote_literal(v_table) || ', ''S'', ''nb_rows'', nb_rows'
              || '      FROM aggregates';
        EXECUTE v_stmt;
    END IF;
--
-- Compare aggregate from both databases.
    SELECT count(*) INTO v_nbDiff
        FROM @extschema@.counter s
             JOIN @extschema@.counter d ON (d.cnt_schema = s.cnt_schema AND d.cnt_table = s.cnt_table AND
                                            d.cnt_database = 'D' AND d.cnt_counter = s.cnt_counter)
        WHERE s.cnt_schema = v_schema
          AND s.cnt_table = v_table
          AND s.cnt_database = 'S'
          AND s.cnt_value <> d.cnt_value;
-- Return the step report.
    r_output.sr_indicator = 'CHECKED_TABLES';
    r_output.sr_value = 1;
    r_output.sr_rank = 90;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'COMPUTED_AGGREGATES';
    r_output.sr_value = v_nbAggregates;
    r_output.sr_rank = 91;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'UNEQUAL_AGGREGATES';
    r_output.sr_value = v_nbDiff;
    r_output.sr_rank = 92;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_check_table$;

-- The _compare_table() function is a generic compare function that is used to process tables.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report including the number of discrepancies found.
-- The function compares a foreign table with its related local table, using a single SQL statement.
-- Discrepancies are inserted into the data2pg.content_diff table.
-- In content_diff, rows content is represented in JSON format, distinguishing key columns and other columns.
-- The comparison takes into account column transformation rules that have been defined for the COPY processing by register_columns_transform_rule() function calls.
-- It also takes into account additional column comparison rules defined by register_column_comparison_rule() function calls.
-- It may be simple masking. In this case, the keyword "MASKED" replaces the column content for the comparison and in the content_diff table.
-- It may be an computation applied on the source column and on the local column. Both may be different if needed.
-- When a comparison rule is applied on a column, its name reported in the content_diff JSON values is enclosed by parenthesis.
CREATE FUNCTION _compare_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_compare_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_sourceExprList           TEXT;
    v_destExprList             TEXT;
    v_destColList              TEXT;
    v_keyJsonBuild             TEXT;
    v_otherJsonBuild           TEXT;
    v_compareSortOrder         TEXT;
    v_partCondition            TEXT;
    v_maxDiff                  TEXT;
    v_maxRows                  TEXT;
    v_stmt                     TEXT;
    v_nbDiff                   BIGINT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the table identity.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_compare_table: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Read the table_to_process table to get table related details.
    SELECT tbl_foreign_schema, tbl_foreign_name
        INTO v_foreignSchema, v_foreignTable
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- Read the table_column table to get details about columns and build pieces of SQL that will be used by the global comparison statement below.
    SELECT string_agg(quote_ident(tco_name), ','),
           string_agg(coalesce(tco_compare_source_expr, quote_literal('MASKED')), ','),
           string_agg(coalesce(tco_compare_dest_expr, quote_literal('MASKED')), ','),
           string_agg(quote_literal(tco_decorated_name) || ',' || quote_ident(tco_name), ',') FILTER (WHERE tco_compare_sort_rank IS NOT NULL),
           string_agg(quote_literal(tco_decorated_name) || ',' || quote_ident(tco_name), ',') FILTER (WHERE tco_compare_sort_rank IS NULL),
           coalesce(string_agg(quote_ident(tco_name), ',' ORDER BY tco_compare_sort_rank) FILTER (WHERE tco_compare_sort_rank IS NOT NULL),
                    string_agg(quote_ident(tco_name), ','))
        INTO v_destColList,
             v_sourceExprList,
             v_destExprList,
             v_keyJsonBuild,
             v_otherJsonBuild,
             v_compareSortOrder
        FROM (
           SELECT tco_name, tco_compare_source_expr, tco_compare_dest_expr, tco_compare_sort_rank,
                  CASE WHEN tco_compare_source_expr = tco_name AND tco_compare_dest_expr = tco_name
                      THEN tco_name
                      ELSE '(' || tco_name || ')'
                  END AS tco_decorated_name
               FROM @extschema@.table_column
               WHERE tco_schema = v_schema AND tco_table = v_table
               ORDER BY tco_number
             ) AS t;
-- If the step concerns a table part, warn if no WHERE clause exists.
--   Pre-processing or post-processing only steps are only useful for batches of type COPY.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition
            INTO v_partCondition
            FROM @extschema@.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
        IF v_partCondition IS NULL THEN
            RAISE WARNING '_compare_table: A table part cannot be compared without a condition. This step % should not have been assigned to this batch.', p_step;
        END IF;
    END IF;
-- Analyze the step options.
    v_maxDiff = p_stepOptions->>'COMPARE_MAX_DIFF';
    v_maxRows = p_stepOptions->>'COMPARE_MAX_ROWS';
--
-- Compare processing.
--
-- The compare processing is not performed for a 'TABLE_PART' step without condition.
    IF v_partNum IS NULL OR v_partCondition IS NOT NULL THEN
-- Compare the foreign table and the destination table.
        v_stmt = format(
            'WITH ft (%s) AS (
                     SELECT %s FROM %I.%I %s
                     ORDER BY %s %s),
                  t (%s) AS (
                     SELECT %s FROM %I.%I %s
                     ORDER BY %s %s),
                  source_diff AS (
                     SELECT * FROM ft
                         EXCEPT
                     SELECT * FROM t
                     LIMIT %s),
                  destination_diff AS (
                     SELECT * FROM t
                         EXCEPT
                     SELECT * FROM ft
                     LIMIT %s),
                  all_diff AS (
                     SELECT %s, ''S'' AS diff_database, json_build_object(%s) AS diff_key_cols, json_build_object(%s) AS diff_other_cols FROM source_diff
                         UNION ALL
                     SELECT %s, ''D'', json_build_object(%s), json_build_object(%s) FROM destination_diff
                     LIMIT %s),
                  formatted_diff AS (
                     SELECT %L AS diff_schema, %L AS diff_relation, dense_rank() OVER (ORDER BY %s) AS diff_rank, diff_database, diff_key_cols, diff_other_cols
                        FROM all_diff ORDER BY %s, diff_database DESC),
                  inserted_diff AS (
                     INSERT INTO @extschema@.content_diff
                         (diff_schema, diff_relation, diff_rank, diff_database, diff_key_cols, diff_other_cols)
                         SELECT * FROM formatted_diff %s
                         RETURNING diff_rank)
                  SELECT max(diff_rank) FROM inserted_diff',
            -- ft CTE variables
            v_destColList,
            v_sourceExprList, v_foreignSchema, v_foreignTable, coalesce('WHERE ' || v_partCondition, ''),
            v_compareSortOrder, coalesce('LIMIT ' || v_maxRows, ''),
            -- t CTE variables
            v_destColList,
            v_destExprList, v_schema, v_table, coalesce('WHERE ' || v_partCondition, ''),
            v_compareSortOrder, coalesce('LIMIT ' || v_maxRows, ''),
            -- source_diff CTE variables
            coalesce (v_maxDiff, 'ALL'),
            -- destination_diff CTE variables
            coalesce (v_maxDiff, 'ALL'),
            -- all_diff CTE variables
            v_compareSortOrder, v_keyJsonBuild, v_otherJsonBuild,
            v_compareSortOrder, v_keyJsonBuild, v_otherJsonBuild,
            coalesce (v_maxDiff, 'ALL'),
            -- formatted_diff CTE variables
            v_schema, v_table, v_compareSortOrder,
            v_compareSortOrder,
            -- inserted_diff CTE variables
            coalesce ('WHERE diff_rank <= ' || v_maxDiff, '')
            );
--raise warning '%',v_stmt;
        EXECUTE v_stmt INTO v_nbDiff;
    END IF;
-- Return the step report.
    IF v_partNum IS NULL THEN
        r_output.sr_indicator = 'COMPARED_TABLES';
        r_output.sr_rank = 50;
    ELSE
        r_output.sr_indicator = 'COMPARED_TABLE_PARTS';
        r_output.sr_rank = 51;
    END IF;
    r_output.sr_value = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    IF v_partNum IS NULL THEN
        r_output.sr_indicator = 'NON_EQUAL_TABLES';
        r_output.sr_rank = 60;
    ELSE
        r_output.sr_indicator = 'NON_EQUAL_TABLE_PARTS';
        r_output.sr_rank = 61;
    END IF;
    r_output.sr_value = CASE WHEN v_nbDiff IS NULL THEN 0 ELSE 1 END;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    r_output.sr_indicator = 'ROW_DIFFERENCES';
    r_output.sr_value = coalesce(v_nbDiff, 0);
    r_output.sr_rank = 71;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_compare_table$;

-- The _compare_sequence() function compares the characteristics of a source and its destination sequence.
-- Input parameters: batch name, step name and execution options.
-- It returns a step report.
-- Discrepancies are inserted into the data2pg.content_diff table.
CREATE FUNCTION _compare_sequence(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$_compare_sequence$
DECLARE
    v_schema                   TEXT;
    v_sequence                 TEXT;
    v_foreignSchema            TEXT;
    v_sourceDbms               TEXT;
    v_sourceSchema             TEXT;
    v_sourceSequenceName       TEXT;
    v_srcLastValue             BIGINT;
    v_srcIsCalled              TEXT;
    v_destLastValue            BIGINT;
    v_destIsCalled             TEXT;
    v_areSequencesEqual        BOOLEAN;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the sequence.
    SELECT stp_schema, stp_object, 'srcdb_' || stp_schema, mgr_source_dbms, seq_source_schema, seq_source_name
      INTO v_schema, v_sequence, v_foreignSchema, v_sourceDbms, v_sourceSchema, v_sourceSequenceName
        FROM @extschema@.step
             JOIN @extschema@.sequence_to_process ON (seq_schema = stp_schema AND seq_name = stp_object)
             JOIN @extschema@.migration ON (seq_migration = mgr_name)
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_compare_sequence: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Depending on the source DBMS, get the source sequence's characteristics.
    SELECT p_lastValue, p_isCalled INTO v_srcLastValue, v_srcIsCalled
       FROM @extschema@._get_source_sequence(v_sourceDbms, v_sourceSchema, v_sourceSequenceName, v_foreignSchema, v_sequence);
-- Get the destination sequence's characteristics.
    EXECUTE format(
        'SELECT last_value, is_called
             FROM %s',
        quote_ident(v_schema) || '.' || quote_ident(v_sequence)
        )
        INTO v_destLastValue, v_destIsCalled;
-- If both sequences don't match, record it.
    v_areSequencesEqual = (v_srcLastValue = v_destLastValue AND v_srcIsCalled = v_destIsCalled);
    IF NOT v_areSequencesEqual THEN
        INSERT INTO @extschema@.content_diff
            (diff_schema, diff_relation, diff_rank, diff_database, diff_key_cols, diff_other_cols)
            VALUES
            (v_schema, v_sequence, 1, 'S', NULL, ('{"last_value": ' || v_srcLastValue || ', "is_called": ' || v_srcIsCalled || '}')::JSON),
            (v_schema, v_sequence, 1, 'D', NULL, ('{"last_value": ' || v_destLastValue || ', "is_called": ' || v_destIsCalled || '}')::JSON);
    END IF;
-- Return the step report.
    r_output.sr_indicator = 'COMPARED_SEQUENCES';
    r_output.sr_value = 1;
    r_output.sr_rank = 52;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'SEQUENCE_DIFFERENCES';
    r_output.sr_rank = 72;
    r_output.sr_value = CASE WHEN NOT v_areSequencesEqual THEN 1 ELSE 0 END;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_compare_sequence$;

-- The _check_fkey() function supresses and recreates a foreign key to be sure that the constraint is verified.
-- This may not be always the case because tables are populated in replica mode.
-- Input parameters: batch name, step name and execution options.
-- The function does not perform anything if the COPY_MAX_ROWS step option is set, because the referential integrity cannot be garanteed if only a subset of tables is copied.
-- It returns a step report.
CREATE FUNCTION _check_fkey(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_check_fkey$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_fkey                     TEXT;
    v_copyMaxRows              BIGINT;
    v_fkeyDef                  TEXT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_schema, stp_object, stp_sub_object INTO v_schema, v_table, v_fkey
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_check_fkey: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Analyze the step options.
    v_copyMaxRows = p_stepOptions->>'COPY_MAX_ROWS';
-- Do not process any FK check if we are not sure that tables have been fully copied.
    IF v_copyMaxRows IS NULL THEN
-- Get the FK definition.
        SELECT pg_get_constraintdef(pg_constraint.oid) INTO v_fkeyDef
            FROM pg_catalog.pg_constraint
                 JOIN pg_catalog.pg_class ON (pg_class.oid = conrelid)
                 JOIN pg_catalog.pg_namespace ON (pg_namespace.oid = relnamespace)
            WHERE nspname = v_schema
              AND relname = v_table
              AND conname = v_fkey;
-- Check that the FK still exist.
       IF NOT FOUND THEN
           RAISE EXCEPTION '_check_fkey: The foreign key % for table %.% has not been found.', v_fkey, v_schema, v_table;
       END IF;
-- Drop the FK.
       EXECUTE format(
             'ALTER TABLE %I.%I DROP CONSTRAINT %I',
             v_schema, v_table, v_fkey
             );
-- Recreate the FK.
       EXECUTE format(
             'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
             v_schema, v_table, v_fkey, v_fkeyDef
             );
    END IF;
-- Return the step report.
    r_output.sr_indicator = 'CHECKED_FKEYS';
    IF v_copyMaxRows IS NULL THEN
        r_output.sr_value = 1;
    ELSE
        r_output.sr_value = 0;
    END IF;
    r_output.sr_rank = 30;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
  RETURN;
END;
$_check_fkey$;

-- The _discover_table() function scans a foreign table to compute some statistics useful to decide the best postgres data type for its columns.
-- Input parameters: batch name, step name and execution options.
-- It stores the result into the discovery_column table.
-- It returns a step report including the number of analyzed columns and rows.
CREATE OR REPLACE FUNCTION _discover_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$_discover_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_maxRows                  BIGINT;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_aggForeignTable          TEXT;
    v_serverName               TEXT;
    v_sourceSchema             TEXT;
    v_sourceTable              TEXT;
    v_nbColumns                SMALLINT = 0;
    v_colDefArray              TEXT[] = '{}';
    v_colDefList               TEXT;
    v_aggregatesArray          TEXT[] = '{}';
    v_aggregatesList           TEXT;
    v_insertCteArray           TEXT[] = '{}';
    v_insertCteList            TEXT;
    v_commonDscvColToInsert    TEXT;
    v_skippedColumnsArray      TEXT[] = '{}';
    v_skippedColumnsList       TEXT;
    v_commonDscvValToInsert    TEXT;
    v_nbNotNullValue           TEXT;
    v_stmt                     TEXT;
    v_foreignTableQuery        TEXT;
    v_nbRows                   BIGINT;
    v_nbAdvice                 INTEGER = 0;
    r_col                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the table to discover.
    SELECT stp_schema, stp_object, stp_part_num
        INTO v_schema, v_table, v_partNum
        FROM @extschema@.step
        WHERE stp_batch_name = p_batchName
          AND stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION '_discover_table: no step % found for the batch %.', p_step, p_batchName;
    END IF;
-- Read the migration table to get the FDW server name.
    SELECT mgr_server_name
        INTO v_serverName
        FROM @extschema@.migration
             JOIN @extschema@.batch ON (bat_migration = mgr_name)
        WHERE bat_name = p_batchName;
-- Read the table_to_process table to get the foreign schema and build the foreign table name.
    SELECT tbl_foreign_schema, tbl_foreign_name, tbl_source_schema, tbl_source_name, tbl_foreign_name || '_agg'
        INTO v_foreignSchema, v_foreignTable, v_sourceSchema, v_sourceTable, v_aggForeignTable
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- Analyze the step options.
    v_maxRows = p_stepOptions->>'DISCOVER_MAX_ROWS';
--
-- Remove from the discovery_table, discovery_column and discovery_advice tables the previous collected data for the table to process.
--
    DELETE FROM @extschema@.discovery_advice
        WHERE dscv_schema = v_schema
          AND dscv_table = v_table;
    DELETE FROM @extschema@.discovery_column
        WHERE dscv_schema = v_schema
          AND dscv_table = v_table;
    DELETE FROM @extschema@.discovery_table
        WHERE dscv_schema = v_schema
          AND dscv_table = v_table;
-- Depending on the column's type, build:
--    - the columns definition for the foreign table
--    - the aggregates to feed these columns
--    - the insert cte that will copy the computed aggregates from the foreign table to the discover_column table
    v_colDefArray = array_append(v_colDefArray, 'nb_row BIGINT');
    v_aggregatesArray = array_append(v_aggregatesArray, 'count(*) AS nb_row');
    v_commonDscvColToInsert = 'dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_type, dscv_max_size, dscv_nb_not_null,';
    FOR r_col IN
        SELECT attname, attnum, typname, typcategory, CASE WHEN atttypmod >= 0 THEN atttypmod ELSE NULL END AS atttypmod, attnotnull
            FROM pg_attribute a
                 JOIN pg_class c ON (c.oid = a.attrelid)
                 JOIN pg_namespace n ON (n.oid = c.relnamespace)
                 JOIN pg_type t ON (t.oid = a.atttypid)
            WHERE nspname = v_foreignSchema
              AND relname = v_foreignTable
              AND attnum > 0
              AND NOT attisdropped
    LOOP
        IF cardinality(v_aggregatesArray) > 993 THEN
-- If there are too many columns in the foreign table statement definition, don't analyze the remaining columns of the source table
--   and just keep the information to report it.
            v_skippedColumnsArray = array_append(v_skippedColumnsArray, r_col.attname::text);
        ELSE
            v_nbColumns = v_nbColumns + 1;
            v_commonDscvValToInsert = quote_literal(v_schema) || ', ' || quote_literal(v_table) || ', ' || quote_literal(r_col.attname)
                || ', ' || r_col.attnum || ', ' || quote_literal(r_col.typname) || ', ' || coalesce(r_col.atttypmod::TEXT, 'NULL') || ', ';
-- Nullable columns.
            IF NOT r_col.attnotnull THEN
                v_colDefArray = array_append(v_colDefArray, 'nb_not_null_' || r_col.attnum::text || ' BIGINT');
                IF r_col.typname NOT IN ('text', 'bytea') THEN
                    v_aggregatesArray = array_append(v_aggregatesArray, 'count(' || upper(r_col.attname) || ') AS nb_not_null_' || r_col.attnum::text);
                ELSE
-- There is a special Oracle syntax for CLOB and BLOB columns !!!
                    v_aggregatesArray = array_append(v_aggregatesArray, 'count(CASE WHEN ' || upper(r_col.attname) || ' IS NULL THEN NULL ELSE 1 END) AS nb_not_null_' || r_col.attnum::text);
                END IF;
                v_nbNotNullValue = 'nb_not_null_' || r_col.attnum::text;
            ELSE
                v_nbNotNullValue = 'NULL::BIGINT';
            END IF;
            CASE
                WHEN r_col.typcategory = 'N' AND r_col.typname NOT IN ('numeric', 'float4', 'float8') THEN
-- Integer columns.
                    -- the lowest value
                    v_colDefArray = array_append(v_colDefArray, 'num_min_' || r_col.attnum::text || ' NUMERIC');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'min(' || upper(r_col.attname) || ') AS num_min_' || r_col.attnum::text);
                    -- the greatest value
                    v_colDefArray = array_append(v_colDefArray, 'num_max_' || r_col.attnum::text || ' NUMERIC');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(' || upper(r_col.attname) || ') AS num_max_' || r_col.attnum::text);
    
                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_num_min, dscv_num_max) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', num_min_' || r_col.attnum::text || ', num_max_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typcategory = 'N' AND r_col.typname = 'numeric' THEN
-- Numeric non integer columns.
                    -- the lowest value
                    v_colDefArray = array_append(v_colDefArray, 'num_min_' || r_col.attnum::text || ' NUMERIC');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'min(' || upper(r_col.attname) || ') AS num_min_' || r_col.attnum::text);
                    -- the greatest value
                    v_colDefArray = array_append(v_colDefArray, 'num_max_' || r_col.attnum::text || ' NUMERIC');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(' || upper(r_col.attname) || ') AS num_max_' || r_col.attnum::text);
                    -- the greatest needed precision
                    v_colDefArray = array_append(v_colDefArray, 'num_max_precision_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(coalesce(length(translate(to_char(' || upper(r_col.attname) || '), ''_-.,'', ''_'')) ,0)) AS num_max_precision_' || r_col.attnum::text);
                    -- the greatest needed integer part
                    v_colDefArray = array_append(v_colDefArray, 'num_max_integ_part_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(coalesce(length(to_char(abs(trunc(' || upper(r_col.attname) || ')))) ,0)) AS num_max_scale_' || r_col.attnum::text);
                    -- the greatest needed fractional part
                    v_colDefArray = array_append(v_colDefArray, 'num_max_fract_part_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(coalesce(length(to_char(abs(' || upper(r_col.attname) || ' - trunc(' || upper(r_col.attname) || ')))) - 1 ,0)) AS num_max_fract_part_' || r_col.attnum::text);

                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_num_min, dscv_num_max, dscv_num_max_precision, dscv_num_max_integ_part, dscv_num_max_fract_part) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', num_min_' || r_col.attnum::text || ', num_max_' || r_col.attnum::text || ', num_max_precision_' || r_col.attnum::text
                        || ', num_max_integ_part_' || r_col.attnum::text || ', num_max_fract_part_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typcategory = 'S' AND r_col.typname <> 'text' THEN
-- String types other than CLOB.
                    -- the average string length
                    v_colDefArray = array_append(v_colDefArray, 'str_avg_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'round(avg(length(' || upper(r_col.attname) || '))) AS str_avg_len_' || r_col.attnum::text);
                    -- the greatest string length
                    v_colDefArray = array_append(v_colDefArray, 'str_max_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(length(' || upper(r_col.attname) || ')) AS str_max_len_' || r_col.attnum::text);
                    -- the number of values with \x00 characters
                    v_colDefArray = array_append(v_colDefArray, 'str_nul_char_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'sum(case when instr(' || upper(r_col.attname) || ', chr(0)) > 0 then 1 else 0 end) AS str_nul_char_' || r_col.attnum::text);

                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_str_avg_length, dscv_str_max_length, dscv_str_nul_char) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', str_avg_len_' || r_col.attnum::text || ', str_max_len_' || r_col.attnum::text
                        || ', str_nul_char_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typname = 'text' THEN
-- TEXT representing CLOB columns.
                    -- the average string length
                    v_colDefArray = array_append(v_colDefArray, 'str_avg_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'round(avg(DBMS_LOB.GETLENGTH(' || upper(r_col.attname) || '))) AS str_avg_len_' || r_col.attnum::text);
                    -- the greatest string length
                    v_colDefArray = array_append(v_colDefArray, 'str_max_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(DBMS_LOB.GETLENGTH(' || upper(r_col.attname) || ')) AS str_max_len_' || r_col.attnum::text);
                    -- the number of values with \x00 characters
                    v_colDefArray = array_append(v_colDefArray, 'str_nul_char_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'sum(case when instr(' || upper(r_col.attname) || ', chr(0)) > 0 then 1 else 0 end) AS str_nul_char_' || r_col.attnum::text);

                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_str_avg_length, dscv_str_max_length, dscv_str_nul_char) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', str_avg_len_' || r_col.attnum::text || ', str_max_len_' || r_col.attnum::text
                        || ', str_nul_char_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typname = 'bytea' THEN
-- BYTEA representing BLOB columns.
                    -- the average string length
                    v_colDefArray = array_append(v_colDefArray, 'str_avg_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'round(avg(DBMS_LOB.GETLENGTH(' || upper(r_col.attname) || '))) AS str_avg_len_' || r_col.attnum::text);
                    -- the greatest string length
                    v_colDefArray = array_append(v_colDefArray, 'str_max_len_' || r_col.attnum::text || ' INTEGER');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'max(DBMS_LOB.GETLENGTH(' || upper(r_col.attname) || ')) AS str_max_len_' || r_col.attnum::text);

                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_str_avg_length, dscv_str_max_length) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', str_avg_len_' || r_col.attnum::text || ', str_max_len_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typcategory = 'D' AND r_col.typname NOT IN ('timestamp', 'timestamptz') THEN
-- Date or time columns.
                    -- the lowest value
                    v_colDefArray = array_append(v_colDefArray, 'ts_min_' || r_col.attnum::text || ' TEXT');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'to_char(min(' || upper(r_col.attname) || ')) AS ts_min_' || r_col.attnum::text);
                    -- the greatest value
                    v_colDefArray = array_append(v_colDefArray, 'ts_max_' || r_col.attnum::text || ' TEXT');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'to_char(max(' || upper(r_col.attname) || ')) AS ts_max_' || r_col.attnum::text);
	    
                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_ts_min, dscv_ts_max) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', ts_min_' || r_col.attnum::text || ', ts_max_' || r_col.attnum::text || ' FROM aggregates),');
                WHEN r_col.typcategory = 'D' AND r_col.typname IN ('timestamp', 'timestamptz') THEN
-- Timestamp columns.
                    -- the lowest value
                    v_colDefArray = array_append(v_colDefArray, 'ts_min_' || r_col.attnum::text || ' TEXT');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'to_char(min(' || upper(r_col.attname) || ')) AS ts_min_' || r_col.attnum::text);
                    -- the greatest value
                    v_colDefArray = array_append(v_colDefArray, 'ts_max_' || r_col.attnum::text || ' TEXT');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'to_char(max(' || upper(r_col.attname) || ')) AS ts_max_' || r_col.attnum::text);
                    -- the number of timestamp values with a '00:00:00' time component (i.e. real date)
                    v_colDefArray = array_append(v_colDefArray, 'ts_nb_date_' || r_col.attnum::text || ' BIGINT');
                    v_aggregatesArray = array_append(v_aggregatesArray, 'sum(case when to_char(' || upper(r_col.attname) || ', ''HH24:MI:SS'') = ''00:00:00'' then 1 else 0 end) AS ts_nb_date_' || r_col.attnum::text);

                    v_insertCteArray = array_append(v_insertCteArray, 'ins_' || r_col.attnum::text
                        || ' AS (INSERT INTO data2pg.discovery_column (' || v_commonDscvColToInsert || ' dscv_ts_min, dscv_ts_max, dscv_ts_nb_date) '
                        || 'SELECT ' || v_commonDscvValToInsert || v_nbNotNullValue
                        || ', ts_min_' || r_col.attnum::text || ', ts_max_' || r_col.attnum::text || ', ts_nb_date_' || r_col.attnum::text || ' FROM aggregates),');
                ELSE CONTINUE;
            END CASE;
        END IF;
    END LOOP;
    v_colDefList = array_to_string(v_colDefArray, ', ');
    v_aggregatesList = array_to_string(v_aggregatesArray, ', ');
    v_insertCteList = array_to_string(v_insertCteArray, '');
    v_skippedColumnsList = array_to_string(v_skippedColumnsArray, ', ');
--
-- Create the foreign table.
--
    IF v_maxRows IS NULL THEN
	    v_foreignTableQuery := format(
	    	'(SELECT %s FROM %I.%I)',
	    	v_aggregatesList, v_sourceSchema, v_sourceTable
	    );
    ELSE
	    v_foreignTableQuery := format(
	    	'(SELECT %s FROM (SELECT * FROM %I.%I WHERE ROWNUM <= %s))',
	    	v_aggregatesList, v_sourceSchema, v_sourceTable, v_maxRows
	    );
    END IF;
    v_stmt = format(
        'CREATE FOREIGN TABLE %I.%I (%s) SERVER %I OPTIONS ( table %L )',
        v_foreignSchema, v_aggForeignTable, v_colDefList, v_serverName, v_foreignTableQuery
    );
    EXECUTE v_stmt;
--
-- Analyze the Oracle table content by querying the foreign table.
--
    v_stmt = 'WITH aggregates AS ('
          || '  SELECT * FROM ' || quote_ident(v_foreignSchema) || '.' || quote_ident(v_aggForeignTable)
          || '  ), '
          || v_insertCteList
          || '     discovered_table AS ('
          || '  INSERT INTO @extschema@.discovery_table (dscv_schema, dscv_table, dscv_max_row, dscv_nb_row)'
          || '    SELECT ' || quote_literal(v_schema) || ', ' || quote_literal(v_table) || ', ' || coalesce(v_maxRows::TEXT, 'NULL') || ', nb_row'
          || '        FROM aggregates'
          || '  ) '
          || 'SELECT nb_row FROM aggregates';
-- Execute the first table scan. It creates a row per analyzed column into the discovery_column table
    EXECUTE v_stmt INTO v_nbRows;
-- Drop the foreign table, that is now useless
    EXECUTE format(
        'DROP FOREIGN TABLE %I.%I',
        v_foreignSchema, v_aggForeignTable
    );

--TODO: Get information about potential boolean columns

--
-- Generate advice.
--
-- Warnings at table level.
    IF v_nbRows = 0 THEN
        INSERT INTO @extschema@.discovery_advice
            (dscv_schema, dscv_table, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
            VALUES
            (v_schema, v_table, 'W', 'EMPTY_TABLE', 'The table ' || v_schema || '.' || v_table || ' is empty.');
    ELSE
        IF v_nbRows = v_maxRows THEN
            INSERT INTO @extschema@.discovery_advice
               (dscv_schema, dscv_table, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                VALUES
                (v_schema, v_table, 'W', 'NOT_ALL_ROWS', 'The table ' || v_schema || '.' || v_table || ' has un-analyzed rows.');
        END IF;
    END IF;
    IF v_skippedColumnsList <> '' THEN
        INSERT INTO @extschema@.discovery_advice
            (dscv_schema, dscv_table, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
            VALUES
            (v_schema, v_table, 'W', 'SKIPPED_COLUMNS', 'For the table ' || v_schema || '.' || v_table || ', columns ' || v_skippedColumnsList || 'have not been analyzed.');
    END IF;
-- Pieces of advice at column level.
    IF v_nbRows > 0 THEN
        FOR r_col IN
            SELECT c.*, t.dscv_nb_row
                FROM @extschema@.discovery_column c
                     JOIN @extschema@.discovery_table t ON (t.dscv_schema = c.dscv_schema AND t.dscv_table = c.dscv_table)
                WHERE dscv_nb_row > 0
                  AND t.dscv_schema = v_schema
                  AND t.dscv_table = v_table
        LOOP
            IF r_col.dscv_nb_not_null = 0 THEN
-- If all values are NULL, just issue a warning
                INSERT INTO @extschema@.discovery_advice
                   (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                    VALUES
                    (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'W', 'ONLY_NULL',
                     'The column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' contains nothing but NULL.');
            ELSE
-- On integer columns.
                IF r_col.dscv_num_max_fract_part = 0 THEN
                    IF r_col.dscv_num_max_integ_part <= 4 THEN
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'SMALLINT',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type SMALLINT.');
                    ELSIF r_col.dscv_num_max_integ_part <= 8 THEN
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'INTEGER', 
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type INTEGER.');
                    ELSIF r_col.dscv_num_max_integ_part <= 18 THEN
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'BIGINT',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type BIGINT.');
                    ELSE
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'TOO_LARGE_INTEGER',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' is a too large integer to be hold in a BIGINT column.');
                    END IF;
                    v_nbAdvice = v_nbAdvice + 1;
-- On numeric but not integer columns.
                ELSIF r_col.dscv_num_max_fract_part > 0 THEN
                    IF r_col.dscv_num_max_precision <= 7 THEN
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'REAL_OR_NUMERIC',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type REAL or NUMERIC or ' ||
                             'NUMERIC(' || r_col.dscv_num_max_integ_part + r_col.dscv_num_max_fract_part || ', ' || r_col.dscv_num_max_fract_part || ').');
                    ELSIF r_col.dscv_num_max_precision <= 15 THEN
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'DOUBLE_OR_NUMERIC',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type DOUBLE PRECISION or NUMERIC or ' ||
                             'NUMERIC(' || r_col.dscv_num_max_integ_part + r_col.dscv_num_max_fract_part || ', ' || r_col.dscv_num_max_fract_part || ').');
                    ELSE
                        INSERT INTO @extschema@.discovery_advice
                            (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                            VALUES
                            (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'NUMERIC',
                             'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                             'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type NUMERIC or ' ||
                             'NUMERIC(' || r_col.dscv_num_max_integ_part + r_col.dscv_num_max_fract_part || ', ' || r_col.dscv_num_max_fract_part || ').');
                    END IF;
                    v_nbAdvice = v_nbAdvice + 1;
                END IF;
-- On DATE columns.
                IF r_col.dscv_ts_nb_date = coalesce(r_col.dscv_nb_not_null, r_col.dscv_nb_row) THEN
                    INSERT INTO @extschema@.discovery_advice
                        (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                        VALUES
                        (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'DATE',
                         'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                         'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could be of type DATE.');
                    v_nbAdvice = v_nbAdvice + 1;
                END IF;
-- On boolean columns.
                IF r_col.dscv_has_2_values THEN
                    INSERT INTO @extschema@.discovery_advice
                        (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                        VALUES
                        (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'BOOLEAN',
                         'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                         'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column
                          || ' has less than 3 distinct values and could be transformed into BOOLEAN type.');
                    v_nbAdvice = v_nbAdvice + 1;
                END IF;
-- On columns without NULL but not declared NOT NULL.
                IF r_col.dscv_nb_not_null = least(r_col.dscv_nb_row, v_maxRows) THEN
                    INSERT INTO @extschema@.discovery_advice
                        (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                        VALUES
                        (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'NOT_NULL',
                         'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                         'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could perhaps be declared NOT NULL.');
                    v_nbAdvice = v_nbAdvice + 1;
                END IF;
-- On CHAR or VARCHAR columns whose max size is much larger (more than twice) than the maximum data content.
                IF r_col.dscv_max_size > 0 AND r_col.dscv_str_max_length > 2 * r_col.dscv_max_size THEN
                    INSERT INTO @extschema@.discovery_advice
                        (dscv_schema, dscv_table, dscv_column, dscv_column_num, dscv_advice_type, dscv_advice_code, dscv_advice_msg)
                        VALUES
                        (v_schema, v_table, r_col.dscv_column, r_col.dscv_column_num, 'A', 'TOO_LARGE_STRING',
                         'From ' || coalesce (r_col.dscv_nb_not_null, r_col.dscv_nb_row) || ' examined values, ' ||
                         'the column ' || v_schema || '.' || v_table || '.' || r_col.dscv_column || ' could perhaps be declared smaller (max size: real = '
                         || r_col.dscv_str_max_length || '/ declared = ' || r_col.dscv_max_size || ').');
                    v_nbAdvice = v_nbAdvice + 1;
                END IF;
            END IF;
        END LOOP;
    END IF;
--
-- Return the step report.
--
    r_output.sr_indicator = 'DISCOVERED_COLUMNS';
    r_output.sr_value = v_nbColumns;
    r_output.sr_rank = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'DISCOVERED_ROWS';
    r_output.sr_value = v_nbRows;
    r_output.sr_rank = 2;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'GENERATED_ADVICE';
    r_output.sr_value = v_nbAdvice;
    r_output.sr_rank = 3;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_discover_table$;

--
-- Service functions called by the Data2Pg scheduler.
--

-- The __get_batch_ids() function is called by the Data2Pg scheduler to get the list of all configured batches.
CREATE FUNCTION __get_batch_ids()
    RETURNS SETOF batch_id_type LANGUAGE sql AS
$__get_batch_ids$
SELECT bat_name, bat_type, bat_migration, mgr_config_completed
    FROM @extschema@.batch
         JOIN @extschema@.migration ON (bat_migration = mgr_name);
$__get_batch_ids$;

-- The __get_working_plan() function is called by the Data2Pg scheduler to build its working plan.
-- Only the data useful for its purpose are returned, through the working_plan_type structure.
CREATE FUNCTION __get_working_plan(
    p_batchName                TEXT
    )
    RETURNS SETOF working_plan_type LANGUAGE plpgsql AS
$__get_working_plan$
DECLARE
    v_migration              TEXT;
    v_isConfigCompleted      BOOLEAN;
BEGIN
-- Check that the batch exists and that the its migration is known as having a completed configuration.
-- The calling program is supposed to have already called the check_batch_id() function. But recheck to be sure.
    SELECT bat_migration, mgr_config_completed INTO v_migration, v_isConfigCompleted
        FROM @extschema@.batch
             JOIN @extschema@.migration ON (bat_migration = mgr_name)
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION '__get_working_plan: batch "%" not found.', p_batchName;
    END IF;
    IF NOT v_isConfigCompleted THEN
        RAISE EXCEPTION '__get_working_plan: the migration "%" configuration is not marked as completed. Execute the complete_migration_configuration() function.',
                        v_migration;
    END IF;
-- Deliver steps.
    RETURN QUERY
        SELECT stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_parents
            FROM @extschema@.step
            WHERE stp_batch_name = p_batchName;
    RETURN;
END;
$__get_working_plan$;

-- The __check_step_options() function is called by the Data2Pg scheduler. It checks the content of the step_options parameter presented in TEXT format.
-- It verifies that the syntax is JSON compatible and that the keywords and values are valid.
-- Input parameter: the step options, in TEXT format.
-- Output parameter: the error message, or an empty string when no problem is detected.
CREATE FUNCTION __check_step_options(
    p_stepOptions            TEXT
    )
    RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS
$__check_step_options$
DECLARE
    v_jsonStepOptions        JSONB;
    r_key                    RECORD;
BEGIN
-- Check that the parameter syntax is a proper JSON field.
    BEGIN
        v_jsonStepOptions = p_stepOptions::JSONB;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'The step options parameter ' || p_stepOptions || ' is not in a valid JSON format.';
    END;
-- Check each option.
    FOR r_key IN
        SELECT * FROM jsonb_object_keys(v_jsonStepOptions) AS t(key)
    LOOP
        CASE r_key.key
           WHEN 'COPY_MAX_ROWS' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COPY_MAX_ROWS') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'COPY_MAX_ROWS') <> 'null' THEN
                   RETURN 'The value for the COPY_MAX_ROWS step option must be a number.';
               END IF;
           WHEN 'COPY_PCT_ROWS' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COPY_PCT_ROWS') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'COPY_PCT_ROWS') <> 'null' THEN
                   RETURN 'The value for the COPY_PCT_ROWS step option must be a number.';
               END IF;
               IF (v_jsonStepOptions->>'COPY_PCT_ROWS')::real < 0 OR
                  (v_jsonStepOptions->>'COPY_PCT_ROWS')::real > 100 THEN
                   RETURN 'The value for the COPY_PCT_ROWS step option must be a real between 0 and 100.';
               END IF;
           WHEN 'COPY_SLOW_DOWN' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COPY_SLOW_DOWN') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'COPY_SLOW_DOWN') <> 'null' THEN
                   RETURN 'The value for the COPY_SLOW_DOWN step option must be a number.';
               END IF;
           WHEN 'COMPARE_MAX_DIFF' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COMPARE_MAX_DIFF') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'COMPARE_MAX_DIFF') <> 'null' THEN
                   RETURN 'The value for the COMPARE_MAX_DIFF step option must be a number.';
               END IF;
           WHEN 'COMPARE_MAX_ROWS' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COMPARE_MAX_ROWS') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'COMPARE_MAX_ROWS') <> 'null' THEN
                   RETURN 'The value for the COMPARE_MAX_ROWS step option must be a number.';
               END IF;
           WHEN 'DISCOVER_MAX_ROWS' THEN
               IF jsonb_typeof(v_jsonStepOptions->'DISCOVER_MAX_ROWS') <> 'number' AND
                  jsonb_typeof(v_jsonStepOptions->'DISCOVER_MAX_ROWS') <> 'null' THEN
                   RETURN 'The value for the DISCOVER_MAX_ROWS step option must be a number.';
               END IF;
           WHEN 'COMPARE_TRUNCATE_DIFF' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COMPARE_TRUNCATE_DIFF') <> 'boolean' AND
                  jsonb_typeof(v_jsonStepOptions->'COMPARE_TRUNCATE_DIFF') <> 'null' THEN
                   RETURN 'The value for the COMPARE_TRUNCATE_DIFF step option must be a boolean.';
               END IF;
           ELSE
               RETURN r_key.key || ' is not a known step option.';
        END CASE;
    END LOOP;
--
    RETURN '';
END;
$__check_step_options$;

-- The __terminate_data2pg_backends() function is called by the Data2Pg scheduler for its 'abort' actions.
-- It terminates Postgres backends that could be still in execution.
-- Input parameter: an array of the pids to terminate, if they are still in execution.
-- Output parameter: an array of the pids that have been effectively terminated.
CREATE FUNCTION __terminate_data2pg_backends(
    p_pids                   INT[]
    )
    RETURNS INT[] LANGUAGE SQL AS
$__terminate_data2pg_backends$
    SELECT array_agg(pid) FROM
        (SELECT pid, pg_terminate_backend(pid) AS has_been_terminated
            FROM pg_stat_activity
                WHERE pid = ANY ($1)
                  AND application_name = 'data2pg') AS t
        WHERE has_been_terminated;
$__terminate_data2pg_backends$;

--
-- Set the appropriate rights.
--
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA @extschema@ FROM public;

DO $$ BEGIN EXECUTE format('GRANT ALL ON DATABASE %s TO data2pg;', current_database()); END;$$;
GRANT ALL ON SCHEMA @extschema@ TO data2pg;
GRANT ALL ON ALL TABLES IN SCHEMA @extschema@ TO data2pg;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA @extschema@ TO data2pg;

-- Add the extension tables and sequences to the list of content that pg_dump has to save.
SELECT pg_catalog.pg_extension_config_dump('migration', '');
SELECT pg_catalog.pg_extension_config_dump('batch', '');
SELECT pg_catalog.pg_extension_config_dump('step', '');
SELECT pg_catalog.pg_extension_config_dump('table_to_process', '');
SELECT pg_catalog.pg_extension_config_dump('table_column', '');
SELECT pg_catalog.pg_extension_config_dump('table_index', '');
SELECT pg_catalog.pg_extension_config_dump('table_part', '');
SELECT pg_catalog.pg_extension_config_dump('sequence_to_process', '');
SELECT pg_catalog.pg_extension_config_dump('source_table_stat', '');
SELECT pg_catalog.pg_extension_config_dump('content_diff', '');
SELECT pg_catalog.pg_extension_config_dump('discovery_table', '');
SELECT pg_catalog.pg_extension_config_dump('discovery_column', '');
SELECT pg_catalog.pg_extension_config_dump('discovery_advice', '');
