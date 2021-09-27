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
-- The batch_id_type coposite type is used as output record for the get_batch_ids() function called by the Data2Pg scheduler.
CREATE TYPE batch_id_type AS (
    bi_batch_name              TEXT,                    -- The name of the batch
    bi_batch_type              TEXT,                    -- The batch type
    bi_mgr_name                TEXT,                    -- A name of the migration the batch belongs to
    bi_mgr_config_completed    BOOLEAN                  -- Boolean indicating whether the migration configuration is completed or not
);

-- The working_plan_type composite type is used as output record for the get_working_plan() function called by the Data2Pg scheduler.
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
    mgr_name                   VARCHAR(16) NOT NULL,    -- The migration name
    mgr_source_dbms            TEXT NOT NULL            -- The RDBMS as source database
                               CHECK (mgr_source_dbms IN ('Oracle', 'SQLServer', 'Sybase_ASA', 'PostgreSQL')),
    mgr_extension              TEXT NOT NULL,           -- Extension name
    mgr_server_name            TEXT NOT NULL,           -- The FDW server name
    mgr_server_options         TEXT NOT NULL,           -- The options for the server, like 'host ''localhost'', port ''5432'', dbname ''test1'''
    mgr_user_mapping_options   TEXT NOT NULL,           -- The user mapping options used by the data2pg role to reach the source database
                                                        --   like 'user ''postgres'', password ''pwd''', but the password is masked
    mgr_config_completed       BOOLEAN,                 -- Boolean indicating whether the migration configuration is completed or not
    PRIMARY KEY (mgr_name)
);

-- The batch table contains a row per batch, i.e. a set of tables and sequences processed by a single data2pg run.
CREATE TABLE batch (
    bat_name                   TEXT NOT NULL,           -- The batch name
    bat_migration              TEXT NOT NULL,           -- The migration the batch belongs to
    bat_type                   TEXT NOT NULL            -- The batch type, i.e. the type of action to perform
                               CHECK (bat_type IN ('COPY', 'CHECK', 'COMPARE')),
    bat_start_with_truncate    BOOLEAN,                 -- Boolean indicating whether a truncate step has to be included in the batch working plan
                                                        --   It only concerns batches of type COPY
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
                               CHECK (stp_type IN ('TRUNCATE', 'TABLE', 'SEQUENCE', 'TABLE_PART', 'FOREIGN_KEY')),
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
    stp_parents                TEXT[],                  -- A set of parent steps that need to be completed to allow the step to start
    PRIMARY KEY (stp_batch_name, stp_name),
    FOREIGN KEY (stp_batch_name) REFERENCES batch (bat_name)
);

-- The table_to_process table contains a row for table to process, with some useful data to migrate it.
CREATE TABLE table_to_process (
    tbl_schema                 TEXT NOT NULL,           -- The schema of the target table
    tbl_name                   TEXT NOT NULL,           -- The name of the target table
    tbl_migration              VARCHAR(16) NOT NULL,    -- The migration the table is linked to
    tbl_foreign_schema         TEXT NOT NULL,           -- The schema of the schema containing the foreign table representing the source table
    tbl_foreign_name           TEXT NOT NULL,           -- The name of the foreign table representing the source table
    tbl_rows                   BIGINT,                  -- The approximative number of rows of the source table
    tbl_kbytes                 FLOAT,                   -- The size in K-Bytes of the source table
    tbl_constraint_names       TEXT[],                  -- The constraints to drop before copying the table data
    tbl_constraint_definitions TEXT[],                  -- The constraints to recreate after copying the table data
    tbl_index_names            TEXT[],                  -- The indexes to drop before copying the table data
    tbl_index_definitions      TEXT[],                  -- The indexes to recreate after copying the table data
    tbl_copy_source_exprs      TEXT[],                  -- For a COPY batch, the columns to select
    tbl_copy_dest_cols         TEXT[],                  -- For a COPY batch, the columns to insert, in the same order as tbl_copy_source_exprs
    tbl_copy_sort_order        TEXT,                    -- The ORDER BY clause if needed for the INSERT SELECT copy statement (NULL if no sort)
    tbl_compare_source_exprs   TEXT[],                  -- For a COMPARE batch, the columns/expressions to select from the foreign table
    tbl_compare_dest_cols      TEXT[],                  -- For a COMPARE batch, the columns/expressions to select from the local postgres table,
                                                        --   in the same order as tbl_compare_source_exprs
    tbl_compare_pk_cols_jb     TEXT[],                  -- For a COMPARE batch, the expression to build the json object representing the pk columns in the diff table
    tbl_compare_other_cols_jb  TEXT[],                  -- For a COMPARE batch, the expression to build the json object representing the other columns in the diff table
    tbl_compare_sort_order     TEXT,                    -- The ORDER BY clause if needed for the INSERT SELECT compare statement
    tbl_some_gen_alw_id_col    BOOLEAN,                 -- TRUE when there are some "generated always as identity columns" in the table's definition
    tbl_referencing_tables     TEXT[],                  -- The other tables that are referenced by FKeys on this table
    tbl_referenced_tables      TEXT[],                  -- The other tables whose FKeys references this table
    PRIMARY KEY (tbl_schema, tbl_name),
    FOREIGN KEY (tbl_migration) REFERENCES migration (mgr_name)
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
    seq_migration              VARCHAR(16) NOT NULL,    -- The migration the sequence is linked to
    seq_foreign_schema         TEXT NOT NULL,           -- The schema of the schema containing the foreign table representing the source sequence
    seq_foreign_name           TEXT NOT NULL,           -- The name of the foreign table representing the source sequence
    seq_source_schema          TEXT NOT NULL,           -- The schema or user owning the sequence in the source database
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

-- The content_diff table is populated with the result of elementary compare_table and compare_sequence steps.
-- The table is just a report and has no pkey.
CREATE TABLE content_diff (
    diff_timestamp           TIMESTAMPTZ                 -- The transaction timestamp of the relation comparison
                             DEFAULT current_timestamp,
    diff_schema              TEXT NOT NULL,              -- The name of the destination schema
    diff_relation            TEXT NOT NULL,              -- The schema of the destination table used for the comparison
    diff_rank                BIGINT,                     -- A difference number
    diff_database            CHAR NOT NULL               -- The database the rows comes from ; either Source or Destination
                             CHECK (diff_database IN ('S', 'D')),
    diff_pkey_cols           JSON,                       -- The JSON representation of the table pk for rows that are different between both databases
    diff_other_cols          JSON                        -- The JSON representation of the table columns not in PK for rows that are different between
                                                         --   both databases or the sequence characteristics that are different between both databases
);

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
    p_migration               TEXT,
    p_sourceDbms             TEXT,
    p_extension              TEXT,
    p_serverOptions          TEXT,
    p_userMappingOptions     TEXT,
    p_userHasPrivileges      BOOLEAN DEFAULT false
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
                regexp_replace(p_userMappingOptions, '(password\s+'').*?('')', '\1########\2'));
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
        -- that will be used by the copy_sequence() function.
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
            SELECT p_migration, nspname, relname, reltuples, relpages * 8
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
    p_migration               VARCHAR(16)
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
    p_migration              VARCHAR(16),             -- Migration name
    p_batchType              TEXT,                    -- Batch type (either 'COPY', 'CHECK' or 'COMPARE')
    p_startWithTruncate      BOOLEAN                  -- Boolean indicating whether a truncate step will need to be added to the batch working plan
    )
    RETURNS INTEGER LANGUAGE plpgsql AS
$create_batch$
DECLARE
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
-- Check that the migration exists and set it as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'create_batch: The migration "%" does not exist.', p_migration;
    END IF;
-- Check the batch type.
    IF p_batchType <> 'COPY' AND p_batchType <> 'CHECK' AND p_batchType <> 'COMPARE' THEN
        RAISE EXCEPTION 'create_batch: Illegal batch type (%). It must be eiter COPY or CHECK or COMPARE.', p_batchType;
    END IF;
-- If the p_startWithTruncate boolean is TRUE, check that the batch is of type COPY.
    IF p_startWithTruncate AND p_batchType <> 'COPY' THEN
        RAISE EXCEPTION 'create_batch: A batch of type % cannot start with a truncate step.', p_batchType;
    END IF;
-- Checks are OK.
-- Record the batch into the batch table.
    INSERT INTO @extschema@.batch (bat_name, bat_migration, bat_type, bat_start_with_truncate)
        VALUES (p_batchName, p_migration, p_batchType, coalesce(p_startWithTruncate, FALSE));
-- If the batch needs a truncate step, add it into the step table.
    IF p_batchType = 'COPY' AND p_startWithTruncate THEN
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_sql_function, stp_cost)
            VALUES ('TRUNCATE_' || p_migration, p_batchName, 'TRUNCATE', 'truncate_all', 1);
    END IF;
    IF p_batchType = 'COMPARE' THEN
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_sql_function, stp_cost)
            VALUES ('TRUNCATE_DIFF', p_batchName, 'TRUNCATE', 'truncate_content_diff', 1);
    END IF;
--
    RETURN 1;
END;
$create_batch$;

-- The drop_batch() function remove all components linked to a given batch.
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
-- A foreign table is created for each table.
-- The schema to hold foreign objects is created if needed.
-- Some characteristics of the table are recorded into the table_to_process table.
CREATE FUNCTION register_tables(
    p_migration              TEXT,               -- The migration linked to the tables
    p_schema                 TEXT,               -- The schema which tables have to be assigned to the batch
    p_tablesToInclude        TEXT,               -- Regexp defining the tables to assign for the schema
    p_tablesToExclude        TEXT,               -- Regexp defining the tables to exclude (NULL to exclude no table)
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL,
    p_sourceTableStatLoc     TEXT                -- The data2pg table that contains statistics about these target tables
                             DEFAULT 'source_table_stat',
    p_createForeignTable     BOOLEAN             -- Boolean indicating whether the FOREIGN TABLE have to be created
                             DEFAULT TRUE,       --   (if FALSE, an external operation must create them before launching a migration)
    p_sortByPKey             BOOLEAN             -- Boolean indicating whether the source data must be sorted on PKey at migration time
                             DEFAULT FALSE       --   (they are sorted anyway if a clustered index exists)
    )
    RETURNS INT LANGUAGE plpgsql                 -- returns the number of effectively assigned tables
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$register_tables$
DECLARE
    v_serverName             TEXT;
    v_sourceDbms             TEXT;
    v_sourceSchema           TEXT;
    v_foreignSchema          TEXT;
    v_pgVersion              INT;
    v_nbTables               INT;
    v_prevMigration          TEXT;
    v_copySortOrder          TEXT;
    v_compareSortOrder       TEXT;
    v_indexToDropNames       TEXT[];
    v_indexToDropDefs        TEXT[];
    v_constraintToDropNames  TEXT[];
    v_constraintToDropDefs   TEXT[];
    v_stmt                   TEXT;
    v_pkColList              TEXT;
    v_columnsInPkJB          TEXT[];
    v_columnsNotInPkJB       TEXT[];
    v_columnsToCopy          TEXT[];
    v_columnsToCompare       TEXT[];
    v_nbGenAlwaysIdentCol    INT;
    v_nbGenAlwaysExprCol     INT;
    v_referencingTables      TEXT[];
    v_referencedTables       TEXT[];
    v_sourceRows             BIGINT;
    v_sourceKBytes           FLOAT;
    r_tbl                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_tables: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check that the migration exists and get its server name and source DBMS.
    SELECT mgr_server_name, mgr_source_dbms INTO v_serverName, v_sourceDbms
        FROM @extschema@.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_tables: Migration "%" not found.', p_migration;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = p_migration;
-- Check that the schema exists.
    PERFORM 0 FROM pg_catalog.pg_namespace
        WHERE nspname = p_schema;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_tables: Schema "%" not found.', p_schema;
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
-- Get the selected tables.
    v_nbTables = 0;
    FOR r_tbl IN
        SELECT relname, pg_class.oid AS table_oid, pg_namespace.oid AS schema_oid
            FROM pg_catalog.pg_class
                 JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = p_schema
              AND relkind = 'r'
              AND relname ~ p_tablesToInclude
              AND (p_tablesToExclude IS NULL OR relname !~ p_tablesToExclude)
    LOOP
        v_nbTables = v_nbTables + 1;
-- Check that the table is not already registered for another migration.
        SELECT tbl_migration INTO v_prevMigration
            FROM @extschema@.table_to_process
            WHERE tbl_schema = p_schema
              AND tbl_name = r_tbl.relname;
        IF FOUND THEN
            RAISE EXCEPTION 'register_tables: The table %.% is already assigned to the migration %.',
                            p_schema, r_tbl.relname, v_prevMigration;
        END IF;
-- Look at the indexes associated to the table.
        v_copySortOrder = NULL;
-- Get the clustered index columns list, if it exists.
-- This list will be used in an ORDER BY clause in the table copy function.
        SELECT substring(pg_get_indexdef(pg_class.oid) FROM ' USING .*\((.+)\)') INTO v_copySortOrder
            FROM pg_catalog.pg_index
                 JOIN pg_catalog.pg_class ON (pg_class.oid = indexrelid)
            WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
              AND indisclustered;
-- If no clustered index exists and the sort is allowed on PKey, get the primary key index columns list, if it exists.
        IF v_copySortOrder IS NULL AND p_sortByPKey THEN
            SELECT substring(pg_get_indexdef(pg_class.oid) FROM ' USING .*\((.+)\)') INTO v_copySortOrder
                FROM pg_catalog.pg_index
                     JOIN pg_catalog.pg_class ON (pg_class.oid = indexrelid)
                WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
                  AND indisprimary;
        END IF;
-- Build the array of constraints to drop and their definition.
-- These constraints are of type PKEY, UNIQUE or EXCLUDE
        SELECT array_agg(conname), array_agg(constraint_def) INTO v_constraintToDropNames, v_constraintToDropDefs
            FROM (
                SELECT c1.conname, pg_get_constraintdef(c1.oid) AS constraint_def
                    FROM pg_catalog.pg_constraint c1
                    WHERE c1.conrelid = r_tbl.table_oid
                      AND c1.contype IN ('p', 'u', 'x')
----                      AND c1.contype IN ('u', 'x')
                      AND NOT EXISTS(                             -- the index linked to this constraint must not be linked to other constraints
                          SELECT 0 FROM pg_catalog.pg_constraint c2
                          WHERE c2.conindid = c1.conindid AND c2.oid <> c1.oid
                          )
                    ORDER BY conname
                 ) AS t;
-- Build the array of indexes to drop and their definition.
-- These indexes are not clustered indexes that are not linked to any constraint (pkey or others).
        SELECT array_agg(index_name), array_agg(index_def) INTO v_indexToDropNames, v_indexToDropDefs
            FROM (
                SELECT relname AS index_name, pg_get_indexdef(pg_class.oid) AS index_def
                    FROM pg_catalog.pg_index
                         JOIN pg_catalog.pg_class ON (pg_class.oid = indexrelid)
                    WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
                      AND NOT indisclustered
                      AND NOT EXISTS(                             -- the index must not be linked to any constraint
                          SELECT 0 FROM pg_catalog.pg_constraint
                          WHERE pg_constraint.conindid = pg_class.oid
                          )
                    ORDER BY relname
                 ) AS t;
-- Get the PK columns in two formats: 
--  * a list of literals, for the next statement
--  * a list of identifiers, for the ORDER BY to use in the compare_table() function
--  * an array of expressions to build the pk json structure in the compare_table() function.
        SELECT string_agg(quote_literal(attname), ','), string_agg(quote_ident(attname), ','),
               array_agg(quote_literal(attname) || ',' || quote_ident(attname))
          INTO v_pkColList, v_compareSortOrder,
               v_columnsInPkJB
            FROM pg_catalog.pg_attribute
                JOIN pg_catalog.pg_index ON (pg_index.indrelid = pg_attribute.attrelid)
            WHERE attnum = ANY (indkey)
              AND indrelid = r_tbl.table_oid
              AND indisprimary
              AND attnum > 0
              AND attisdropped = FALSE;
-- Build the list of columns to copy and columns to compare and some indicators to store into the table_to_process table.
        v_stmt = 'SELECT array_agg(quote_ident(attname)),'
--                           the columns array for the table comparison step
                 '       array_agg(quote_ident(attname)) FILTER (WHERE attgenerated = ''''),'
--                           the columns array for the table copy step, excluding the GENERATED ALWAYS AS (expression) columns
                 '       array_agg(quote_literal(attname) || '','' || quote_ident(attname)) FILTER (WHERE NOT (attname = ANY (ARRAY[%s]::TEXT[]) )),'
--                           the array of expressions to build the non pk json structure in the compare_table() function
                 '       count(*) FILTER (WHERE attidentity = ''a''),'
--                           the number of GENERATED ALWAYS AS IDENTITY columns
                 '       count(*) FILTER (WHERE attgenerated <> '''')'
--                           the number of GENERATED ALWAYS AS (expression) columns
                 '    FROM ('
                 '        SELECT attname, %s AS attidentity, %s AS attgenerated'
                 '            FROM pg_catalog.pg_attribute'
                 '            WHERE attrelid = %s'
                 '              AND attnum > 0 AND NOT attisdropped'
                 '            ORDER BY attnum) AS t';
        EXECUTE format(v_stmt,
                       v_pkColList,
                       CASE WHEN v_pgVersion >= 100000 THEN 'attidentity' ELSE '''''::TEXT' END,
                       CASE WHEN v_pgVersion >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                       r_tbl.table_oid)
            INTO v_columnsToCompare, v_columnsToCopy, v_columnsNotInPkJB, v_nbGenAlwaysIdentCol, v_nbGenAlwaysExprCol;
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
        EXECUTE format (
            'SELECT stat_rows, stat_kbytes
                FROM @extschema@.%I
                WHERE stat_schema = %L
                  AND stat_table = %L',
            p_sourceTableStatLoc, v_sourceSchema,
            CASE WHEN v_sourceDbms = 'Oracle' THEN UPPER(r_tbl.relname) ELSE r_tbl.relname END)
            INTO v_sourceRows, v_sourceKBytes;
-- Register the table into the table_to_process table.
        INSERT INTO @extschema@.table_to_process (
                tbl_schema, tbl_name, tbl_migration, tbl_foreign_schema, tbl_foreign_name,
                tbl_rows, tbl_kbytes,
                tbl_constraint_names, tbl_constraint_definitions, tbl_index_names, tbl_index_definitions,
                tbl_copy_dest_cols, tbl_copy_source_exprs, tbl_copy_sort_order,
                tbl_compare_source_exprs, tbl_compare_dest_cols,  tbl_compare_sort_order,
                tbl_compare_pk_cols_jb, tbl_compare_other_cols_jb, tbl_some_gen_alw_id_col,
                tbl_referencing_tables, tbl_referenced_tables
            ) VALUES (
                p_schema, r_tbl.relname, p_migration, v_foreignSchema, r_tbl.relname,
                coalesce(v_sourceRows, 0), coalesce(v_sourceKBytes, 0),
                v_constraintToDropNames, v_constraintToDropDefs, v_indexToDropNames, v_indexToDropDefs,
                v_columnsToCopy, v_columnsToCopy, v_copySortOrder,
                v_columnsToCompare, v_columnsToCompare, coalesce(v_compareSortOrder, array_to_string(v_columnsToCompare, ',')),
                v_columnsInPkJB, v_columnsNotInPkJB, (v_nbGenAlwaysIdentCol > 0),
                v_referencingTables, v_referencedTables
            );
-- Create the foreign table mapped on the table in the source database.
--    For Oracle, the source schema name is forced in upper case.
        IF p_createForeignTable THEN
            EXECUTE format(
                'IMPORT FOREIGN SCHEMA %s LIMIT TO (%I) FROM SERVER %I INTO %I',
                CASE WHEN v_sourceDbms = 'Oracle' THEN '"' || v_sourceSchema || '"' ELSE quote_ident(v_sourceSchema) END,
                r_tbl.relname, v_serverName, v_foreignSchema);
        END IF;
    END LOOP;
-- If no table has been selected, raise an exception.
    IF v_nbTables = 0 THEN
        RAISE EXCEPTION 'register_tables: No table has been found in the schema "%" using the provided selection criteria.', p_schema;
    END IF;
--
    RETURN v_nbTables;
END;
$register_tables$;

-- The register_column_transform_rule() functions defines a column change from the source table to the destination table.
-- It allows to manage columns with different names, with different types and or with specific computation rule.
-- The target column is defined with the schema, table and column name.
-- Several transformation rule may be applied for the same column. In this case, the p_column parameter of the second rule must be the p_expression of the first one.
CREATE FUNCTION register_column_transform_rule(
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_column                 TEXT,               -- The column name as it would appear in the INSERT SELECT statement of the copy processing
    p_expression             TEXT                -- The column name as it will appear in the INSERT SELECT statement of the copy processing. It may be
                                                 --   another column name if the column is renamed or an expression if the column content requires a
                                                 --   transformation rule. The column name may need to be double-quoted.
    )
    RETURNS VOID LANGUAGE plpgsql AS
$register_column_transform_rule$
DECLARE
    v_migrationName          TEXT;
    v_copyExpressions        TEXT[];
    v_compareExpressions     TEXT[];
    v_colPosition            INTEGER;
BEGIN
-- Check that no parameter is not NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_column IS NULL OR p_expression IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: None of the input parameters can be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name and select columns array for both COPY and COMPARE steps.
    SELECT tbl_migration, tbl_copy_source_exprs, tbl_compare_source_exprs INTO v_migrationName, v_copyExpressions, v_compareExpressions
        FROM @extschema@.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_transform_rule: Table %.% not found.', p_schema, p_table;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Look in the tables copy column array if the source column exists, and apply the change.
    v_colPosition = array_position(v_copyExpressions, p_column);
    IF v_colPosition IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: The column % is not found in the list of columns to copy %.', p_column, v_copyExpressions;
    END IF;
    v_copyExpressions[v_colPosition] = p_expression;
-- Look in the tables compare column array if the source column exists, and apply the change.
    v_colPosition = array_position(v_compareExpressions, p_column);
    IF v_colPosition IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: The column % is not found in the list of columns to compare %.', p_column, v_compareExpressions;
    END IF;
    v_compareExpressions[v_colPosition] = p_expression;
-- Record the changes.
    UPDATE @extschema@.table_to_process
        SET tbl_copy_source_exprs = v_copyExpressions, tbl_compare_source_exprs = v_compareExpressions
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
--
    RETURN;
END;
$register_column_transform_rule$;

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
    RETURNS INT LANGUAGE plpgsql AS              -- returns the number of effectively assigned table part, i.e. 1
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
       WHERE mgr_name = v_migrationName;
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
    p_sequencesToInclude     TEXT,               -- Regexp defining the sequences to assign for the schema
    p_sequencesToExclude     TEXT,               -- Regexp defining the sequences to exclude (NULL to exclude no sequence)
    p_sourceSchema           TEXT                -- The schema or user name in the source database (equals p_schema if NULL)
                             DEFAULT NULL
    )
    RETURNS INTEGER LANGUAGE plpgsql             -- returns the number of effectively registered sequences
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$register_sequences$
DECLARE
    v_serverName             TEXT;
    v_sourceDbms             TEXT;
    v_nbSequences            INT;
    v_foreignSchema          TEXT;
    v_prevMigration          TEXT;
    r_seq                    RECORD;
BEGIN
-- Check that the first 3 parameters are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_sequencesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_sequences: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the migration exists and get its server name and source DBMS.
    SELECT mgr_server_name, mgr_source_dbms INTO v_serverName, v_sourceDbms
        FROM @extschema@.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_sequences: Migration "%" not found.', p_migration;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = p_migration;
-- Check that the schema exists.
    PERFORM 0 FROM pg_catalog.pg_namespace
        WHERE nspname = p_schema;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_sequences: Schema % not found.', p_schema;
    END IF;
-- Create the foreign schema if it does not exist.
    v_foreignSchema = 'srcdb_' || p_schema;
    EXECUTE format(
        'CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION data2pg',
        v_foreignSchema);
-- Get the selected sequences.
    v_nbSequences = 0;
    FOR r_seq IN
        SELECT relname
            FROM pg_catalog.pg_class
                 JOIN pg_catalog.pg_namespace ON (relnamespace = pg_namespace.oid)
            WHERE nspname = p_schema
              AND relkind = 'S'
              AND relname ~ p_sequencesToInclude
              AND (p_sequencesToExclude IS NULL OR relname !~ p_sequencesToExclude)
    LOOP
        v_nbSequences = v_nbSequences + 1;
-- Check that the sequence is not already assigned to another migration.
        SELECT seq_migration INTO v_prevMigration
            FROM @extschema@.sequence_to_process
            WHERE seq_schema = p_schema
              AND seq_name = r_seq.relname;
        IF FOUND THEN
            RAISE EXCEPTION 'register_sequences: The sequence %.% is already assigned to the migration %.',
                            p_schema, r_seq.relname, v_prevMigration;
        END IF;
-- For PostgreSQL source database only,
-- Create the foreign table mapped on the sequence in the source database to get its current value.
        IF v_sourceDbms = 'PostgreSQL' THEN
            EXECUTE format(
                'CREATE FOREIGN TABLE %I.%I (last_value BIGINT, is_called BOOLEAN)'
                '    SERVER %I OPTIONS (schema_name %L)',
                v_foreignSchema, r_seq.relname, v_serverName, coalesce(p_sourceSchema, p_schema));
            EXECUTE format(
                'ALTER FOREIGN TABLE %I.%I OWNER TO data2pg',
                v_foreignSchema, r_seq.relname);
        END IF;
-- Register the sequence into the sequence_to_process table.
        INSERT INTO @extschema@.sequence_to_process (
                seq_schema, seq_name, seq_migration, seq_foreign_schema, seq_foreign_name, seq_source_schema
            ) VALUES (
                p_schema, r_seq.relname, p_migration, v_foreignSchema, r_seq.relname, coalesce(p_sourceSchema, p_schema)
            );
    END LOOP;
-- If no sequence has been selected, raise an exception.
    IF v_nbSequences = 0 THEN
        RAISE EXCEPTION 'register_sequences: No sequence has been found in the schema "%" using the provided selection criteria.', p_schema;
    END IF;
--
    RETURN v_nbSequences;
END;
$register_sequences$;

-- The assign_tables_to_batch() function assigns a set of tables of a single schema to a batch.
-- Two regexp filter tables already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_tables_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema which tables have to be assigned to the batch
    p_tablesToInclude        TEXT,               -- Regexp defining the tables to assign for the schema
    p_tablesToExclude        TEXT                -- Regexp defining the tables to exclude (NULL to exclude no table)
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- returns the number of effectively assigned tables
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
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_tables_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
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
-- Check that the table is not already fully assigned to a batch of the same type.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM @extschema@.step
                 JOIN @extschema@.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || r_tbl.tbl_name
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_tables_to_batch: The table %.% is already assigned to the batch "%".',
                            p_schema, r_tbl.tbl_name, v_prevBatchName;
        END IF;
-- Check that the table has no table part already assigned to any batch of the same type.
        PERFORM 0
            FROM @extschema@.step
                 JOIN @extschema@.batch ON (bat_name = stp_batch_name)
            WHERE stp_name LIKE p_schema || '.' || r_tbl.tbl_name || '.%'
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_tables_to_batch: The table %.% has at least 1 part already assigned to a batch of type %.',
                            p_schema, r_tbl.tbl_name, v_batchType;
        END IF;
-- Register the table into the step table.
        INSERT INTO @extschema@.step (
                stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                stp_sql_function, stp_cost
            ) VALUES (
                p_schema || '.' || r_tbl.tbl_name, p_batchName, 'TABLE', p_schema, r_tbl.tbl_name,
                CASE v_batchType
                    WHEN 'COPY' THEN 'copy_table'
                    WHEN 'CHECK' THEN 'check_table'
                    WHEN 'COMPARE' THEN 'compare_table'
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

-- The assign_table_part_to_batch() function assigns a table's part to a batch.
-- Two regexp filter tables already registered to a migration to include and exclude to the batch.
CREATE FUNCTION assign_table_part_to_batch(
    p_batchName              TEXT,               -- Batch identifier
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_partNum                INTEGER             -- The part number to assign
    )
    RETURNS INTEGER LANGUAGE plpgsql AS          -- returns the number of effectively assigned table part, ie. 1
$assign_table_part_to_batch$
DECLARE
    v_migrationName          TEXT;
    v_batchType              TEXT;
    v_rows                   BIGINT;
    v_kbytes                 FLOAT;
    v_prevBatchName          TEXT;
BEGIN
-- Check that all parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_table IS NULL OR p_partNum IS NULL THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: No input parameter can be NULL.';
    END IF;
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Check that the table part has been registered and get the table statistics.
    SELECT tbl_rows, tbl_kbytes INTO v_rows, v_kbytes
        FROM @extschema@.table_part
             JOIN @extschema@.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
        WHERE prt_schema = p_schema AND prt_table = p_table AND prt_number = p_partNum;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% has not been registered.',
                        p_partNum, p_schema, p_table;
    END IF;
-- Check that the table part has not been already assigned to a batch of the same type.
    SELECT stp_batch_name INTO v_prevBatchName
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_name = p_schema || '.' || p_table || '.' || p_partNum
          AND bat_type = v_batchType;
    IF FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% is already assigned to the batch %.',
                        p_partNum, p_schema, p_table, v_prevBatchName;
    END IF;
-- Check that the table is not already fully assigned to a batch of the same type.
    SELECT stp_batch_name INTO v_prevBatchName
        FROM @extschema@.step
             JOIN @extschema@.batch ON (bat_name = stp_batch_name)
        WHERE stp_name = p_schema || '.' || p_table
          AND bat_type = v_batchType;
    IF FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The table %.% is already assigned to the batch "%".',
                        p_schema, p_table, v_prevBatchName;
    END IF;
-- Register the table part into the step table.
    INSERT INTO @extschema@.step (
            stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_part_num,
            stp_sql_function, stp_cost
        ) VALUES (
            p_schema || '.' || p_table || '.' || p_partNum, p_batchName, 'TABLE_PART', p_schema, p_table, p_partNum,
            CASE v_batchType
                WHEN 'COPY' THEN 'copy_table'
                WHEN 'CHECK' THEN 'check_table'
                WHEN 'COMPARE' THEN 'compare_table'
            END,
            v_kbytes
        );
--
    RETURN 1;
END;
$assign_table_part_to_batch$;

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
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Check taht the batch is not of type 'CHECK'.
    IF v_batchType = 'CHECK' THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: sequences cannot be assigned to a batch of type %.', p_batchType;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
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
-- Check that the sequence is not already assigned to a batch of the same type.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM @extschema@.step
                 JOIN @extschema@.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || r_seq.seq_name
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_sequences_to_batch: The sequence %.% is already assigned to the batch %.',
                            p_schema, r_seq.seq_name, v_prevBatchName;
        END IF;
-- Register the sequence into the step table.
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_sql_function, stp_cost)
            VALUES (
                p_schema || '.' || r_seq.seq_name, p_batchName, 'SEQUENCE', p_schema, r_seq.seq_name,
                CASE v_batchType
                    WHEN 'COPY' THEN 'copy_sequence'
                    WHEN 'COMPARE' THEN 'compare_sequence'
                END,
                10);
    END LOOP;
--
    RETURN v_nbSequences;
END;
$assign_sequences_to_batch$;

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
    v_migrationName          TEXT;
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
-- Check that the batch exists and is of type COPY and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM @extschema@.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: batch "%" not found.', p_batchName;
    END IF;
    IF v_batchType <> 'COPY' THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: batch "%" is of type %. FK ckecks can only be assigned to batches of type COPY.', p_batchName, v_batchType;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE @extschema@.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
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
-- Register the sequence into the step table.
        INSERT INTO @extschema@.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                                  stp_sub_object, stp_sql_function, stp_cost)
            VALUES (p_schema || '.' || p_table || '.' || r_fk.conname, p_batchName, 'FOREIGN_KEY', p_schema, p_table,
                    r_fk.conname, 'check_fkey', v_cost);
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

-- The complete_migration_configuration() function is the final function in migration's configuration.
-- It checks that all registered and assigned data are consistent and builds the chaining constraints between steps.
CREATE FUNCTION complete_migration_configuration(
    p_migration              VARCHAR(16)
    )
    RETURNS VOID LANGUAGE plpgsql AS
$complete_migration_configuration$
DECLARE
    v_batchArray             TEXT[];
    v_countStartWithTruncate INT;
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
-- Get the list of related batches and check that the number of batches marked as the first one is exactly 1.
    SELECT array_agg(bat_name), count(bat_name) FILTER (WHERE bat_start_with_truncate)
        INTO v_batchArray, v_countStartWithTruncate
        FROM @extschema@.batch
        WHERE bat_migration = p_migration;
    IF v_countStartWithTruncate <> 1 THEN
        RAISE WARNING 'complete_migration_configuration: % batches are declared as starting with a TRUNCATE step. It is usualy 1', v_countStartWithTruncate;
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
-- Check that all functions referenced in the step table for the migration exist and will be callable by the scheduler.
    FOR r_function IN
        SELECT DISTINCT stp_sql_function || '(TEXT, TEXT, JSONB)' AS function_prototype
            FROM @extschema@.step
            WHERE stp_batch_name = ANY (v_batchArray)
    LOOP
-- If the function (or the data2pg role) does not exist, trying to check the privilege will raise a standart postgres exception.
        IF NOT has_function_privilege('data2pg', r_function.function_prototype, 'execute') THEN
            RAISE EXCEPTION 'complete_migration_configuration: The function % will not be callable by the data2pg scheduler.', r_function.function_prototype;
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
-- The table parts set as the last step for their table must have all the other parts of the same batch as parents.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_part_num
            FROM @extschema@.step
                 JOIN @extschema@.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'TABLE_PART'
              AND prt_is_last_step
    LOOP
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
-- Process the chaining constraints related to the TRUNCATE step for the COPY batches starting with a TRUNCATE.
    UPDATE @extschema@.step
        SET stp_parents = ARRAY['TRUNCATE_' || p_migration]
        FROM @extschema@.batch
        WHERE stp_batch_name = bat_name
          AND bat_type = 'COPY'
          AND bat_start_with_truncate
          AND stp_type IN ('TABLE', 'TABLE_PART')
          AND stp_parents IS NULL;
-- Process the chaining constraints related to the TRUNCATE step for the COMPARE batches starting with a TRUNCATE.
    UPDATE @extschema@.step
        SET stp_parents = ARRAY['TRUNCATE_DIFF']
        FROM @extschema@.batch
        WHERE stp_batch_name = bat_name
          AND bat_type = 'COMPARE'
          AND stp_type IN ('TABLE', 'TABLE_PART', 'SEQUENCE')
          AND stp_parents IS NULL;
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
-- Compute the cost of the Truncate step, now that the number of tables to truncate is known.
    UPDATE @extschema@.step
        SET stp_cost = (
             SELECT 10 * count(*)
                 FROM @extschema@.table_to_process

                 WHERE tbl_migration = p_migration
                       )
        WHERE stp_name = 'TRUNCATE_' || p_migration;
--
    RETURN;
END;
$complete_migration_configuration$;

--
-- Functions called by the data2pg scheduler.
--

-- The copy_table() function is the generic copy function that is used to processes tables.
-- Input parameters: batch and step names.
-- It returns a step report including the number of copied rows.
-- It is set as session_replication_role = 'replica', so that no check are performed on foreign keys and no regular trigger are executed.
CREATE FUNCTION copy_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SET session_replication_role = 'replica'
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$copy_table$
DECLARE
    v_rowsThreshold            CONSTANT BIGINT = 10000;      -- The estimated number of rows limit to drop and recreate the secondary indexes
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_partCondition            TEXT;
    v_isFirstStep              BOOLEAN = TRUE;
    v_isLastStep               BOOLEAN = TRUE;
    v_copyMaxRows              BIGINT;
    v_copySlowDown             BIGINT;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_estimatedNbRows          BIGINT;
    v_copySortOrder            TEXT;
    v_constraintToDropNames    TEXT[];
    v_constraintToCreateDefs   TEXT[];
    v_indexToDropNames         TEXT[];
    v_indexToCreateDefs        TEXT[];
    v_insertColList            TEXT;
    v_selectExprList           TEXT;
    v_someGenAlwaysIdentCol    BOOLEAN;
    v_constraint               TEXT;
    v_i                        INT;
    v_index                    TEXT;
    v_indexDef                 TEXT;
    v_stmt                     TEXT;
    v_nbRows                   BIGINT = 0;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the table.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM @extschema@.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'copy_table: Step % not found in the step table.', p_step;
    END IF;
-- Read the table_to_process table to get additional details.
    SELECT tbl_foreign_schema, tbl_foreign_name, tbl_rows, tbl_copy_sort_order,
           tbl_constraint_names, tbl_constraint_definitions, tbl_index_names, tbl_index_definitions,
           array_to_string(tbl_copy_dest_cols, ','), array_to_string(tbl_copy_source_exprs, ','), tbl_some_gen_alw_id_col
        INTO v_foreignSchema, v_foreignTable, v_estimatedNbRows, v_copySortOrder,
             v_constraintToDropNames, v_constraintToCreateDefs, v_indexToDropNames, v_indexToCreateDefs,
             v_insertColList, v_selectExprList, v_someGenAlwaysIdentCol
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- If the step concerns a table part, get additional details about the part.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition, prt_is_first_step, prt_is_last_step
            INTO v_partCondition, v_isFirstStep, v_isLastStep
            FROM @extschema@.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
    END IF;
-- Analyze the step options.
    v_copyMaxRows = p_stepOptions->>'COPY_MAX_ROWS';
    v_copySlowDown = p_stepOptions->>'COPY_SLOW_DOWN';
--
-- Pre-processing.
--
    IF v_isFirstStep THEN
-- Drop the constraints known as 'to be dropped' (unique, exclude).
        IF v_constraintToDropNames IS NOT NULL AND v_estimatedNbRows > v_rowsThreshold THEN
            FOREACH v_constraint IN ARRAY v_constraintToDropNames
            LOOP
                EXECUTE format(
                    'ALTER TABLE %I.%I DROP CONSTRAINT %I',
                    v_schema, v_table, v_constraint
                );
            END LOOP;
        END IF;
-- Drop the non pkey or clustered indexes if it is worth to o it, i.e. there are more than v_rowsThreshold rows to process.
        IF v_indexToDropNames IS NOT NULL AND v_estimatedNbRows > v_rowsThreshold THEN
            FOREACH v_index IN ARRAY v_indexToDropNames
            LOOP
                EXECUTE format(
                    'DROP INDEX %I.%I',
                    v_schema, v_index
                );
            END LOOP;
        END IF;
----TODO: to study in the future for the performances
------ truncate the destination table. The CASCADE clause is used to also truncate tables that are referencing this table with FKeys, these
------   tables being supposed to be processed after this one.
----        EXECUTE format(
----            'TRUNCATE %I.%I CASCADE',
----            v_schema, v_table
----            );
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
        IF v_constraintToCreateDefs IS NOT NULL AND v_estimatedNbRows > v_rowsThreshold THEN
            FOR v_i IN 1 .. array_length(v_constraintToCreateDefs, 1)
            LOOP
                EXECUTE format(
                    'ALTER TABLE %I.%I ADD CONSTRAINT %I %s',
                    v_schema, v_table, v_constrainttoDropNames[v_i], v_constraintToCreateDefs[v_i]
                );
            END LOOP;
        END IF;
-- Recreate the non pkey or clustered index indexes that have been previously dropped.
        IF v_indexToCreateDefs IS NOT NULL AND v_estimatedNbRows > v_rowsThreshold THEN
            FOREACH v_indexDef IN ARRAY v_indexToCreateDefs
            LOOP
                EXECUTE v_indexDef;
            END LOOP;
        END IF;
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
--
    RETURN;
END;
$copy_table$;

-- The copy_sequence() function is a generic sequence adjustment function that is used to process individual sequences.
-- Input parameters: batch and step names.
-- It returns a step report.
CREATE FUNCTION copy_sequence(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$copy_sequence$
DECLARE
    v_schema                   TEXT;
    v_sequence                 TEXT;
    v_foreignSchema            TEXT;
    v_sourceSchema             TEXT;
    v_sourceDbms               TEXT;
    v_lastValue                BIGINT;
    v_isCalled                 TEXT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the sequence.
    SELECT stp_schema, stp_object, 'srcdb_' || stp_schema, seq_source_schema, mgr_source_dbms
      INTO v_schema, v_sequence, v_foreignSchema, v_sourceSchema, v_sourceDbms
        FROM @extschema@.step
             JOIN @extschema@.sequence_to_process ON (seq_schema = stp_schema AND seq_name = stp_object)
             JOIN @extschema@.migration ON (seq_migration = mgr_name)
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'copy_sequence: Step % not found in the step table.', p_step;
    END IF;
-- Depending on the source DBMS, get the sequence's characteristics.
    SELECT p_lastValue, p_isCalled INTO v_lastValue, v_isCalled
       FROM @extschema@.get_source_sequence(v_sourceDbms, v_sourceSchema, v_foreignSchema, v_sequence);
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
$copy_sequence$;

-- The get_source_sequence() function get the properties of a sequence on the source database, depending on the RDBMS.
-- It is called by functions processing sequences copy or comparison.
-- Input parameters: source DBSM and the sequence id.
-- The output parameters: last value and is_called properties.
CREATE FUNCTION get_source_sequence(
    p_sourceDbms               TEXT,
    p_sourceSchema             TEXT,
    p_foreignSchema            TEXT,
    p_sequence                 TEXT,
    OUT p_lastValue            BIGINT,
    OUT p_isCalled             TEXT
    )
    RETURNS RECORD LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$get_source_sequence$
BEGIN
-- Depending on the source DBMS, get the sequence's characteristics.
    IF p_sourceDbms = 'Oracle' THEN
        SELECT last_number, 'TRUE' INTO p_lastValue, p_isCalled
           FROM @extschema@.ora_sequences
           WHERE sequence_owner = p_sourceSchema
             AND sequence_name = upper(p_sequence);
    ELSIF p_sourceDbms = 'PostgreSQL' THEN
        EXECUTE format(
            'SELECT last_value, CASE WHEN is_called THEN ''true'' ELSE ''false'' END FROM %I.%I',
            p_foreignSchema, p_sequence
            ) INTO p_lastValue, p_isCalled;
    ELSE
        RAISE EXCEPTION 'get_source_sequence: The DBMS % is not yet implemented (internal error).', p_sourceDbms;
    END IF;
--
    RETURN;
END;
$get_source_sequence$;

-- The compare_table() function is a generic compare function that is used to processes tables.
-- Input parameters: batch and step names.
-- It returns a step report including the number of discrepancies found.
CREATE FUNCTION compare_table(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$compare_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_sourceExprList           TEXT;
    v_destColList              TEXT;
    v_pkJsonBuild              TEXT;
    v_otherJsonBuild           TEXT;
    v_compareSortOrder         TEXT;
    v_partCondition            TEXT;
    v_maxDiff                  TEXT;
    v_stmt                     TEXT;
    v_nbDiff                   BIGINT;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the table.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM @extschema@.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'compare_table: Step % not found in the step table.', p_step;
    END IF;
-- Read the table_to_process table to get additional details.
    SELECT tbl_foreign_schema, tbl_foreign_name,
           array_to_string(tbl_compare_source_exprs, ','), array_to_string(tbl_compare_dest_cols, ','),
           array_to_string(tbl_compare_pk_cols_jb, ','), array_to_string(tbl_compare_other_cols_jb, ','),
           tbl_compare_sort_order
        INTO v_foreignSchema, v_foreignTable,
             v_sourceExprList, v_destColList,
             v_pkJsonBuild, v_otherJsonBuild,
             v_compareSortOrder
        FROM @extschema@.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- If the step concerns a table part, get additional details about the part.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition
            INTO v_partCondition
            FROM @extschema@.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
        IF v_partCondition IS NULL THEN
            RAISE WARNING 'compare_table: A table part cannot be compared without a condition. This step % should not have been assigned to this batch.', p_step;
        END IF;
    END IF;
-- Analyze the step options.
    v_maxDiff = p_stepOptions->>'COMPARE_MAX_DIFF';
--
-- Compare processing.
--
-- The compare processing is not performed for a 'TABLE_PART' step without condition.
    IF v_partNum IS NULL OR v_partCondition IS NOT NULL THEN
-- Compare the foreign table and the destination table.
        v_stmt = format(
            'WITH ft (%s) AS (
                     SELECT %s FROM %I.%I %s),
                  source_diff AS (
                     SELECT %s FROM ft
                         EXCEPT
                     SELECT %s FROM %I.%I %s
                     LIMIT %s),
                  destination_diff AS (
                     SELECT %s FROM %I.%I %s
                         EXCEPT
                     SELECT %s FROM ft
                     LIMIT %s),
                  all_diff AS (
                     SELECT %s, ''S'' AS diff_database, json_build_object(%s) AS diff_pkey_cols, json_build_object(%s) AS diff_other_cols FROM source_diff
                         UNION ALL
                     SELECT %s, ''D'', json_build_object(%s), json_build_object(%s) FROM destination_diff),
                  formatted_diff AS (
                     SELECT %L AS diff_schema, %L AS diff_relation, dense_rank() OVER (ORDER BY %s) AS diff_rank, diff_database, diff_pkey_cols, diff_other_cols
                        FROM all_diff ORDER BY %s, diff_database DESC),
                  inserted_diff AS (
                     INSERT INTO @extschema@.content_diff
                         (diff_schema, diff_relation, diff_rank, diff_database, diff_pkey_cols, diff_other_cols)
                         SELECT * FROM formatted_diff %s
                         RETURNING diff_rank)
                  SELECT max(diff_rank) FROM inserted_diff',
            -- ft CTE variables
            v_destColList,
            v_sourceExprList, v_foreignSchema, v_foreignTable, coalesce('WHERE ' || v_partCondition, ''),
            -- source_diff CTE variables
            v_destColList,
            v_destColList, v_schema, v_table, coalesce('WHERE ' || v_partCondition, ''),
            coalesce (v_maxDiff, 'ALL'),
            -- destination_diff CTE variables
            v_destColList, v_schema, v_table, coalesce('WHERE ' || v_partCondition, ''),
            v_destColList,
            coalesce (v_maxDiff, 'ALL'),
            -- all_diff CTE variables
            v_compareSortOrder, v_pkJsonBuild, v_otherJsonBuild,
            v_compareSortOrder, v_pkJsonBuild, v_otherJsonBuild,
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
        r_output.sr_rank = 52;
    END IF;
    r_output.sr_value = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    IF v_partNum IS NULL THEN
        r_output.sr_indicator = 'NON_EQUAL_TABLES';
        r_output.sr_rank = 51;
    ELSE
        r_output.sr_indicator = 'NON_EQUAL_TABLE_PARTS';
        r_output.sr_rank = 53;
    END IF;
    r_output.sr_value = CASE WHEN v_nbDiff IS NULL THEN 0 ELSE 1 END;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    r_output.sr_indicator = 'ROW_DIFFERENCES';
    r_output.sr_value = coalesce(v_nbDiff, 0);
    r_output.sr_rank = 54;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$compare_table$;

-- The compare_sequence() function compares the characteristics of a source and its destination sequence.
-- Input parameters: batch and step names.
-- It returns a step report.
CREATE FUNCTION compare_sequence(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp  AS
$compare_sequence$
DECLARE
    v_schema                   TEXT;
    v_sequence                 TEXT;
    v_foreignSchema            TEXT;
    v_sourceSchema             TEXT;
    v_sourceDbms               TEXT;
    v_srcLastValue             BIGINT;
    v_srcIsCalled              TEXT;
    v_destLastValue            BIGINT;
    v_destIsCalled             TEXT;
    v_areSequencesEqual        BOOLEAN;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the identity of the sequence.
    SELECT stp_schema, stp_object, 'srcdb_' || stp_schema, seq_source_schema, mgr_source_dbms
      INTO v_schema, v_sequence, v_foreignSchema, v_sourceSchema, v_sourceDbms
        FROM @extschema@.step
             JOIN @extschema@.sequence_to_process ON (seq_schema = stp_schema AND seq_name = stp_object)
             JOIN @extschema@.migration ON (seq_migration = mgr_name)
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'compare_sequence: Step % not found in the step table.', p_step;
    END IF;
-- Depending on the source DBMS, get the source sequence's characteristics.
    SELECT p_lastValue, p_isCalled INTO v_srcLastValue, v_srcIsCalled
       FROM @extschema@.get_source_sequence(v_sourceDbms, v_sourceSchema, v_foreignSchema, v_sequence);
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
            (diff_schema, diff_relation, diff_rank, diff_database, diff_pkey_cols, diff_other_cols)
            VALUES
            (v_schema, v_sequence, 1, 'S', NULL, ('{"last_value": ' || v_srcLastValue || ', "is_called": ' || v_srcIsCalled || '}')::JSON),
            (v_schema, v_sequence, 1, 'D', NULL, ('{"last_value": ' || v_destLastValue || ', "is_called": ' || v_destIsCalled || '}')::JSON);
    END IF;
-- Return the step report.
    r_output.sr_indicator = 'COMPARED_SEQUENCES';
    r_output.sr_value = 1;
    r_output.sr_rank = 60;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'NON_EQUAL_SEQUENCES';
    r_output.sr_rank = 61;
    r_output.sr_is_main_indicator = FALSE;
    r_output.sr_value = CASE WHEN v_areSequencesEqual THEN 0 ELSE 1 END;
    RETURN NEXT r_output;
    r_output.sr_indicator = 'SEQUENCE_DIFFERENCES';
    r_output.sr_rank = 62;
    r_output.sr_value = CASE WHEN NOT v_areSequencesEqual THEN 1 ELSE 0 END;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$compare_sequence$;

-- The truncate_all() function is a generic truncate function to clean up all tables of a migration.
-- It returns a step report including the number of truncated tables.
CREATE FUNCTION truncate_all(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$truncate_all$
DECLARE
    v_batchName                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   @extschema@.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name INTO v_batchName
        FROM @extschema@.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'truncate_all: Step % not found in the step table.', p_step;
    END IF;
-- Build the tables list of all tables of the migration.
    SELECT string_agg('ONLY ' || quote_ident(tbl_schema) || '.' || quote_ident(tbl_name), ', ' ORDER BY tbl_schema, tbl_name), count(*)
        INTO v_tablesList, v_nbTables
        FROM @extschema@.table_to_process
             JOIN @extschema@.batch ON (bat_migration = tbl_migration)
        WHERE bat_name = v_batchName;
    EXECUTE format(
          'TRUNCATE %s CASCADE',
          v_tablesList
          );
-- Return the step report.
    r_output.sr_indicator = 'TRUNCATED_TABLES';
    r_output.sr_value = v_nbTables;
    r_output.sr_rank = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$truncate_all$;

-- The truncate_content_diff() function truncates the content diff table that collects detected tables content differences in batches of type COMPARE.
-- The truncation is only performed when the COMPARE_TRUNCATE_DIFF step option is set to true.
-- It returns a step report including the number of truncated tables, i.e. 1 or 0 depending on the step option parameter.
CREATE FUNCTION truncate_content_diff(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql AS
$truncate_content_diff$
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
$truncate_content_diff$;

-- The check_fkey() function supresses and recreates a foreign key to be sure that the constraint is verified.
-- This may not be always the case because tables are populated in replica mode.
-- The function does not perform anything if the COPY_MAX_ROWS step option is set, because the referential integrity cannot be garanteed if only a subset of tables is copied.
-- It returns a step report.
CREATE FUNCTION check_fkey(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF @extschema@.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$check_fkey$
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
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'check_fkey: Step % not found in the step table.', p_step;
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
           RAISE EXCEPTION 'check_fkey: The foreign key % for table %.% has not been found.', v_fkey, v_schema, v_table;
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
$check_fkey$;

-- The get_batch_ids() function is called by the data2pg scheduler to get the list of all configured batches.
CREATE FUNCTION get_batch_ids()
    RETURNS SETOF batch_id_type LANGUAGE sql AS
$get_batch_ids$
SELECT bat_name, bat_type, bat_migration, mgr_config_completed
    FROM @extschema@.batch
         JOIN @extschema@.migration ON (bat_migration = mgr_name);
$get_batch_ids$;

-- The get_working_plan() function is called by the data2pg scheduler to build its working plan.
-- Only the data useful for its purpose are returned, through the working_plan_type structure.
CREATE FUNCTION get_working_plan(
    p_batchName                TEXT
    )
    RETURNS SETOF working_plan_type LANGUAGE plpgsql AS
$get_working_plan$
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
        RAISE EXCEPTION 'get_working_plan: batch "%" not found.', p_batchName;
    END IF;
    IF NOT v_isConfigCompleted THEN
        RAISE EXCEPTION 'get_working_plan: the migration "%" configuration is not marked as completed. Execute the complete_migration_configuration() function.',
                        v_migration;
    END IF;
-- Deliver steps.
    RETURN QUERY
        SELECT stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_parents
            FROM @extschema@.step
            WHERE stp_batch_name = p_batchName;
    RETURN;
END;
$get_working_plan$;

-- The check_step_options() function is called by the data2pg scheduler. It checks the content of the step_options parameter presented in TEXT format.
-- It verifies that the syntax is JSON compatible and that the keywords and values are valid.
-- Input parameter: the step options, in TEXT format.
-- Output parameter: the error message, or an empty string when no problem is detected.
CREATE FUNCTION check_step_options(
    p_stepOptions            TEXT
    )
    RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS
$check_step_options$
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
               IF jsonb_typeof(v_jsonStepOptions->'COPY_MAX_ROWS') <> 'number' THEN
                   RETURN 'The value for the COPY_MAX_ROWS step option must be a number.';
               END IF;
           WHEN 'COPY_SLOW_DOWN' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COPY_SLOW_DOWN') <> 'number' THEN
                   RETURN 'The value for the COPY_SLOW_DOWN step option must be a number.';
               END IF;
           WHEN 'COMPARE_MAX_DIFF' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COMPARE_MAX_DIFF') <> 'number' THEN
                   RETURN 'The value for the COMPARE_MAX_DIFF step option must be a number.';
               END IF;
           WHEN 'COMPARE_TRUNCATE_DIFF' THEN
               IF jsonb_typeof(v_jsonStepOptions->'COMPARE_TRUNCATE_DIFF') <> 'boolean' THEN
                   RETURN 'The value for the COMPARE_TRUNCATE_DIFF step option must be a boolean.';
               END IF;
           ELSE
               RETURN r_key.key || ' is not a known step option.';
        END CASE;
    END LOOP;
--
    RETURN '';
END;
$check_step_options$;

-- The terminate_data2pg_backends() function is called by the data2pg scheduler for its 'abort' actions.
-- It terminates Postgres backends that could be still in execution.
-- Input parameter: an array of the pids to terminate, if they are still in execution.
-- Output parameter: an array of the pids that have been effectively terminated.
CREATE FUNCTION terminate_data2pg_backends(
    p_pids                   INT[]
    )
    RETURNS INT[] LANGUAGE SQL AS
$terminate_data2pg_backends$
    SELECT array_agg(pid) FROM
        (SELECT pid, pg_terminate_backend(pid) AS has_been_terminated
            FROM pg_stat_activity
                WHERE pid = ANY ($1)
                  AND application_name = 'data2pg') AS t
        WHERE has_been_terminated;
$terminate_data2pg_backends$;

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
SELECT pg_catalog.pg_extension_config_dump('table_part', '');
SELECT pg_catalog.pg_extension_config_dump('sequence_to_process', '');
SELECT pg_catalog.pg_extension_config_dump('source_table_stat', '');
SELECT pg_catalog.pg_extension_config_dump('content_diff', '');