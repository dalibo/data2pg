-- data2pg_schema.sql
-- This file belongs to data2pg, the framework that helps migrating data to PostgreSQL databases from various sources.
-- This sql script creates the structure of the data2pg schema installed into each target database.
-- It must be executed by a superuser role.

\set ON_ERROR_STOP

---
--- Preliminary checks on roles.
---
DO LANGUAGE plpgsql
$$
BEGIN
    PERFORM 0 FROM pg_catalog.pg_roles WHERE rolname = current_user AND rolsuper;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'The current user (%) is not a superuser.', current_user;
    END IF;
END
$$;

BEGIN TRANSACTION;

-- If the data2pg schema already exists, drop all migrations if any, before dropping it.
DO LANGUAGE plpgsql
$$
BEGIN
    PERFORM 0 FROM pg_catalog.pg_namespace WHERE nspname = 'data2pg';
    IF FOUND THEN
        BEGIN
            PERFORM data2pg.drop_migration(mgr_name) FROM data2pg.migration;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Trying to drop migrations failed';
        END;
    END IF;
END;
$$;

--
-- Create the data2pg schema.
--
DROP SCHEMA IF EXISTS data2pg CASCADE;

CREATE SCHEMA data2pg AUTHORIZATION data2pg;
SET search_path = data2pg;

--
-- Create specific types.
--
-- The working_plan_type composite type is used as output record for the get_working_plan() function called by the data2pg.pl scheduler.
CREATE TYPE working_plan_type AS (
    wp_name                    TEXT,                    -- The name of the step
    wp_sql_function            TEXT,                    -- The sql function to execute (for common cases)
    wp_shell_script            TEXT,                    -- A shell script to execute (for specific purpose only)
    wp_cost                    BIGINT,                  -- A relative cost indication used to plan the run (a table size for instance)
    wp_parents                 TEXT[]                   -- A set of parent steps that need to be completed to allow the step to start
);

-- The step_report_type is used as output record for the elementary step functions called by the data2pg.pl scheduler.
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
                                                        --   like 'user ''postgres'', password ''pwd'''
    mgr_config_completed       BOOLEAN,                 -- Boolean indicating whether the migration configuration is completed or not
    PRIMARY KEY (mgr_name)
);

-- The batch table contains a row per batch, i.e. a set of tables and sequences processed by a single data2pg run.
CREATE TABLE batch (
    bat_name                   TEXT NOT NULL,           -- The batch name
    bat_migration              TEXT NOT NULL,           -- The migration the batch belongs to
    bat_type                   TEXT NOT NULL            -- The batch type, i.e. the type of action to perform
                               CHECK (bat_type IN ('COPY', 'CHECK', 'COMPARE')),
    bat_is_first_batch         BOOLEAN,                 -- Boolean indicating whether the batch is the first one of the migration
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
    tbl_sort_order             TEXT,                    -- The ORDER BY clause if needed for the INSERT SELECT copy statement (NULL if no sort)
    tbl_constraint_names       TEXT[],                  -- The constraints to drop before copying the table data
    tbl_constraint_definitions TEXT[],                  -- The constraints to recreate after copying the table data
    tbl_index_names            TEXT[],                  -- The indexes to drop before copying the table data
    tbl_index_definitions      TEXT[],                  -- The indexes to recreate after copying the table data
    tbl_copy_source_cols       TEXT[],                  -- For a COPY batch, the columns to select
    tbl_copy_dest_cols         TEXT[],                  -- For a COPY batch, the columns to insert, in the same order as tbl_copy_source_cols
    tbl_compare_source_cols    TEXT[],                  -- For a COMPARE batch, the columns/expressions to select from the foreign table
    tbl_compare_dest_cols      TEXT[],                  -- For a COMPARE batch, the columns/expressions to select from the local postgres table,
                                                        --   in the same order as tbl_compare_source_cols
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

-- The table_content_diff table is populated with the result of elementary compare_table steps.
-- The table is just a report and has no pkey.
CREATE TABLE table_content_diff (
    diff_schema              TEXT NOT NULL,              -- The name of the destination schema
    diff_relation            TEXT NOT NULL,              -- The schema of the destination table used for the comparison
    diff_database            CHAR NOT NULL               -- The database the rows comes from ; either Source or Destination
                             CHECK (diff_database IN ('S', 'D')),
    diff_row                 JSON                        -- The JSON representation of the rows that are different beetween both databases
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
    p_userMappingOptions     TEXT
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
       FROM data2pg.migration
       WHERE mgr_name = p_migration;
    IF FOUND THEN
        RAISE EXCEPTION 'create_migration: The migration "%" already exists. Call the drop_migration() function to drop it, if needed.', p_migration;
    END IF;
-- Record the supplied parameters into the migration table.
    v_serverName = 'data2pg_' || p_migration || '_server';
    INSERT INTO data2pg.migration
        VALUES (p_migration, p_sourceDbms, p_extension, v_serverName, p_serverOptions, p_userMappingOptions);
-- Create the FDW extension.
    EXECUTE format(
        'CREATE EXTENSION IF NOT EXISTS %s',
        p_extension);
-- Create the Server used to reach the source database.
    EXECUTE format(
        'CREATE SERVER IF NOT EXISTS %s'
        '    FOREIGN DATA WRAPPER %s'
        '    OPTIONS (%s)',
        v_serverName, p_extension, p_serverOptions);
    EXECUTE format(
        'GRANT USAGE ON FOREIGN SERVER %s TO data2pg',
        v_serverName);
-- Create the User Mapping to let the data2pg role or the current superuser installing the function log on the source database.
    EXECUTE format(
        'CREATE USER MAPPING IF NOT EXISTS FOR %s'
        '    SERVER %s'
        '    OPTIONS (%s)',
        current_user, v_serverName, p_userMappingOptions);
-- Load additional objects depending on the selected DBMS.
    PERFORM data2pg.load_dbms_specific_objects(p_migration, p_sourceDbms, v_serverName);
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
    p_serverName             TEXT
    )
    RETURNS VOID LANGUAGE plpgsql AS
$load_dbms_specific_objects$
BEGIN
    -- perform DBMS specific tasks.
    IF p_sourceDbms = 'Oracle' THEN
        -- Create an image of the dba_tables, dba_segments and dba_sequences tables.
        EXECUTE format(
            'IMPORT FOREIGN SCHEMA "SYS" LIMIT TO (dba_tables, dba_segments, dba_sequences) FROM SERVER %s INTO data2pg',
            p_serverName);
        -- Populate the source_table_stat table.
        INSERT INTO data2pg.source_table_stat
            SELECT p_migration, dba_tables.owner, table_name, num_rows, sum(bytes) / 1024
                FROM data2pg.dba_tables, data2pg.dba_segments
                WHERE dba_tables.owner = dba_segments.owner AND table_name = segment_name
                GROUP BY 1,2,3,4;
        -- Drop the now useless foreign tables, but keep the dba_sequences that will be used by the copy_sequence() function.
        DROP FOREIGN TABLE data2pg.dba_tables, data2pg.dba_segments;
    ELSIF p_sourceDbms = 'PostgreSQL' THEN
        -- Create an image of the pg_class table.
        EXECUTE format(
            'CREATE FOREIGN TABLE data2pg.pg_foreign_pg_class ('
            '    relname TEXT,'
            '    relnamespace OID,'
            '    relkind TEXT,'
            '    reltuples BIGINT,'
            '    relpages BIGINT'
            ') SERVER %s OPTIONS (schema_name ''pg_catalog'', table_name ''pg_class'')',
            p_serverName);
        -- Create an image of the pg_namespace table.
        EXECUTE format(
            'CREATE FOREIGN TABLE data2pg.pg_foreign_pg_namespace ('
            '    oid OID,'
            '    nspname TEXT'
            ') SERVER %s OPTIONS (schema_name ''pg_catalog'', table_name ''pg_namespace'')',
            p_serverName);
        -- Populate the source_table_stat table.
        INSERT INTO data2pg.source_table_stat
            SELECT p_migration, nspname, relname, reltuples, relpages * 8
                FROM data2pg.pg_foreign_pg_class
                     JOIN data2pg.pg_foreign_pg_namespace ON (relnamespace = pg_foreign_pg_namespace.oid)
                WHERE relkind = 'r'
                  AND nspname NOT IN ('pg_catalog', 'information_schema');
        -- Drop the now useless foreign tables.
        DROP FOREIGN TABLE data2pg.pg_foreign_pg_class, data2pg.pg_foreign_pg_namespace;
    ELSIF p_sourceDbms = 'Sybase_ASA' THEN
        -- Create an image of the systab table.
        EXECUTE format(
            'CREATE FOREIGN TABLE data2pg.asa_foreign_systab('
            '    user_name TEXT,'
            '    table_name TEXT,'
            '    row_count BIGINT,'
            '    size_kb FLOAT'
            ') SERVER %s OPTIONS (query '
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
        INSERT INTO data2pg.source_table_stat
            SELECT p_migration, user_name, table_name, row_count, size_kb
                FROM data2pg.asa_foreign_systab;
        -- Drop the now useless foreign tables.
        DROP FOREIGN TABLE data2pg.asa_foreign_systab;
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
        FROM data2pg.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RETURN 0;
    END IF;
-- Drop the batches linked to the migration.
    PERFORM data2pg.drop_batch(bat_name)
        FROM data2pg.batch
        WHERE bat_migration = p_migration;
-- Drop foreign tables linked to this migration.
    v_nbForeignTables = 0;
    FOR r_tbl IN
        SELECT tbl_foreign_schema AS foreign_schema, tbl_foreign_name AS foreign_table
            FROM data2pg.table_to_process
            WHERE tbl_migration = p_migration
        UNION
        SELECT seq_foreign_schema, seq_foreign_name
            FROM data2pg.sequence_to_process
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
                 FROM data2pg.table_to_process
                 WHERE tbl_migration = p_migration
           UNION
             SELECT DISTINCT seq_foreign_schema
                 FROM data2pg.sequence_to_process
                 WHERE seq_migration = p_migration
          ) EXCEPT (
             SELECT DISTINCT tbl_foreign_schema AS foreign_schema
                 FROM data2pg.table_to_process
                 WHERE tbl_migration <> p_migration
          ) EXCEPT (
             SELECT DISTINCT seq_foreign_schema
                 FROM data2pg.sequence_to_process
                 WHERE seq_migration <> p_migration
          )
            ) AS t;
    IF v_schemaList IS NOT NULL THEN
        EXECUTE 'DROP SCHEMA IF EXISTS ' || v_schemaList;
    END IF;
-- Remove table parts associated to tables belonging to the migration.
    DELETE FROM data2pg.table_part
        USING data2pg.table_to_process
        WHERE prt_schema = tbl_schema AND prt_table = tbl_name
          AND tbl_migration = p_migration;
-- Remove tables from the table_to_process table.
    DELETE FROM data2pg.table_to_process
        WHERE tbl_migration = p_migration;
-- Remove sequences from the sequence_to_process table.
    DELETE FROM data2pg.sequence_to_process
        WHERE seq_migration = p_migration;
-- Remove the source table statistics from the source_table_stat table.
    DELETE FROM data2pg.source_table_stat
        WHERE stat_migration = p_migration;
-- The FDW is left because it can be used for other purposes.
-- Drop the server, if it exists.
    EXECUTE format(
        'DROP SERVER IF EXISTS %s CASCADE',
        v_serverName);
-- Delete the row from the migration table.
    DELETE FROM data2pg.migration where mgr_name = p_migration;
--
    RETURN v_nbForeignTables;
END;
$drop_migration$;

-- The create_batch() function registers a new batch for an existing migration.
-- It returns the number of created batch, i.e. 1.
CREATE FUNCTION create_batch(
    p_batchName              TEXT,                    -- Batch name
    p_migration              VARCHAR(16),             -- Migration name
    p_batchType              TEXT DEFAULT 'COPY',     -- Batch type (either 'COPY', 'CHECK' or 'COMPARE')
    p_isFirstBatch           BOOLEAN DEFAULT TRUE     -- Boolean indicating whether the batch is the first one of the migration
    )
    RETURNS INTEGER LANGUAGE plpgsql AS
$create_batch$
DECLARE
    v_firstBatch             TEXT;
BEGIN
-- Check that no input parameter is NULL.
    IF p_batchName IS NULL OR p_migration IS NULL OR p_batchType IS NULL OR p_isFirstBatch IS NULL THEN
        RAISE EXCEPTION 'create_batch: No imput parameters can be NULL.';
    END IF;
-- Check that the batch does not exist yet.
    PERFORM 0
       FROM data2pg.batch
       WHERE bat_name = p_batchName;
    IF FOUND THEN
        RAISE EXCEPTION 'create_batch: The batch "%" already exists.', p_batchName;
    END IF;
-- Check that the migration exists and set it as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'create_batch: The migration "%" does not exist.', p_migration;
    END IF;
-- Check the batch type.
    IF p_batchType <> 'COPY' AND p_batchType <> 'CHECK' AND p_batchType <> 'COMPARE' THEN
        RAISE EXCEPTION 'create_batch: Illegal batch type (%). It must be eiter COPY or CHECK or COMPARE.', p_batchType;
    END IF;
-- If the p_isFirstBatch is TRUE, verify that there is no other batch set as the first one of the migration.
    IF p_isFirstBatch THEN
        SELECT bat_name INTO v_firstBatch
            FROM data2pg.batch
            WHERE bat_migration = p_migration AND bat_type = p_batchType AND bat_is_first_batch
            LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'create_batch: The batch "%" is already defined as the first batch of type % for the migration "%".', v_firstBatch, p_batchType, p_migration;
        END IF;
    END IF;
-- Checks are OK.
-- Record the batch into the batch table.
    INSERT INTO data2pg.batch
        VALUES (p_batchName, p_migration, p_batchType, p_isFirstBatch);
-- If the batch is the first one of the migration, add a row into the step table.
    IF p_batchType = 'COPY' AND p_isFirstBatch THEN
        INSERT INTO data2pg.step (stp_name, stp_batch_name, stp_type, stp_sql_function, stp_cost)
            VALUES ('TRUNCATE_' || p_migration, p_batchName, 'TRUNCATE', 'data2pg.truncate_all', 1);
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
    DELETE FROM data2pg.step
        WHERE stp_batch_name = p_batchName;
    GET DIAGNOSTICS v_nbStep = ROW_COUNT;
-- Delete the row from the batch table.
    DELETE FROM data2pg.batch
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
    v_sortOrder              TEXT;
    v_indexToDropNames       TEXT[];
    v_indexToDropDefs        TEXT[];
    v_constraintToDropNames  TEXT[];
    v_constraintToDropDefs   TEXT[];
    v_stmt                   TEXT;
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
-- Check that the first 3 parameter are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_tables: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check that the migration exists and get its server name and source DBMS.
    SELECT mgr_server_name, mgr_source_dbms INTO v_serverName, v_sourceDbms
        FROM data2pg.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_tables: Migration "%" not found.', p_migration;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
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
            FROM data2pg.table_to_process
            WHERE tbl_schema = p_schema
              AND tbl_name = r_tbl.relname;
        IF FOUND THEN
            RAISE EXCEPTION 'register_tables: The table %.% is already assigned to the migration %.',
                            p_schema, r_tbl.relname, v_prevMigration;
        END IF;
-- Look at the indexes associated to the table.
        v_sortOrder = NULL;
-- Get the clustered index columns list, if it exists.
-- This list will be used in an ORDER BY clause in the table copy function.
        SELECT substring(pg_get_indexdef(pg_class.oid) FROM ' USING .*\((.+)\)') INTO v_sortOrder
            FROM pg_index
                 JOIN pg_class ON (pg_class.oid = indexrelid)
            WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
              AND indisclustered;
-- If no clustered index exists and the sort is allowed on PKey, get the primary key index columns list, if it exists.
        IF v_sortOrder IS NULL AND p_sortByPKey THEN
            SELECT substring(pg_get_indexdef(pg_class.oid) FROM ' USING .*\((.+)\)') INTO v_sortOrder
                FROM pg_index
                     JOIN pg_class ON (pg_class.oid = indexrelid)
                WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
                  AND indisprimary;
        END IF;
-- Build the array of constraints of type UNIQUE or EXCLUDE and their definition.
        SELECT array_agg(conname), array_agg(constraint_def) INTO v_constraintToDropNames, v_constraintToDropDefs
            FROM (
                SELECT c1.conname, pg_get_constraintdef(c1.oid) AS constraint_def
                    FROM pg_catalog.pg_constraint c1
                    WHERE c1.conrelid = r_tbl.table_oid
                      AND c1.contype IN ('u', 'x', 'p')
----                      AND c1.contype IN ('u', 'x')
                      AND NOT EXISTS(                             -- the index linked to this constraint must not be linked to other constraints
                          SELECT 0 FROM pg_catalog.pg_constraint c2
                          WHERE c2.conindid = c1.conindid AND c2.oid <> c1.oid
                          )
                    ORDER BY conname
                 ) AS t;
-- Build the array of non pkey and clustered indexes and their definition.
        SELECT array_agg(index_name), array_agg(index_def) INTO v_indexToDropNames, v_indexToDropDefs
            FROM (
                SELECT relname AS index_name, pg_get_indexdef(pg_class.oid) AS index_def
                    FROM pg_index
                         JOIN pg_class ON (pg_class.oid = indexrelid)
                    WHERE relnamespace = r_tbl.schema_oid AND indrelid = r_tbl.table_oid
                      AND NOT indisclustered
                      AND NOT EXISTS(                             -- the index must not be linked to any constraint
                          SELECT 0 FROM pg_catalog.pg_constraint
                          WHERE pg_constraint.conindid = pg_class.oid
                          )
                    ORDER BY relname
                 ) AS t;
-- Build the list of columns to copy and columns to compare and some indicators to store into the step table.
        v_stmt = 'SELECT array_agg(quote_ident(attname)),'
--                           the columns list for the table comparison step, excluding the GENERATED ALWAYS AS (expression) columns
                 '       array_agg(quote_ident(attname)) FILTER (WHERE attgenerated = ''''),'
--                           the columns list for the table copy step, excluding the GENERATED ALWAYS AS (expression) columns
                 '       count(*) FILTER (WHERE attidentity = ''a''),'
--                           the number of GENERATED ALWAYS AS IDENTITY columns
                 '       count(*) FILTER (WHERE attgenerated <> '''')'
--                           the number of GENERATED ALWAYS AS (expression) columns
                 '    FROM ('
                 '        SELECT attname, %s AS attidentity, %s AS attgenerated'
                 '            FROM pg_catalog.pg_attribute'
                 '            WHERE attrelid = %s::regclass'
                 '              AND attnum > 0 AND NOT attisdropped'
                 '            ORDER BY attnum) AS t';
        EXECUTE format(v_stmt,
                       CASE WHEN v_pgVersion >= 100000 THEN 'attidentity' ELSE '''''::TEXT' END,
                       CASE WHEN v_pgVersion >= 120000 THEN 'attgenerated' ELSE '''''::TEXT' END,
                       quote_literal(quote_ident(p_schema) || '.' || quote_ident(r_tbl.relname)))
            INTO v_columnsToCompare, v_columnsToCopy, v_nbGenAlwaysIdentCol, v_nbGenAlwaysExprCol;
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
                FROM data2pg.%I
                WHERE stat_schema = %L
                  AND stat_table = %L',
            p_sourceTableStatLoc, v_sourceSchema,
            CASE WHEN v_sourceDbms = 'Oracle' THEN UPPER(r_tbl.relname) ELSE r_tbl.relname END)
            INTO v_sourceRows, v_sourceKBytes;
-- Register the table into the table_to_process table.
        INSERT INTO data2pg.table_to_process (
                tbl_schema, tbl_name, tbl_migration, tbl_foreign_schema, tbl_foreign_name,
                tbl_rows, tbl_kbytes, tbl_sort_order,
                tbl_constraint_names, tbl_constraint_definitions, tbl_index_names, tbl_index_definitions,
                tbl_copy_dest_cols,
                tbl_copy_source_cols, tbl_compare_source_cols, tbl_compare_dest_cols, tbl_some_gen_alw_id_col,
                tbl_referencing_tables, tbl_referenced_tables
            ) VALUES (
                p_schema, r_tbl.relname, p_migration, v_foreignSchema, r_tbl.relname,
                coalesce(v_sourceRows, 0), coalesce(v_sourceKBytes, 0), v_sortOrder,
                v_constraintToDropNames, v_constraintToDropDefs, v_indexToDropNames, v_indexToDropDefs,
----                CASE WHEN v_nbGenAlwaysExprCol > 0 THEN v_columnsToCopy ELSE NULL END,
                v_columnsToCopy, v_columnsToCopy, v_columnsToCompare, v_columnsToCompare, (v_nbGenAlwaysIdentCol > 0),
                v_referencingTables, v_referencedTables
            );
-- Create the foreign table mapped on the table in the source database.
--    For Oracle, the source schema name is forced in upper case.
        IF p_createForeignTable THEN
            EXECUTE format(
                'IMPORT FOREIGN SCHEMA %s LIMIT TO (%I) FROM SERVER %s INTO %I',
                CASE WHEN v_sourceDbms = 'Oracle' THEN '"' || v_sourceSchema || '"' ELSE quote_ident(v_sourceSchema) END,
                r_tbl.relname, v_serverName, v_foreignSchema);
        END IF;
    END LOOP;
--
    RETURN v_nbTables;
END;
$register_tables$;

-- The register_column_transform_rule() functions defines a column change from the source table to the destination table.
-- It allows to manage columns with different names, with different types and or with specific computation rule.
-- The target column is defined with the schema, table and column name.
-- Several transformation rule may be applied for the same column. In this case, the p_column parameter of the second rule must be the p_value of the first one.
CREATE FUNCTION register_column_transform_rule(
    p_schema                 TEXT,               -- The schema name of the related table
    p_table                  TEXT,               -- The table name
    p_column                 TEXT,               -- The column name as it would appear in the INSERT SELECT statement of the copy processing
    p_value                  TEXT                -- The column name as it will appear in the INSERT SELECT statement of the copy processing. It may be
                                                 --   another column name if the column is renamed or an expression if the column content requires a
                                                 --   transformation rule. The column name may need to be double-quoted.
    )
    RETURNS VOID LANGUAGE plpgsql AS
$register_column_transform_rule$
DECLARE
    v_migrationName          TEXT;
    v_copyColumns            TEXT[];
    v_compareColumns         TEXT[];
    v_colPosition            INTEGER;
BEGIN
-- Check that no parameter is not NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_column IS NULL OR p_value IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: None of the input parameters can be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name and select columns array for both COPY and COMPARE steps.
    SELECT tbl_migration, tbl_copy_source_cols, tbl_compare_source_cols INTO v_migrationName, v_copyColumns, v_compareColumns
        FROM data2pg.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_column_transform_rule: Table %.% not found.', p_schema, p_table;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Look in the tables copy column array if the source column exists, and apply the change.
    v_colPosition = array_position(v_copyColumns, p_column);
    IF v_colPosition IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: The column % is not found in the list of columns to copy %.', p_column, v_copyColumns;
    END IF;
    v_copyColumns[v_colPosition] = p_value;
-- Look in the tables compare column array if the source column exists, and apply the change.
    v_colPosition = array_position(v_compareColumns, p_column);
    IF v_colPosition IS NULL THEN
        RAISE EXCEPTION 'register_column_transform_rule: The column % is not found in the list of columns to compare %.', p_column, v_compareColumns;
    END IF;
    v_compareColumns[v_colPosition] = p_value;
-- Record the changes.
    UPDATE data2pg.table_to_process
        SET tbl_copy_source_cols = v_copyColumns, tbl_compare_source_cols = v_compareColumns
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
-- Check that the first 3 parameter are not NULL.
    IF p_schema IS NULL OR p_table IS NULL OR p_partNum IS NULL THEN
        RAISE EXCEPTION 'register_table_part: None of the first 3 input parameters can be NULL.';
    END IF;
-- Check that the table is already registered and get its migration name.
    SELECT tbl_migration INTO v_migrationName
        FROM data2pg.table_to_process
        WHERE tbl_schema = p_schema AND tbl_name = p_table;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_table_part: Table %.% not found.', p_schema, p_table;
    END IF;
-- Set the related migration as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Check that the table part doesn't exist yet.
    PERFORM 0
        FROM data2pg.table_part
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
    INSERT INTO data2pg.table_part (
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
-- Check that the first 3 parameter are not NULL.
    IF p_migration IS NULL OR p_schema IS NULL OR p_sequencesToInclude IS NULL THEN
        RAISE EXCEPTION 'register_sequences: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the migration exists and get its server name and source DBMS.
    SELECT mgr_server_name, mgr_source_dbms INTO v_serverName, v_sourceDbms
        FROM data2pg.migration
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'register_sequences: Migration "%" not found.', p_migration;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
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
            FROM data2pg.sequence_to_process
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
                '    SERVER %s OPTIONS (schema_name %L)',
                v_foreignSchema, r_seq.relname, v_serverName, coalesce(p_sourceSchema, p_schema));
            EXECUTE format(
                'ALTER FOREIGN TABLE %I.%I OWNER TO data2pg',
                v_foreignSchema, r_seq.relname);
        END IF;
-- Register the sequence into the sequence_to_process table.
        INSERT INTO data2pg.sequence_to_process (
                seq_schema, seq_name, seq_migration, seq_foreign_schema, seq_foreign_name, seq_source_schema
            ) VALUES (
                p_schema, r_seq.relname, p_migration, v_foreignSchema, r_seq.relname, coalesce(p_sourceSchema, p_schema)
            );
    END LOOP;
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
-- Check that the first 3 parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_tablesToInclude IS NULL THEN
        RAISE EXCEPTION 'assign_tables_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM data2pg.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_tables_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Get the selected tables.
    v_nbTables = 0;
    FOR r_tbl IN
        SELECT tbl_name, tbl_rows, tbl_kbytes, tbl_referencing_tables
            FROM data2pg.table_to_process
            WHERE tbl_migration = v_migrationName
              AND tbl_schema = p_schema
              AND tbl_name ~ p_tablesToInclude
              AND (p_tablesToExclude IS NULL OR tbl_name !~ p_tablesToExclude)
    LOOP
        v_nbTables = v_nbTables + 1;
-- Check that the table is not already fully assigned to a batch of the same type.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM data2pg.step
                 JOIN data2pg.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || r_tbl.tbl_name
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_tables_to_batch: The table %.% is already assigned to the batch "%".',
                            p_schema, r_tbl.rtbl_name, v_prevBatchName;
        END IF;
-- Check that the table has no table part already assigned to any batch of the same type.
        PERFORM 0
            FROM data2pg.step
                 JOIN data2pg.batch ON (bat_name = stp_batch_name)
            WHERE stp_name LIKE p_schema || '.' || r_tbl.tbl_name || '.%'
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_tables_to_batch: The table %.% has at least 1 part already assigned to a batch of type %.',
                            p_schema, r_tbl.rtbl_name, v_batchType;
        END IF;
-- Register the table into the step table.
        INSERT INTO data2pg.step (
                stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                stp_sql_function, stp_cost
            ) VALUES (
                p_schema || '.' || r_tbl.tbl_name, p_batchName, 'TABLE', p_schema, r_tbl.tbl_name,
                CASE v_batchType
                    WHEN 'COPY' THEN 'data2pg.copy_table'
                    WHEN 'CHECK' THEN 'data2pg.check_table'
                    WHEN 'COMPARE' THEN 'data2pg.compare_table'
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
        FROM data2pg.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Check that the table part has been registered and get the table statistics.
    SELECT tbl_rows, tbl_kbytes INTO v_rows, v_kbytes
        FROM data2pg.table_part
             JOIN data2pg.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
        WHERE prt_schema = p_schema AND prt_table = p_table AND prt_number = p_partNum;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% has not been registered.',
                        p_partNum, p_schema, p_table;
    END IF;
-- Check that the table part has not been already assigned to a batch of the same type.
    SELECT stp_batch_name INTO v_prevBatchName
        FROM data2pg.step
             JOIN data2pg.batch ON (bat_name = stp_batch_name)
        WHERE stp_name = p_schema || '.' || p_table || '.' || p_partNum
          AND bat_type = v_batchType;
    IF FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The part % of the table %.% is already assigned to the batch %.',
                        p_partNum, p_schema, p_table, v_prevBatchName;
    END IF;
-- Check that the table is not already fully assigned to a batch of the same type.
    SELECT stp_batch_name INTO v_prevBatchName
        FROM data2pg.step
             JOIN data2pg.batch ON (bat_name = stp_batch_name)
        WHERE stp_name = p_schema || '.' || p_table
          AND bat_type = v_batchType;
    IF FOUND THEN
        RAISE EXCEPTION 'assign_table_part_to_batch: The table %.% is already assigned to the batch "%".',
                        p_schema, p_table, v_prevBatchName;
    END IF;
-- Register the table part into the step table.
    INSERT INTO data2pg.step (
            stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_part_num,
            stp_sql_function, stp_cost
        ) VALUES (
            p_schema || '.' || p_table || '.' || p_partNum, p_batchName, 'TABLE_PART', p_schema, p_table, p_partNum,
            'data2pg.copy_table', v_kbytes
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
-- Check that the first 3 parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_sequencesToInclude IS NULL THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists and get its migration name.
    SELECT bat_migration, bat_type INTO v_migrationName, v_batchType
        FROM data2pg.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Check taht the batch is not of type 'CHECK'.
    IF v_batchType = 'CHECK' THEN
        RAISE EXCEPTION 'assign_sequences_to_batch: sequences cannot be assigned to a batch of type %.', p_batchType;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
       SET mgr_config_completed = FALSE
       WHERE mgr_name = v_migrationName;
-- Get the selected sequences.
    v_nbSequences = 0;
    FOR r_seq IN
        SELECT seq_name
            FROM data2pg.sequence_to_process
            WHERE seq_migration = v_migrationName
              AND seq_schema = p_schema
              AND seq_name ~ p_sequencesToInclude
              AND (p_sequencesToExclude IS NULL OR seq_name !~ p_sequencesToExclude)
    LOOP
        v_nbSequences = v_nbSequences + 1;
-- Check that the sequence is not already assigned to a batch of the same type.
        SELECT stp_batch_name INTO v_prevBatchName
            FROM data2pg.step
                 JOIN data2pg.batch ON (bat_name = stp_batch_name)
            WHERE stp_name = p_schema || '.' || r_seq.seq_name
              AND bat_type = v_batchType;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_sequences_to_batch: The sequence %.% is already assigned to the batch %.',
                            p_schema, r_seq.seq_name, v_prevBatchName;
        END IF;
-- Register the sequence into the step table.
        INSERT INTO data2pg.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object, stp_sql_function, stp_cost)
            VALUES (
                p_schema || '.' || r_seq.seq_name, p_batchName, 'SEQUENCE', p_schema, r_seq.seq_name,
                CASE v_batchType
                    WHEN 'COPY' THEN 'data2pg.copy_sequence'
                    WHEN 'COMPARE' THEN 'data2pg.compare_sequence'
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
    v_tableKbytes            FLOAT;
    v_refTableKbytes         FLOAT;
    v_nbFKey                 INT;
    v_prevBatchName          TEXT;
    v_cost                   BIGINT;
    r_fk                     RECORD;
BEGIN
-- Check that the first 3 parameter are not NULL.
    IF p_batchName IS NULL OR p_schema IS NULL OR p_table IS NULL THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: The first 3 input parameters cannot be NULL.';
    END IF;
-- Check that the batch exists and get its migration name.
    SELECT bat_migration INTO v_migrationName
        FROM data2pg.batch
        WHERE bat_name = p_batchName;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'assign_fkey_checks_to_batch: batch "%" not found.', p_batchName;
    END IF;
-- Set the migration as 'configuration in progress'.
    UPDATE data2pg.migration
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
        FROM data2pg.table_to_process
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
            FROM data2pg.step
            WHERE stp_name = p_schema || '.' || p_table || '.' || r_fk.conname;
        IF FOUND THEN
            RAISE EXCEPTION 'assign_fkey_checks_to_batch: The fkey % of table %.% is already assigned to the batch %.',
                            r_fk.conname, p_schema, p_table, v_prevBatchName;
        END IF;
-- Get the table size for the referenced table, if registered.
        v_refTableKbytes = 0;
        SELECT tbl_kbytes INTO v_refTableKbytes
            FROM data2pg.table_to_process
            WHERE tbl_schema = r_fk.nspname
              AND tbl_name = r_fk.relname;
-- Compute the global cost and check that at least one of both tables are registered.
        v_cost = (v_tableKbytes + v_refTableKbytes);
        IF v_cost = 0 THEN
            RAISE EXCEPTION 'assign_fkey_checks_to_batch: none of both tables linked by the fkey %.%.% are registered.',
                            p_schema, p_table, r_fk.conname;
        END IF;
-- Register the sequence into the step table.
        INSERT INTO data2pg.step (stp_name, stp_batch_name, stp_type, stp_schema, stp_object,
                                  stp_sub_object, stp_sql_function, stp_cost)
            VALUES (p_schema || '.' || p_table || '.' || r_fk.conname, p_batchName, 'FOREIGN_KEY', p_schema, p_table,
                    r_fk.conname, 'data2pg.check_fkey', v_cost);
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
    v_firstBatchArray        TEXT[];
    v_parents                TEXT[];
    v_refSchema              TEXT;
    v_refTable               TEXT;
    v_parentsToAdd           TEXT[];
    r_tbl                    RECORD;
    r_step                   RECORD;
BEGIN
-- Check that the migration exist and set it config_completed flag as true.
    UPDATE data2pg.migration
        SET mgr_config_completed = TRUE
        WHERE mgr_name = p_migration;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'complete_migration_configuration: Migration "%" not found.', p_migration;
    END IF;
-- Get the list of related batches and check that the number of batches marked as the first one is exactly 1.
    SELECT array_agg(bat_name), array_agg(bat_name) FILTER (WHERE bat_is_first_batch)
        INTO v_batchArray, v_firstBatchArray
        FROM data2pg.batch
        WHERE bat_migration = p_migration;
    IF array_ndims(v_firstBatchArray) <> 1 THEN
        RAISE EXCEPTION 'complete_migration_configuration: There must be 1 and only 1 batch in the migration set as the first one to be executed.';
    END IF;
-- Check that all tables registered into the migration must have a unique part set as the first one and a unique part as the last one.
    FOR r_tbl IN
        SELECT prt_schema, prt_table, nb_first, nb_last
            FROM (
                SELECT prt_schema, prt_table,
                       count(*) FILTER (WHERE prt_is_first_step) AS nb_first,
                       count(*) FILTER (WHERE prt_is_last_step) AS nb_last
                    FROM data2pg.table_part
                         JOIN data2pg.table_to_process ON (prt_schema = tbl_schema AND prt_table = tbl_name)
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
--
-- Build the chaining constraints between steps.
--
-- Reset the stp_parents column for the all steps of the migration.
    UPDATE data2pg.step
       SET stp_parents = NULL
       WHERE stp_batch_name = ANY (v_batchArray)
         AND stp_parents IS NOT NULL;
-- Create the links between table parts.
-- The table parts set as the first step for their table must be the parents of all the others parts of the same batch.
    FOR r_step IN
        SELECT stp_batch_name, stp_name, stp_schema, stp_object, stp_part_num
            FROM data2pg.step
                 JOIN data2pg.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
                 JOIN data2pg.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
            WHERE stp_type = 'TABLE_PART'
              AND tbl_migration = p_migration
              AND prt_is_first_step
    LOOP
        UPDATE data2pg.step
            SET stp_parents = array_append(stp_parents, r_step.stp_name)
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_part_num <> r_step.stp_part_num;
    END LOOP;
-- The table parts set as the last step for their table must have all the other parts of the same batch as parents.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_part_num
            FROM data2pg.step
                 JOIN data2pg.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
            WHERE stp_batch_name = ANY (v_batchArray)
              AND stp_type = 'TABLE_PART'
              AND prt_is_last_step
    LOOP
        SELECT array_agg(stp_name) INTO v_parents
            FROM data2pg.step
                 JOIN data2pg.table_part ON (prt_schema = stp_schema AND prt_table = stp_object AND prt_number = stp_part_num)
                 JOIN data2pg.table_to_process ON (tbl_schema = prt_schema AND tbl_name = prt_table)
            WHERE stp_type = 'TABLE_PART'
              AND stp_schema = r_step.stp_schema
              AND stp_object = r_step.stp_object
              AND stp_part_num <> r_step.stp_part_num
              AND stp_batch_name = r_step.stp_batch_name;
        UPDATE data2pg.step
            SET stp_parents = array_cat(stp_parents, v_parents)
            WHERE stp_name = r_step.stp_name;
    END LOOP;
-- Add chaining constraints for foreign keys checks.
    FOR r_step IN
        SELECT stp_name, stp_batch_name, stp_schema, stp_object, stp_sub_object
            FROM data2pg.step
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
            FROM data2pg.step
            WHERE stp_batch_name = r_step.stp_batch_name
              AND stp_type IN ('TABLE', 'TABLE_PART')
              AND ((    stp_schema = r_step.stp_schema
                    AND stp_object = r_step.stp_object
                   ) OR (
                        stp_schema = v_refSchema
                    AND stp_object = v_refTable
                   ));
        UPDATE data2pg.step
            SET stp_parents = array_cat(stp_parents, v_parentsToAdd)
            WHERE stp_name = r_step.stp_name;
    END LOOP;
-- Process the chaining constraints related to the TRUNCATE step in the first batch.
    UPDATE data2pg.step
        SET stp_parents = ARRAY['TRUNCATE_' || p_migration]
        WHERE stp_batch_name = v_firstBatchArray[1]
          AND stp_type IN ('TABLE', 'TABLE_PART')
          AND stp_parents IS NULL;
-- Compute the cost of the Truncate step, now that the number of tables to truncate is known.
    UPDATE data2pg.step
        SET stp_cost = (
             SELECT 10 * count(*)
                 FROM data2pg.table_to_process
                 WHERE tbl_migration = p_migration
                       )
        WHERE stp_name = 'TRUNCATE_' || p_migration;
-- Remove duplicate steps in the all parents array of the migration.
    WITH parent_rebuild AS (
        SELECT step_name, array_agg(step_parent ORDER BY step_parent) AS unique_parents
            FROM (
               SELECT DISTINCT stp_name AS step_name, unnest(stp_parents) AS step_parent
                   FROM data2pg.step
                   WHERE stp_batch_name = ANY (v_batchArray)
                 ) AS t
            GROUP BY step_name)
    UPDATE data2pg.step
        SET stp_parents = parent_rebuild.unique_parents
        FROM parent_rebuild
        WHERE stp_batch_name = ANY (v_batchArray)
          AND step.stp_name = parent_rebuild.step_name
          AND step.stp_parents <> parent_rebuild.unique_parents;
--
    RETURN;
END;
$complete_migration_configuration$;

--
-- Functions called by the data2pg scheduler.
--

-- The copy_table() function is the generic copy function that is used to processes tables.
-- Input parameters: batch and step names.
-- The output parameter is the number of copied rows.
-- It is set as session_replication_role = 'replica', so that no check are performed on foreign keys and no regular trigger are executed.
CREATE FUNCTION copy_table(
    p_batchName                TEXT,
    p_step                     TEXT
    )
    RETURNS SETOF data2pg.step_report_type LANGUAGE plpgsql
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
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_estimatedNbRows          BIGINT;
    v_sortOrder                TEXT;
    v_constraintToDropNames    TEXT[];
    v_constraintToCreateDefs   TEXT[];
    v_indexToDropNames         TEXT[];
    v_indexToCreateDefs        TEXT[];
    v_insertColList            TEXT;
    v_selectColList            TEXT;
    v_someGenAlwaysIdentCol    BOOLEAN;
    v_constraint               TEXT;
    v_i                        INT;
    v_index                    TEXT;
    v_indexDef                 TEXT;
    v_stmt                     TEXT;
    v_nbRows                   BIGINT = 0;
    r_output                   data2pg.step_report_type;
BEGIN
-- Get the identity of the table.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM data2pg.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'copy_table: Step % not found in the step table.', p_step;
    END IF;
-- Read the table_to_process table to get additional details.
    SELECT tbl_foreign_schema, tbl_foreign_name, tbl_rows, tbl_sort_order,
           tbl_constraint_names, tbl_constraint_definitions, tbl_index_names, tbl_index_definitions,
           array_to_string(tbl_copy_dest_cols, ','), array_to_string(tbl_copy_source_cols, ','), tbl_some_gen_alw_id_col
        INTO v_foreignSchema, v_foreignTable, v_estimatedNbRows, v_sortOrder,
             v_constraintToDropNames, v_constraintToCreateDefs, v_indexToDropNames, v_indexToCreateDefs,
             v_insertColList, v_selectColList, v_someGenAlwaysIdentCol
        FROM data2pg.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- If the step concerns a table part, get additional details about the part.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition, prt_is_first_step, prt_is_last_step
            INTO v_partCondition, v_isFirstStep, v_isLastStep
            FROM data2pg.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
    END IF;
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
-- Do not sort the source data when the table is processed with a where clause.
        IF v_partCondition IS NOT NULL THEN
            v_sortOrder = NULL;
        END IF;
-- Copy the foreign table to the destination table.
        v_stmt = format(
            'INSERT INTO %I.%I (%s) %s
               SELECT %s
               FROM ONLY %I.%I
               %s
               %s',
            v_schema, v_table, v_InsertColList,
            CASE WHEN v_someGenAlwaysIdentCol THEN ' OVERRIDING SYSTEM VALUE' ELSE '' END,
            v_selectColList, v_foreignSchema, v_foreignTable,
            coalesce('WHERE ' || v_partCondition, ''),
            coalesce('ORDER BY ' || v_sortOrder, '')
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
---- temporary slowdown (for testing purpose)
----      PERFORM pg_sleep(v_nbRows/1000);
    END IF;
-- Return the result records
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

-- The copy_sequence() function is a generic sequence adjustment function that isb used to processes individual sequences.
-- Input parameters: batch and step names.
-- The output parameter is 0.
CREATE FUNCTION copy_sequence(
    p_batchName                TEXT,
    p_step                     TEXT
    )
    RETURNS SETOF data2pg.step_report_type LANGUAGE plpgsql
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
    r_output                   data2pg.step_report_type;
BEGIN
-- Get the identity of the sequence.
    SELECT stp_schema, stp_object, 'srcdb_' || stp_schema, seq_source_schema, mgr_source_dbms
      INTO v_schema, v_sequence, v_foreignSchema, v_sourceSchema, v_sourceDbms
        FROM data2pg.step
             JOIN data2pg.sequence_to_process ON (seq_schema = stp_schema AND seq_name = stp_object)
             JOIN data2pg.migration ON (seq_migration = mgr_name)
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'copy_sequence: Step % not found in the step table.', p_step;
    END IF;
-- Depending on the source DBMS, set the sequence's characteristics.
    IF v_sourceDbms = 'Oracle' THEN
        SELECT last_number, 'TRUE' INTO v_lastValue, v_isCalled
           FROM data2pg.dba_sequences
           WHERE sequence_owner = v_sourceSchema
             AND sequence_name = upper(v_sequence);
    ELSIF v_sourceDbms = 'PostgreSQL' THEN
-- Copy the foreign table to the destination table.
        EXECUTE format(
            'SELECT last_value, CASE WHEN is_called THEN ''TRUE'' ELSE ''FALSE'' END FROM %I.%I',
            v_foreignSchema, v_sequence
            ) INTO v_lastValue, v_isCalled;
    ELSE
        RAISE EXCEPTION 'copy_sequence: The DBMS % is not yet implemented (internal error).', p_sourceDbms;
    END IF;
-- Set the sequence's characteristics.
    EXECUTE format(
        'SELECT setval(%L, %s, %s)',
        quote_ident(v_schema) || '.' || quote_ident(v_sequence), v_lastValue, v_isCalled
        );
-- Return the result record
    r_output.sr_indicator = 'COPIED_SEQUENCES';
    r_output.sr_value = 1;
    r_output.sr_rank = 20;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$copy_sequence$;

-- The compare_table() function is a generic compare function that is used to processes tables.
-- Input parameters: batch and step names.
-- The output parameter is the number of discrepancies found.
CREATE FUNCTION compare_table(
    p_batchName                TEXT,
    p_step                     TEXT
    )
    RETURNS SETOF data2pg.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$compare_table$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_partNum                  INTEGER;
    v_partCondition            TEXT;
    v_foreignSchema            TEXT;
    v_foreignTable             TEXT;
    v_estimatedNbRows          BIGINT;
    v_sourceColList            TEXT;
    v_destColList              TEXT;
    v_stmt                     TEXT;
    v_nbRows                   BIGINT = 0;
    r_output                   data2pg.step_report_type;
BEGIN
-- Get the identity of the table.
    SELECT stp_schema, stp_object, stp_part_num INTO v_schema, v_table, v_partNum
        FROM data2pg.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'compare_table: Step % not found in the step table.', p_step;
    END IF;
-- Read the table_to_process table to get additional details.
    SELECT tbl_foreign_schema, tbl_foreign_name,
           array_to_string(tbl_compare_source_cols, ','), array_to_string(tbl_compare_dest_cols, ',')
        INTO v_foreignSchema, v_foreignTable,
             v_sourceColList, v_destColList
        FROM data2pg.table_to_process
        WHERE tbl_schema = v_schema AND tbl_name = v_table;
-- If the step concerns a table part, get additional details about the part.
    IF v_partNum IS NOT NULL THEN
        SELECT prt_condition
            INTO v_partCondition
            FROM data2pg.table_part
            WHERE prt_schema = v_schema AND prt_table = v_table AND prt_number = v_partNum;
        IF v_partCondition IS NULL THEN
            RAISE EXCEPTION 'compare_table: A table part cannot be compared without a condition. This step % should not have been assigned to this batch.', p_step;
        END IF;
    END IF;
--
-- Compare processing.
--
-- The compare processing is not performed for a 'TABLE_PART' step without condition.
    IF v_partNum IS NULL OR v_partCondition IS NOT NULL THEN
-- Compare the foreign table and the destination table.
        v_stmt = format(
            'WITH ft (%s) AS (
                     SELECT %s FROM %I.%I %s),
                  source_rows AS (
                     SELECT %s FROM ft
                         EXCEPT
                     SELECT %s FROM %I.%I %s),
                  destination_rows AS (
                     SELECT %s FROM %I.%I %s
                         EXCEPT
                     SELECT %s FROM ft)
             INSERT INTO data2pg.table_content_diff
                 SELECT %L, %L, ''S'', to_json(row(source_rows))->''f1'' FROM source_rows
                     UNION ALL
                 SELECT %L, %L, ''D'', to_json(row(destination_rows))->''f1'' FROM destination_rows',
            v_destColList,
            v_sourceColList, v_foreignSchema, v_foreignTable, coalesce('WHERE ' || v_partCondition, ''),
            v_destColList,
            v_destColList, v_schema, v_table, coalesce('WHERE ' || v_partCondition, ''),
            v_destColList, v_schema, v_table, coalesce('WHERE ' || v_partCondition, ''),
            v_destColList,
            v_schema, v_table,
            v_schema, v_table
            );
--raise warning '%',v_stmt;
        EXECUTE v_stmt;
        GET DIAGNOSTICS v_nbRows = ROW_COUNT;
    END IF;
-- Return the result record
    r_output.sr_indicator = 'COMPARED_TABLES';
    r_output.sr_value = 1;
    r_output.sr_rank = 50;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
    IF v_nbRows = 0 THEN
        r_output.sr_indicator = 'EQUAL_TABLES';
        r_output.sr_value = 1;
        r_output.sr_rank = 51;
        r_output.sr_is_main_indicator = FALSE;
        RETURN NEXT r_output;
    END IF;
    r_output.sr_indicator = 'DIFFERENCES';
    r_output.sr_value = v_nbRows;
    r_output.sr_rank = 52;
    r_output.sr_is_main_indicator = TRUE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$compare_table$;

-- The truncate_all() function is a generic truncate function to clean up all tables of a migration.
-- It returns 0.
CREATE FUNCTION truncate_all(
    p_batchName                TEXT,
    p_step                     TEXT
    )
    RETURNS SETOF data2pg.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$truncate_all$
DECLARE
    v_batchName                TEXT;
    v_tablesList               TEXT;
    v_nbTables                 BIGINT;
    r_tbl                      RECORD;
    r_output                   data2pg.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_batch_name INTO v_batchName
        FROM data2pg.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'truncate_all: Step % not found in the step table.', p_step;
    END IF;
-- Build the tables list of all tables of the migration.
    SELECT string_agg('ONLY ' || quote_ident(tbl_schema) || '.' || quote_ident(tbl_name), ', ' ORDER BY tbl_schema, tbl_name), count(*)
        INTO v_tablesList, v_nbTables
        FROM data2pg.table_to_process
             JOIN data2pg.batch ON (bat_migration = tbl_migration)
        WHERE bat_name = v_batchName;
    EXECUTE format(
          'TRUNCATE %s CASCADE',
          v_tablesList
          );
-- Return the result record
    r_output.sr_indicator = 'TRUNCATED_TABLES';
    r_output.sr_value = v_nbTables;
    r_output.sr_rank = 1;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$truncate_all$;

-- The check_fkey() function supress and recreate a foreign key to be sure that the constraint is verified.
-- This may not always be the case because tables are populated in replica mode.
-- It returns 0.
CREATE FUNCTION check_fkey(
    p_batchName                TEXT,
    p_step                     TEXT
    )
    RETURNS SETOF data2pg.step_report_type LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS
$check_fkey$
DECLARE
    v_schema                   TEXT;
    v_table                    TEXT;
    v_fkey                     TEXT;
    v_fkeyDef                  TEXT;
    r_output                   data2pg.step_report_type;
BEGIN
-- Get the step characteristics.
    SELECT stp_schema, stp_object, stp_sub_object INTO v_schema, v_table, v_fkey
        FROM data2pg.step
        WHERE stp_name = p_step;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'check_fkey: Step % not found in the step table.', p_step;
    END IF;
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
-- Return the result record
    r_output.sr_indicator = 'CHECKED_FKEYS';
    r_output.sr_value = 1;
    r_output.sr_rank = 30;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
  RETURN;
END;
$check_fkey$;

-- The check_batch_id() function is called by the data2pg scheduler to check that the requested batch name exists and is in a correct state.
-- It returns either the batch type or an error message.
CREATE FUNCTION check_batch_id(
    p_batchName                TEXT,
OUT p_batchType                TEXT,
OUT p_errorMsg                 TEXT
    )
    LANGUAGE plpgsql AS
$check_batch_id$
DECLARE
    v_migration              TEXT;
    v_isConfigCompleted      BOOLEAN;
BEGIN
    p_batchType = NULL;
    p_errorMsg = '';
-- Get information about the requested batch.
    SELECT coalesce(bat_type, ''), bat_migration, mgr_config_completed
        INTO p_batchType, v_migration, v_isConfigCompleted
        FROM data2pg.batch
             JOIN data2pg.migration ON (bat_migration = mgr_name)
        WHERE bat_name = p_batchName;
-- Perform the checks.
    IF NOT FOUND THEN
        p_errorMsg = 'Batch not found.';
    ELSIF NOT v_isConfigCompleted THEN
        p_errorMsg = 'The migration (' || v_migration || ') configuration is not marked as completed.';
    END IF;
--
    RETURN;
END;
$check_batch_id$;

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
        FROM data2pg.batch
             JOIN data2pg.migration ON (bat_migration = mgr_name)
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
            FROM data2pg.step
            WHERE stp_batch_name = p_batchName;
    RETURN;
END;
$get_working_plan$;

--
-- Set the appropriate rights.
--
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA data2pg FROM public;

DO $$ BEGIN EXECUTE format ('GRANT ALL ON DATABASE %s TO data2pg;', current_database()); END;$$;
GRANT ALL ON ALL TABLES IN SCHEMA data2pg TO data2pg;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA data2pg TO data2pg;

COMMIT;
RESET search_path;
