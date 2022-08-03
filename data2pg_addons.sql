-- data2pg_addons.sql
-- This script contents additional components that may be needed for specific migration projects.
--
\set ON_ERROR_STOP ON

-- Set the search_path to the data2pg extension installation schema.
SELECT set_config('search_path', nspname, false)
    FROM pg_extension JOIN pg_namespace ON (pg_namespace.oid = extnamespace)
    WHERE extname = 'data2pg';

BEGIN TRANSACTION;

-- Add the functions called by your custom steps.
-- They must have the following interface:
-- - Input parameters: p_batchName text, p_step text, p_stepOptions jsonb
-- - Output parameters: SETOF step_report_type
-- They could be declared as SECURITY DEFINER if their actions need it.

-- As an example, the _do_nothing() function can be called by a custom step to do ... nothing.
CREATE OR REPLACE FUNCTION _do_nothing(
    p_batchName                TEXT,
    p_step                     TEXT,
    p_stepOptions              JSONB
    )
    RETURNS SETOF step_report_type LANGUAGE plpgsql AS
$_do_nothing$
DECLARE
    r_output                   step_report_type;
BEGIN
-- Return the step report.
    r_output.sr_indicator = 'DO_NOTHING_OK';
    r_output.sr_value = 1;
    r_output.sr_rank = 98;
    r_output.sr_is_main_indicator = FALSE;
    RETURN NEXT r_output;
--
    RETURN;
END;
$_do_nothing$;


-- Add other components if needed.


COMMIT;
