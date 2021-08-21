<?php
// dbaccess.php
// This file belongs to the Data2Pg web client
// It contains all the sql accesses to the data2pg database.

// The sql_connect() function opens a connection to the data2pg database and verifies that the run table exists
function sql_connect() {
	global $conf;

	// Connection
	$dsn = "host=${conf['data2pg_host']} port=${conf['data2pg_port']} dbname=data2pg user=data2pg password=${conf['data2pg_pwd']}";
	$conn = pg_connect($dsn)
		or die ("Problem while connecting to the data2pg database. You may have to adjust the 'data2pgDsn' configuration value.");

	// Verify that the data2pg schema exists
	$sql = "SELECT 0 FROM information_schema.tables WHERE table_schema = 'data2pg' AND table_name = 'run'";
	$res = pg_query($conn, $sql)
		or die (pg_last_error());
	if (pg_num_rows($res) == 0) {
		die ("No 'data2pg.run' table found in the data2pg database.");
	}
	return $conn;
}

// The sql_close() function closes the connection to the data2pg database.
function sql_close($conn) {

	pg_close($conn);
	return;
}

// The sql_getDatabases() returns the list of target databases that have been configured for Data2Pg.
function sql_getDatabases(){
	global $conn;

	$sql = "SELECT tdb_id, tdb_host, tdb_port, tdb_dbname, coalesce(tdb_description, '') AS tdb_description, tdb_locked,
				   count(run_id) AS nb_run
			FROM data2pg.target_database
				 LEFT OUTER JOIN data2pg.run ON (run_database = tdb_id)
			GROUP BY tdb_id
			ORDER BY tdb_id DESC";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// The sql_getDatabase() returns the entire row for a given target database.
function sql_getDatabase($tdbId){
	global $conn;

	$sql = "SELECT tdb_id, tdb_host, tdb_port, tdb_dbname, coalesce(tdb_description, '') AS tdb_description, tdb_locked,
				   count(run_id) AS nb_run
			FROM data2pg.target_database
				 LEFT OUTER JOIN data2pg.run ON (run_database = tdb_id)
			WHERE tdb_id = $1
			GROUP BY tdb_id";
	$res = pg_query_params($conn, $sql, array($tdbId)) or die(pg_last_error());
	return $res;
}

// The sql_existDatabaseId() function returns a boolean indicating whether a database identified by its id exists in the target_database table.
function sql_existDatabaseId($tdbId){
	global $conn;

	$sql = "SELECT EXISTS(
				SELECT tdb_id
					FROM data2pg.target_database
					WHERE tdb_id = $1)";
	$res = pg_query_params($conn, $sql, array($tdbId)) or die(pg_last_error());
	return $res;
}

// The sql_existDatabase() function returns a boolean indicating whether a database  identified by its name, host name and port exists in the target_database table.
function sql_existDatabase($tdbHost, $tdbPort, $tdbDbname){
	global $conn;

	$sql = "SELECT EXISTS(
				SELECT tdb_id
					FROM data2pg.target_database
					WHERE tdb_host = $1 AND tdb_port = $2 AND tdb_dbname = $3)";
	$res = pg_query_params($conn, $sql, array($tdbHost, $tdbPort, $tdbDbname)) or die(pg_last_error());
	return $res;
}

// The sql_insertDatabase() inserts a row into the target_database table.
function sql_insertDatabase($tdbId, $tdbHost, $tdbPort, $tdbDbname, $tdbDescription){
	global $conn;

	$sql = "INSERT INTO data2pg.target_database
					(tdb_id, tdb_host, tdb_port, tdb_dbname, tdb_description)
			VALUES  ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING";
	$res = pg_query_params($conn, $sql, array($tdbId, $tdbHost, $tdbPort, $tdbDbname,
											  ($tdbDescription <> '') ? $tdbDescription : NULL))
		or die(pg_last_error());
	return $res;
}

// The sql_updateLockDatabase() sets the tdb_locked column to TRUE or FALSE.
function sql_updateLockDatabase($tdbId, $trueFalse){
	global $conn;

	$sql = "UPDATE data2pg.target_database
				SET tdb_locked = $trueFalse
			WHERE tdb_id = $1";
	$res = pg_query_params($conn, $sql, array($tdbId))
		or die(pg_last_error());
	return $res;
}

// The sql_updateDatabase() sets the modified database properties.
function sql_updateDatabase($tdbId, $tdbHost, $tdbPort, $tdbDbname, $tdbDescription){
	global $conn;

	$sql = "UPDATE data2pg.target_database
				SET tdb_host = $1, tdb_port = $2, tdb_dbname = $3, tdb_description = $4
			WHERE tdb_id = $5";
	$res = pg_query_params($conn, $sql, array($tdbHost, $tdbPort, $tdbDbname,
											  ($tdbDescription <> '') ? $tdbDescription : NULL, $tdbId))
		or die(pg_last_error());
	return $res;
}

// The sql_deleteDatabase() deletes a row from the target_database table.
function sql_deleteDatabase($tdbId){
	global $conn;

	$sql = "DELETE FROM data2pg.target_database
			WHERE tdb_id = $1";
	$res = pg_query_params($conn, $sql, array($tdbId))
		or die(pg_last_error());
	return $res;
}

// The sql_waitForRunStart() function waits until a given run is started.
// It returns 1 as soon as the run is visible into the run table. It returns 0 if the run is not visible after 5 seconds.
function sql_waitForRunStart($runId) {
	global $conn;

// Look at the run table at most 10 times and wait 1/2 second between each attempt.
	$maxRetry = 10;
	$sleepDelay = 500000;			// in microseconds
	$retryCount = 0;
	$found = 0;
	while (!$found && $retryCount < $maxRetry) {
		$sql = "SELECT 1 FROM data2pg.run WHERE run_id = $runId";
		$res = pg_query($conn, $sql) or die(pg_last_error());
		if (pg_num_rows($res) == 0) {
			$retryCount++;
		} else {
			$found = 1;
		}
		usleep($sleepDelay);
	}
	return $found;
}

// The sql_getPreviousRun() function returns the id and the status about the most recent run for a given target database and batch.
function sql_getPreviousRun($tdbId, $batch) {
	global $conn;

	$sql = "SELECT run_id, run_status
			FROM data2pg.run
			WHERE run_database = $1 AND run_batch_name = $2
			ORDER BY run_start_ts DESC
			LIMIT 1";
	$res = pg_query_params($conn, $sql, array($tdbId, $batch))
		or die(pg_last_error());
	return $res;
}

// The sql_getAllRuns() function returns a synthesis of all runs.
function sql_getAllRuns(){
	global $conn;

	$sql = "SELECT run_id, run_database, run_batch_name, run_batch_type,
				   run_init_max_ses, run_init_asc_ses, run_comment,
				   to_char(run_start_ts, 'YYYY-MM-DD HH24:MI:SS') as run_start_ts,
				   to_char(run_end_ts, 'YYYY-MM-DD HH24:MI:SS') as run_end_ts,
				   CASE WHEN coalesce(run_end_ts, current_timestamp) - run_start_ts < '1 DAY'
							THEN to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'HH24:MI:SS')
						ELSE to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'FMDDD \"days\" HH24:MI:SS')
						END as elapse,
				   run_status, run_max_sessions, run_asc_sessions,
				   run_error_msg, run_restart_id, run_perl_pid
			FROM data2pg.run
			ORDER BY run_id DESC";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// The sql_getInProgressRuns() function returns a synthesis of all runs.
function sql_getInProgressRuns(){
	global $conn;

	$sql = "SELECT run_id, run_database, run_batch_name, run_batch_type,
				   run_init_max_ses, run_init_asc_ses, run_comment,
				   to_char(run_start_ts, 'YYYY-MM-DD HH24:MI:SS') as run_start_ts,
				   to_char(run_end_ts, 'YYYY-MM-DD HH24:MI:SS') as run_end_ts,
				   CASE WHEN coalesce(run_end_ts, current_timestamp) - run_start_ts < '1 DAY'
							THEN to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'HH24:MI:SS')
						ELSE to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'FMDDD \"days\" HH24:MI:SS')
						END as elapse,
				   run_status, run_max_sessions, run_asc_sessions,
				   run_error_msg, run_restart_id, run_perl_pid,
				   count(step.*) AS total_steps,
				   count(step.*) FILTER (WHERE stp_status = 'Completed') AS completed_steps
			FROM data2pg.run
			   JOIN data2pg.step ON (stp_run_id = run_id)
			WHERE run_status IN ('Initializing', 'In_progress', 'Ending')
			GROUP BY run_id
			ORDER BY run_id DESC";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// The sql_getAdjacentRuns() function returns the first, previous, next and last run id for a given run id.
function sql_getAdjacentRuns($runId) {
	global $conn;

	$sql = "SELECT min(run_id) AS first_run,
				   max(run_id) FILTER (WHERE run_id < $runId) AS previous_run,
				   min(run_id) FILTER (WHERE run_id > $runId) AS next_run,
				   max(run_id) AS last_run
				FROM data2pg.run";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// The sql_getRun() function returns a summary of a given run.
function sql_getRun($runId) {
	global $conn;

	$sql = "SELECT run_id, run_database, run_batch_name, run_batch_type, run_step_options,
				   run_init_max_ses, run_init_asc_ses, run_comment,
				   to_char(run_start_ts, 'YYYY-MM-DD HH24:MI:SS') as run_start_ts,
				   to_char(run_end_ts, 'YYYY-MM-DD HH24:MI:SS') as run_end_ts,
				   CASE WHEN coalesce(run_end_ts, current_timestamp) - run_start_ts < '1 DAY'
							THEN to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'HH24:MI:SS')
						ELSE to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'FMDDD \"days\" HH24:MI:SS')
						END as elapse,
				   run_status, run_max_sessions, run_asc_sessions,
				   run_error_msg, run_restart_id, run_restarted_id, run_perl_pid,
				   tdb_host, tdb_port, tdb_dbname, tdb_description,
				   count(step.*) AS total_steps,
				   sum(stp_cost) AS total_cost,
				   count(step.*) FILTER (WHERE stp_status = 'Completed') AS completed_steps,
				   sum(stp_cost) FILTER (WHERE stp_status = 'Completed') AS completed_cost,
				   count(step.*) FILTER (WHERE stp_status = 'In_progress') AS in_progress_steps,
				   sum(stp_cost) FILTER (WHERE stp_status = 'In_progress') AS in_progress_cost
			  FROM data2pg.run
				   JOIN data2pg.target_database ON (tdb_id = run_database)
				   JOIN data2pg.step ON (stp_run_id = run_id)
			WHERE run_id = $runId
			GROUP BY run_id, tdb_id";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// the sql_getStepResultsSummary() function returns an aggregate of elementary steps results for a given run.
function sql_getStepResultsSummary($runId) {
	global $conn;

	$sql = "SELECT sr_indicator, sr_rank, sum(sr_value) AS sum_value
			FROM data2pg.step_result
			WHERE sr_run_id = $runId
			GROUP BY sr_indicator, sr_rank
			ORDER BY sr_rank";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// the sql_getSteps() function returns the details of each elementary step of a given run.
function sql_getSteps($runId, $runStatus) {
	global $conn;

	$sql = "SELECT stp_name, stp_cost, stp_status, array_length(stp_blocking, 1) AS nb_blocking, stp_ses_id,
				   to_char(stp_start_ts, 'YYYY-MM-DD HH24:MI:SS.US') AS stp_start_ts, to_char(stp_end_ts, 'YYYY-MM-DD HH24:MI:SS.US') AS stp_end_ts,
				   CASE WHEN stp_status = 'Completed' AND stp_end_ts - stp_start_ts < '1 DAY'
							THEN to_char(stp_end_ts - stp_start_ts, 'HH24:MI:SS.US')
						WHEN stp_status = 'Completed' AND stp_end_ts - stp_start_ts >= '1 DAY'
							THEN to_char(stp_end_ts - stp_start_ts, 'FMDDD \"days\" HH24:MI:SS.US')";
// Only compute elapse time of the in progress steps when the run is effectively in progress (i.e. has not been aborted)
	if ($runStatus == 'In_progress') {
		$sql .= "
						WHEN stp_status = 'In_progress' AND coalesce(stp_end_ts, current_timestamp) - stp_start_ts < '1 DAY'
							THEN to_char(current_timestamp - stp_start_ts, 'HH24:MI:SS')
						WHEN stp_status = 'In_progress' AND coalesce(stp_end_ts, current_timestamp) - stp_start_ts >= '1 DAY'
							THEN to_char(coalesce(stp_end_ts, current_timestamp) - stp_start_ts, 'FMDDD \"days\" HH24:MI:SS')";
	}
	$sql .= "
						ELSE NULL
				   END AS stp_elapse,
				   sr_value
			FROM data2pg.step
				 LEFT OUTER JOIN data2pg.step_result ON (stp_run_id = sr_run_id AND stp_name = sr_step AND sr_is_main_indicator),
				 (VALUES ('In_progress',1),('Ready',2),('Blocked',3),('Completed',4)) AS state(state_name, state_order)
			WHERE state_name = stp_status::TEXT
			  AND stp_run_id = $runId
			ORDER BY state_order, stp_start_ts DESC, stp_cost DESC, stp_name";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// the sql_getNextRunId() function increments the run_run_id_seq sequence and returns the new value.
function sql_getNextRunId() {
	global $conn;

	$sql = "SELECT nextval('run_run_id_seq')";
	$res = pg_query($conn, $sql) or die(pg_last_error());
	return $res;
}

// The sql_getExternalRunStart() returns the columns to display of the external_run_start table for a given run id, if exists.
function sql_getExternalRunStart($runId){
	global $conn;

	$sql = "SELECT ext_client, ext_sched_log_file
			FROM data2pg.external_run_start
			WHERE ext_run_id = $1";
	$res = pg_query_params($conn, $sql, array($runId)) or die(pg_last_error());
	return $res;
}

// The sql_insertExternalRunStart() inserts a row into the external_run_start table.
function sql_insertExternalRunStart($runId, $schedAddr, $schedAccount, $schedCommand, $schedLogFile){
	global $conn;

	$sql = "INSERT INTO data2pg.external_run_start
					(ext_run_id, ext_client, ext_sched_addr, ext_sched_account, ext_sched_command, ext_sched_log_file)
			VALUES  ($1, 'Data2Pg WebApp', $2, $3, $4, $5)";
	$res = pg_query_params($conn, $sql, array($runId, $schedAddr, $schedAccount, $schedCommand, $schedLogFile))
		or die(pg_last_error());
	return $res;
}

// The sql_updateRun() sets the modified run properties.
function sql_updateRun($runId, $maxSession, $ascSessions, $comment){
	global $conn;

	$sql = "UPDATE data2pg.run
				SET run_max_sessions = $1, run_asc_sessions = $2, run_comment = $3
			WHERE run_id = $4";
	$res = pg_query_params($conn, $sql, array($maxSession, $ascSessions, ($comment <> '') ? $comment : NULL, $runId))
		or die(pg_last_error());
	return $res;
}

?>
