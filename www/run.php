<?php
// run.php
// This file belongs to the Data2Pg web client

	session_start();
	$pageId="run.php";

	require_once('inc/constants.php');
	require_once('conf/config.inc.php');
	require_once('inc/htmlcommon.php');
	require_once('inc/dbaccess.php');
	require_once('inc/shell.php');

	PageHeader();

	$a = @$_GET["a"];
	$runId = @$_GET["runId"];

	$conn = sql_connect();

	switch ($a) {
	  case "runDetails":
		runDetails($runId);
		break;
	  case "checkRun":
		checkRun($runId);
		break;
	  case "alterRun":
		alterRun($runId);
		break;
	  case "doAlterRun":
		doAlterRun($runId);
		break;
	  case "abortRun":
		abortRun($runId);
		break;
	  case "doAbortRun":
		doAbortRun($runId);
		break;
	  case "restartRun":
		restartRun($runId);
		break;
	  case "doRestartRun":
		doRestartRun($runId);
		break;
	  default:
		echo "!!! The action $a is unknown !!!;";
		break;
	}
	sql_close($conn);

	if (isset($runId)) $_SESSION["runid"] = $runId;

	PageFooter();

// The runDetails() function displays data related to each step of a given run.
function runDetails($runId, $msg = '') {
	global $conf;

// Display the message, if passed as parameter
	if ($msg != '') {
		$class = (substr($msg, 0, 5) == 'Error') ? "errMsg" : "msg";
		echo "\t\t\t<p class=\"$class\">$msg</p>\n";
	}

// Get general information about the run
	$res = sql_getRun($runId);
	$nbRows = pg_num_rows($res);

	if ($nbRows > 0) {
		$run = pg_fetch_assoc($res);

// Get data about the external application that launched the run, if any.
		$res = sql_getExternalRunStart($runId);
		$existExternalRunStart = pg_num_rows($res);
		if ($existExternalRunStart) {$externalRunStart = pg_fetch_assoc($res);}

// Get the min, max and adjacent run ids to prepare links.
		$res = sql_getAdjacentRuns($runId);
		$runIds = pg_fetch_assoc($res);
		$firstRun = $runIds['first_run'];
		$previousRun = $runIds['previous_run'];
		$nextRun = $runIds['next_run'];
		$lastRun = $runIds['last_run'];

// Determine whether an auto refresh button has to be generated
		$autoRefresh = ($run['run_status'] == 'Initializing' || $run['run_status'] == 'In_progress');

// Display the page title.
		$centerTitle = '';
		$class = ($runId > $firstRun) ? "" : " hidden";
		$centerTitle .= "<a href=run.php?a=runDetails&runId=$firstRun class=\"button mainButton $class\">&nbsp;&lt;&lt;&nbsp;</a>";
		$centerTitle .= "<a href=run.php?a=runDetails&runId=$previousRun class=\"button mainButton $class\">&nbsp;&lt;&nbsp;</a>";

		$centerTitle .= "&nbsp;&nbsp;Run #$runId&nbsp;&nbsp;";

		$class = ($runId < $lastRun) ? "" : " hidden";
		$centerTitle .= "<a href=run.php?a=runDetails&runId=$nextRun class=\"button mainButton $class\">&nbsp;&gt;&nbsp;</a>";
		$centerTitle .= "<a href=run.php?a=runDetails&runId=$lastRun class=\"button mainButton $class\">&nbsp;&gt;&gt;&nbsp;</a>";

// Add some javascript code for the automatic page reload, if needed.
		if ($autoRefresh) {
			echo "<script language='Javascript'>\n";
			echo "\tautoRefresh=1;\n";
			echo "\tfunction reload() {window.location.href=\"run.php?a=runDetails&runId=$runId\";}\n";
			echo "\tfunction switchRefresh() { \n";
			echo "\t  if (autoRefresh) {\n";
			echo "\t    autoRefresh=0;\n";
			echo "\t    clearTimeout(refreshTimer);\n";
			echo "\t    document.getElementById('refreshButton').innerHTML= 'Start Refresh';\n";
			echo "\t  }else{\n";
			echo "\t    reload();\n";
			echo "\t  }\n";
			echo "\t}\n";
			$delay = $conf['refresh_delay'] * 1000;
			echo "\trefreshTimer=window.setInterval(\"reload();\", $delay);\n";
			echo "</script>\n";
		}

// Display additional buttons on the left div title, depending on the run status.
		$leftTitle = '';
		if ($autoRefresh) {
			$leftTitle .= "\t\t<a id=\"refreshButton\" onclick=\"switchRefresh();\" class=\"button mainButton\">Stop Refresh</a>\n";
		}
	
// Display additional buttons on the right div title, depending on the run status.
		$rightTitle = '';
		if ($conf['read_only'] == 0) {
			if ($run['run_status'] == 'Initializing' || $run['run_status'] == 'In_progress') {
				if ($conf['exec_command'] <> 0) {
					$rightTitle .= "\t\t<a href=\"run.php?a=checkRun&runId=$runId\" class=\"button mainButton\">Check</a>\n";
				}
				$rightTitle .= "\t\t<a href=\"run.php?a=alterRun&runId=$runId\" class=\"button mainButton\">Alter</a>\n";
				if ($conf['exec_command'] <> 0) {
					$rightTitle .= "\t\t<a href=\"run.php?a=abortRun&runId=$runId\" class=\"button mainButton\">Abort</a>\n";
				}
			}
			if ($run['run_status'] == 'Suspended' || ($run['run_status'] == 'Aborted' && $run['run_restart_id'] == '' )) {
				if ($conf['exec_command'] <> 0) {
					$rightTitle .= "\t\t<a href=\"run.php?a=restartRun&runId=$runId\" class=\"button mainButton\">Restart</a>\n";
				}
			}
		}

		mainTitle($leftTitle, $centerTitle , $rightTitle);

// Display the global information about the run.
		echo "<div id=\"runDetails\">\n";

		echo "\t<p>Target database = <span class=\"bold\">" . htmlspecialchars($run['run_database']) . "</span>&nbsp;";
		echo "(" . htmlspecialchars($run['tdb_dbname']) . "&nbsp;on&nbsp;" . htmlspecialchars($run['tdb_host']) . ":" . htmlspecialchars($run['tdb_port']) . ")\n";
		if ($run['tdb_description'] != '') {
			echo "<img src=\"img/comment.png\" alt=\"comment\" width=\"24\" height=\"24\" title=\"" . htmlspecialchars($run['tdb_description']) . "\">";
		}

		echo "\t</p>\n";
		echo "\t<p>Batch = <span class=\"bold\">" . htmlspecialchars($run['run_batch_name']) . "</span>" .
			"&nbsp;(type " . htmlspecialchars($run['run_batch_type']) . ")";
		if ($run['run_step_options'] != '') {
			echo "&nbsp;&nbsp;Step options = " . htmlspecialchars($run['run_step_options']);
		}
		echo "\t</p>\n";

		if ($run['run_comment'] != '') {
			echo "\t<p>Comment = " . htmlspecialchars($run['run_comment']) . "</p>\n";
		}

		if ($existExternalRunStart) {
			echo "\t<p>Launched by \"" . htmlspecialchars($externalRunStart['ext_client']) . "\" - Log file = " .
				htmlspecialchars($externalRunStart['ext_sched_log_file']) . "</p>\n";
		}

		echo "\t<p>Status = <span class=\"bold\">" . htmlspecialchars($run['run_status']) . "</span>";
		if ($run['run_status'] == 'Initializing' || $run['run_status'] == 'In_progress') {
			echo " - Scheduler pid = ${run['run_perl_pid']}";
		}
		if (isset($run['run_restart_id'])) {
			echo " - Restarted by the run <a href=\"run.php?a=runDetails&runId=${run['run_restart_id']}\">" .
				htmlspecialchars($run['run_restart_id']) . "</a>";
		}
		if (isset($run['run_restarted_id'])) {
			echo " - Has restarted the run <a href=\"run.php?a=runDetails&runId=${run['run_restarted_id']}\">" .
				htmlspecialchars($run['run_restarted_id']) . "</a>";
		}
		echo "\t</p>\n";

		echo "\t<p>Max sessions = ${run['run_max_sessions']} (initialy ${run['run_init_max_ses']})&nbsp;-&nbsp;";
		echo "Sessions in cost ascending order = ${run['run_asc_sessions']} (initialy ${run['run_init_asc_ses']})</p>\n";

		if ($run['run_error_msg'] != '') {
			echo "\t<p>Error = " . htmlspecialchars($run['run_error_msg']) . "</p>\n";
		}
		echo "\t<p>Start = ${run['run_start_ts']}";
		if ($run['run_end_ts'] != '') {
			echo " - End = ${run['run_end_ts']} ";
		}
		echo " - Elapse = ${run['elapse']}</p>\n";

		echo "\t<p>Steps: " . htmlspecialchars($run['total_steps']);
		if ($run['total_steps'] > 0) {
			echo " - Completed: "  . htmlspecialchars($run['completed_steps']) .
				 " (" . htmlspecialchars(number_format(($run['completed_steps'] * 100 / $run['total_steps']), 1)) . "% / " .
						htmlspecialchars(number_format($run['completed_cost'] * 100 / $run['total_cost']), 1) ."%)";
			if ($run['run_status'] == 'In_progress') {
				echo " - In-progress: "  . htmlspecialchars($run['in_progress_steps']) .
					" (" . htmlspecialchars(number_format($run['in_progress_steps'] * 100 / $run['total_steps'], 1)) . "% / " .
							htmlspecialchars(number_format($run['in_progress_cost'] * 100 / $run['total_cost'], 1)) ."%)";
				echo " - Others: "  . htmlspecialchars($run['total_steps'] - $run['completed_steps'] - $run['in_progress_steps']) .
					" (" . htmlspecialchars(number_format((($run['total_steps'] - $run['completed_steps'] - $run['in_progress_steps']) * 100) / $run['total_steps'], 1)) . "% / " .
							htmlspecialchars(number_format((($run['total_cost'] - $run['completed_cost'] - $run['in_progress_cost']) * 100) / $run['total_cost'], 1)) ."%)";
			}
		}
		echo "\t</p>\n";
		echo "</div>\n";

// Get the step results summary
		$res = sql_getStepResultsSummary($runId);
		$nbRows = pg_num_rows($res);

		if ($nbRows > 0) {
// Display the step results summary
			echo "<div id=\"resultsSummary\">\n";
// Header.
			echo "<table class='tbl'>\n";
			echo "\t<tr>\n";
			echo "\t\t<th>Indicator</th>\n";
			echo "\t\t<th>Sum values</th>\n";
			echo "\t</tr>\n";
// Display each line of the sheet.
			for ($i = 0; $i < $nbRows; $i++)
			{
				$row = pg_fetch_assoc($res);
				$style = "even"; if ($i % 2 != 0) {$style = "odd";}

				echo "\t<tr class='${style}'>\n";
				echo "\t\t<td>" . htmlspecialchars($row['sr_indicator']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($row['sum_value']) . "</td>\n";
				echo "\t</tr>\n";
			}
			echo "</table></div>\n";
		}

// Get the elementary steps.
		$res = sql_getSteps($runId, $run['run_status']);
		$nbRows = pg_num_rows($res);

		if ($nbRows > 0) {
// Display the step sheet.
// Header.
			echo "<table class='tbl'>\n";
			echo "\t<tr>\n";
			echo "\t\t<th>Step name</th>\n";
			echo "\t\t<th>Estim. cost</th>\n";
			echo "\t\t<th>Status</th>\n";
			echo "\t\t<th>Session</th>\n";
			echo "\t\t<th>#blocking</th>\n";
			echo "\t\t<th>Start</th>\n";
			echo "\t\t<th>End</th>\n";
			echo "\t\t<th>Elapse</th>\n";
			echo "\t\t<th>Main return</th>\n";
			echo "\t</tr>\n";

// Display each line of the sheet.
			for ($i = 0; $i < $nbRows; $i++)
			{
				$step = pg_fetch_assoc($res);
				$style = "even"; if ($i % 2 != 0) {$style = "odd";}
				if ($step['stp_status'] == 'Completed' && $step['stp_start_ts'] < $run['run_start_ts']) $style .= " fromPreviousRun";

				echo "\t<tr class=\"$style\">\n";
				echo "\t\t<td class='alignLeft'>" . htmlspecialchars($step['stp_name']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_cost']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_status']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_ses_id']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['nb_blocking']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_start_ts']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_end_ts']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['stp_elapse']) . "</td>\n";
				echo "\t\t<td>" . htmlspecialchars($step['sr_value']) . "</td>\n";
				echo "\t</tr>\n";
			}
			echo "</table>\n";
		}
	} else {

// The requested run id does not exist !
		echo "<p class=\"errMsg\">The run id $runId does not exist.</p>\n";
	}
}

// The checkRun() function checks the state of a given run.
// If the data2pg.pl pid associated to the run is not executing anymore, it marks the run as aborted.
function checkRun($runId) {
	global $conf;

// Get the run characteristics
	$res = sql_getRun($runId);
	$run = pg_fetch_assoc($res);

	if ($run['run_status'] != 'Initializing' && $run['run_status'] != 'In_progress') {
// The run has not the right state anymore to be checked.
		runDetails($runId, "The run $runId is not in 'Initializing' or 'In_progess' state anymore'.");

	} else {
// OK, perform the check.
		mainTitle('', "Checking the run #$runId", '');

// Look at the perl scheduler pid on its system, using a ps shell command.
		$shellConn = shellOpen();
		$cmd = "ps --pid ${run['run_perl_pid']} -o args --no-headers |grep 'data2pg.pl'";
		$outputPs = shellExec($shellConn, $cmd);

// Display the current run state.
		if ($outputPs != '') {
// The run is executing. Just display a message.
			runDetails($runId, "The data2pg.pl pid for the run $runId is still executing.");
		} else {
// The run is not in execution anymore, so abort it using a "data2pg.pl --action abort" command
			$cmd = $conf['schedulerPath'] .
				" --host ${conf['data2pg_host']} --port ${conf['data2pg_port']} --action abort --target ${run['run_database']} --batch ${run['run_batch_name']} 2>&1";
			$outputAbort = shellExec($shellConn, $cmd);
			if (strpos($outputAbort, ' has been aborted.') !== false) {
				echo "<p align='center'>The run " . htmlspecialchars($row['run_id']) . " was not executing anymore. It has been marked as 'aborted'.</p>\n";
				echo "<a href=runs.php?a=displayInProgressRuns class=\"button mainButton\">OK</a\n";
			} else {
				echo "<p align='center'>The run " . htmlspecialchars($row['run_id']) . " was not executing anymore. But an error occured when trying to mark it as 'aborted'.</p>\n";
				echo "<pre style=\"text-align:left;\">$outputAbort</pre>";
				echo "<a href=run.php?a=runDetails&runId=$runId class=\"button mainButton\">OK</a\n";
			}
		}
		shellClose($shellConn);
	}
}

// The alterRun() function allows to adjust number of parallel sessions for in progress runs and comment for any run.
function alterRun($runId) {

// Get the run characteristics.
	$res = sql_getRun($runId);
	if (pg_num_rows($res) <> 1) {
		runDetails($runId, "Error: internal error while getting the characteristics of the run $runId.");
	} else {
		$run = pg_fetch_assoc($res);

// Display the form.
		mainTitle('', "Alter the run #$runId", '');
		echo "<div id=\"newRun\">\n";
		echo "\t<form name=\"altetRun\" action=\"run.php\" method=\"get\">\n";
		echo "<div class=\"form-container\">\n";
		echo "\t\t<input type=\"hidden\" name=\"a\" value=\"doAlterRun\">\n";
	
		echo "\t\t<input type=\"hidden\" name=\"runId\" value=\"$runId\">\n";
	
		echo "\t\t<div class=\"form-label\">Max sessions (0 to suspend the run)</div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"maxSession\" size=3 min=0 max=999 value=${run['run_max_sessions']}></div>\n";
	
		echo "\t\t<div class=\"form-label\">Sessions in cost ascending order</div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"ascSession\" size=3 min=0 max=999 value=${run['run_asc_sessions']}></div>\n";
	
		echo "\t\t<div class=\"form-label\">Comment</div>";
		echo "\t\t<div class=\"form-input\"><input name=\"comment\" size=60 value=\"${run['run_comment']}\"></div>\n";

		echo "\t</div>\n";
		echo "\t<p>\n";
		echo "\t\t<input type=\"submit\" name=\"OK\" value=\"OK\">\n";
		echo "\t\t<input type=\"reset\" value=\"Reset\" onClick='boutonReset();'>\n";
		echo "\t\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='run.php?a=runDetails&runId=$runId';\">\n";
		echo "\t</p></form>\n";
		echo "</div>\n";
	}
}

// The doAlterRun() function effectively alters a given run.
function doAlterRun($runId) {

	$comment = @$_GET["comment"];
	$maxSession = @$_GET["maxSession"];
	$ascSession = @$_GET["ascSession"];

// Perform the insertion
	$res = sql_updateRun($runId, $maxSession, $ascSession, $comment);

	if (pg_affected_rows($res) <> 1) {
		$msg = "Error: internal error while updating the run's properties.";
	} else {
		$msg = "The run $runId has been altered.";
	}

// Display the modified databases list.
	runDetails($runId, $msg);
}

// The abortRun() function aborts a run that is effectively in progress, or marks a run whose perl_pid is not in execution anymore.
function abortRun($runId) {

// Get the run characteristics.
	$res = sql_getRun($runId);
	if (pg_num_rows($res) <> 1) {
		runDetails($runId, "Error: internal error while getting the characteristics of the run $runId.");
	} else {
		$run = pg_fetch_assoc($res);

// Check the run is always in 'In_Progress' state.
		if ($run['run_status'] != 'Initializing' && $run['run_status'] != 'In_progress') {
			runDetails($runId, "Error: the run $runId is not in 'Initializing' or 'In_progress' state anymore.");
		} else {

// Ask for the confirmation.
			mainTitle('', "Please confirm the abort of the run #$runId", '');
	
			echo "<div id=\"runToAbort\">\n";
			echo "\t<p>Target database <b>${run['run_database']}</b> - Batch <b>${run['run_batch_name']}</b> (type ${run['run_batch_type']})</p>\n";
			echo "\t<p><form name=\"confirmAbortRun\" action='run.php' method='get'>\n";
			echo "\t\t<input type='hidden' name='a' value='doAbortRun'>\n";
			echo "\t\t<input type='hidden' name='runId' value='$runId'>\n";
			echo "\t\t<input type='submit' value='OK'>\n";
			echo "\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='run.php?a=runDetails&runId=$runId';\">\n";
			echo "\t</form></p>\n";
			echo "</div>\n";
		}
	}
}

function doAbortRun($runId) {
	global $conf;

// Get the run characteristics
	$res = sql_getRun($runId);
	$run = pg_fetch_assoc($res);

	if ($run['run_status'] != 'Initializing' && $run['run_status'] != 'In_progress') {
// The run has not the right state anymore to be aborted.
		$msg = "The run $runId is not in 'Initializing' or 'In_progess' state anymore";

	} else {
// OK, perform the run abort using a "data2pg.pl --action abort" command.
		$shellConn = shellOpen();
		$cmd = $conf['schedulerPath'] .
			" --host ${conf['data2pg_host']} --port ${conf['data2pg_port']} --action abort --target ${run['run_database']} --batch ${run['run_batch_name']} 2>&1";
		$outputAbort = shellExec($shellConn, $cmd);
// Check the data2pg.pl report.
		if (strpos($outputAbort, ' has been aborted.') !== false) {
			$msg = "The run $runId has been aborted.";
		} else {
			$msg = "Error: An error occurred while aborting the run $runId.<br><pre>$outputAbort</pre>";
		}
		shellClose($shellConn);
	}
	runDetails($runId, $msg);
}

// The restartRun() function allows to restart a job that was either suspended or aborted and that has not been already restarted.
// After some checks, it asks the user to confirm the restart action.
function restartRun($runId) {
	global $conf;

// Get the run characteristics
	$res = sql_getRun($runId);

	if (pg_num_rows($res) <> 1) {
		runDetails($runId, "Error: internal error while getting the characteristics of the run $runId.");
	} else {
		$run = pg_fetch_assoc($res);

// Check the run is always in 'Suspended' or 'Aborted' state and has not been already restarted.
		if ($run['run_status'] != 'Suspended' && $run['run_status'] != 'Aborted') {
			runDetails($runId, "Error: the run $runId is not in 'Suspended' state anymore.");
		} elseif (isset($run['run_restart_id'])) {
			runDetails($runId, "Error: the run $runId has already been restarted.");
		} else {

// Decode the step options from the previous run.
			$copyMaxRows = ''; $copySlowDown = '';
			if (preg_match('/"COPY_MAX_ROWS": (\d+)/', $run['run_step_options'], $matches)) {
				$copyMaxRows = $matches[1];
			}
			if (preg_match('/"COPY_SLOW_DOWN": (\d+)/', $run['run_step_options'], $matches)) {
				$copySlowDown = $matches[1];
			}
			if (preg_match('/"COMPARE_MAX_ROWS": (\d+)/', $run['run_step_options'], $matches)) {
				$compareMaxRows = $matches[1];
			}
			if (preg_match('/"COMPARE_MAX_DIFF": (\d+)/', $run['run_step_options'], $matches)) {
				$compareMaxDiff = $matches[1];
			}
			$compareTruncateDiff = preg_match('/"COMPARE_MAX_DIFF":/', $run['run_step_options']);
			if (preg_match('/"DISCOVER_MAX_ROWS": (\d+)/', $run['run_step_options'], $matches)) {
				$discoverMaxRows = $matches[1];
			}

// OK, display the form to adjust the run execution parameters, if needed. The parameters from the previous run are proposed by default.
			mainTitle('', "Restart the run #$runId", '');
	
			echo "<div id=\"runToRestart\">\n";
			echo "\t<p>Target database <b>${run['run_database']}</b> - Batch <b>${run['run_batch_name']}</b> (type ${run['run_batch_type']})</p>\n";
			echo "\t<p><form name=\"confirmRestartRun\" action='run.php' method='get'>\n";
			echo "\t<div class=\"form-container\">\n";
			echo "\t\t<input type='hidden' name='a' value='doRestartRun'>\n";
			echo "\t\t<input type='hidden' name='runId' value='$runId'>\n";

			echo "\t\t<div class=\"form-label\">Max sessions</div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"maxSession\" size=3 min=0 max=999 value=${run['run_init_max_ses']}></div>\n";

			echo "\t\t<div class=\"form-label\">Sessions in cost ascending order</div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"ascSession\" size=3 min=0 max=999 value=${run['run_init_asc_ses']}></div>\n";

			echo "\t\t<div class=\"form-label\">Comment</div>";
			echo "\t\t<div class=\"form-input\"><input name=\"comment\" size=60 value=\"" . htmlspecialchars($run['run_comment']) . "\"></div>\n";

			echo "\t</div>\n";
			echo "\t<p>Step options</p>";

			echo "<div class=\"form-container\">\n";

			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COPY_MAX_ROWS</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"copyMaxRows\" size=6 min=1 value=$copyMaxRows></div>\n";

			if ($conf['development_mode']) {
				echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COPY_SLOW_DOWN (µs/row)</span></div>";
				echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"copySlowDown\" size=6 min=0 value = $copySlowDown></div>\n";
			}

			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_MAX_ROWS</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"compareMaxRows\" size=6 min=1 value=$compareMaxRows></div>\n";

			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_MAX_DIFF</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"compareMaxDiff\" size=6 min=1 value=$compareMaxDiff></div>\n";

			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_TRUNCATE_DIFF</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"checkbox\" name=\"compareTruncateDiff\"";
			if ($compareTruncateDiff) {
				echo "checked";
			}
			echo "></div>\n";

			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">DISCOVER_MAX_ROWS</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"discoverMaxRows\" size=6 min=1 value=$discoverMaxRows></div>\n";

			echo "\t</div>\n";

			echo "\t<p>\n";
			echo "\t\t<input type='submit' value='Start and Monitor'>\n";
			echo "\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='run.php?a=runDetails&runId=$runId';\">\n";
			echo "\t</form></p>\n";
			echo "</div>\n";
		}
	}
}

// The doRestartRun() function effectively restarts a run.
function doRestartRun($runId) {
	global $conf;

	$comment = @$_GET["comment"];
	$maxSession = @$_GET["maxSession"];
	$ascSession = @$_GET["ascSession"];
	$copyMaxRows = @$_GET["copyMaxRows"];
	$copySlowDown = @$_GET["copySlowDown"];
	$compareMaxRows = @$_GET["compareMaxRows"];
	$compareMaxDiff = @$_GET["compareMaxDiff"];
	$compareTruncateDiff = @$_GET["compareTruncateDiff"];
	$discoverMaxRows = @$_GET["discoverMaxRows"];

// Get the run characteristics.
	$res = sql_getRun($runId);

	if (pg_num_rows($res) <> 1) {
		runDetails($runId, "Error: internal error while getting the characteristics of the run $runId.");
	} else {
		$run = pg_fetch_assoc($res);

// Check the run is always in 'Suspended' or 'Aborted' state and has not been already restarted.
		if ($run['run_status'] != 'Suspended' && $run['run_status'] != 'Aborted') {
			runDetails($runId, "Error: the run $runId is not in 'Suspended' state anymore.");
		} elseif (isset($run['run_restart_id'])) {
			runDetails($runId, "Error: the run $runId has already been restarted.");
		} else {

// Define the log file name.
			$logFile = $conf['scheduler_log_dir'] . '/' . date('Ymd_His');

// Create the log file on the scheduler server.
// The directory tree is previously created, if needed.
			$shellConn = shellOpen();
			$cmd = "mkdir -p ${conf['scheduler_log_dir']}; touch $logFile; chmod 666 $logFile 2>&1";
			$outputTouch = shellExec($shellConn, $cmd);

			if ($outputTouch != '') {
				runDetails($runId, "Error: trying to create the scheduler log file failed.<pre style=\"text-align:left;\">$outputTouch</pre>");
				shellClose($shellConn);

			} else {

// OK, get the next run id from the run_run_id_seq sequence.
				$res = sql_getNextRunId();
				$newRunId = pg_fetch_result($res, 0, 0);

// Build the step options parameter.
				$stepOptions = '';
				if ($copyMaxRows <> '' && $copyMaxRows > 0) {
					$stepOptions .= '\"COPY_MAX_ROWS\":' . $copyMaxRows . ',';
				}
				if ($copySlowDown <> '' && $copySlowDown > 0) {
					$stepOptions .= '\"COPY_SLOW_DOWN\":' . $copySlowDown . ',';
				}
				if ($compareMaxRows <> '' && $compareMaxRows > 0) {
					$stepOptions .= '\"COMPARE_MAX_ROWS\":' . $compareMaxRows . ',';
				}
				if ($compareMaxDiff <> '' && $compareMaxDiff > 0) {
					$stepOptions .= '\"COMPARE_MAX_DIFF\":' . $compareMaxDiff . ',';
				}
				if ($compareTruncateDiff <> '') {
					$stepOptions .= '\"COMPARE_TRUNCATE_DIFF\": true' . ',';
				}
				if ($discoverMaxRows <> '' && $discoverMaxRows > 0) {
					$stepOptions .= '\"DISCOVER_MAX_ROWS\":' . $discoverMaxRows . ',';
				}
				if ($stepOptions <> '') {
					// Strip the last ',' and enclose with {} to get a proper JSON object.
					$stepOptions = preg_replace('/(.*).$/', '{$1}', $stepOptions);
				}

// And spawn the scheduler.
				$bashCmd = $conf['schedulerPath'] . ' --host ' . $conf['data2pg_host'] .' --port ' . $conf['data2pg_port'] .
						' --action restart' . ' --run ' . $newRunId . ' --target ' . $run['run_database'] .
						' --batch ' . $run['run_batch_name'] . ' --sessions ' . $maxSession . ' --asc_sessions ' . $ascSession;
				if ($stepOptions <> '') {
					$bashCmd .= ' --step_options "' . $stepOptions . '"';
				}
				if ($comment <> '') {
					// Add a \ before ", if any
					$bashCmd .= ' --comment "' . str_replace('"', '\"', $comment) . '"';
				}
				if ($conf['development_mode']) {
					$bashCmd .= ' --debug';
				}
				// Add a \ before any \ and " in the command to spawn
				$cmd = 'nohup bash -c "' . str_replace(array('\\', '"'), array('\\\\', '\\"'), $bashCmd) . '" 1>' . $logFile . ' 2>&1 &';
				$outputRun = shellExec($shellConn, $cmd);

				shellClose($shellConn);

// Register the run start attempt.
// TODO: the scheduler address and account need to be register
				sql_insertExternalRunStart($newRunId, NULL, NULL, $bashCmd, $logFile);

// Wait until the run starts and display the new run details.
				$isRunVisible = sql_waitForRunStart($newRunId);
				if ($isRunVisible) {
					runDetails($newRunId);
				} else {
// The run is not visible. This is probably abnormal. So stay on the old run and display the log file name so that the user can get the run results.
					runDetails($runId, "A scheduler run has been spawned (id = $newRunId). But the run is not yet visible. Its output file is located at $logFile.");
				}
			}
		}
	}
}
?>
