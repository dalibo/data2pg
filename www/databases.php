<?php
// databases.php
// This file belongs to the Data2Pg web client

	session_start();
	$pageId="databases.php";

	require_once('inc/constants.php');
	require_once('conf/config.inc.php');
	require_once('inc/htmlcommon.php');
	require_once('inc/dbaccess.php');
	require_once('inc/shell.php');

	PageHeader();

	$a = @$_GET["a"];
	$tdbId = @$_GET["tdbId"];

	$conn = sql_connect();

	switch ($a) {
	  case "display":
		displayDb();
		break;
	  case "newDb":
		newDb();
		break;
	  case "confirmNewDb":
		confirmNewDb();
		break;
	  case "doNewDb":
		doNewDb();
		break;
	  case "newRun":
		newRun();
		break;
	  case "doNewRun":
		doNewRun();
		break;
	  case "checkDb":
		checkDb();
		break;
	  case "editDb":
		editDb();
		break;
	  case "doEditDb":
		doEditDb();
		break;
	  case "lockDb":
		lockDb();
		break;
	  case "unlockDb":
		unlockDb();
		break;
	  case "confirmDeleteDb":
		confirmDeleteDb();
		break;
	  case "doDeleteDb":
		doDeleteDb();
		break;
	  default:
		echo "!!! The action $a is unknown !!!;";
		break;
	}
	sql_close($conn);

	PageFooter();

// The displayDb function display the list of target databases.
function displayDb($msg = '') {
	global $conf;

// Display the message, if passed as parameter
	if ($msg != '') {
		$class = (substr($msg, 0, 5) == 'Error') ? "errMsg" : "msg";
		echo "\t\t\t<p class=\"$class\">$msg</p>\n";
	}

// Display the page title
	$rightTitle = "\t\t<a href=\"databases.php?a=newDb\" class=\"button mainButton\">New</a>\n";
	mainTitle('', "Target databases", $rightTitle);

// Read the database table.
	$res = sql_getDatabases();
	$nbRows = pg_num_rows($res);

	if ($nbRows == 0) {
// No database to display.
		echo "<p align=center>No database to display.</p>\n";
	} else {

// Display the databases sheet.
// Header
		echo "<table class='tbl'>\n";
		echo "\t<tr>\n";
		echo "\t\t<th>Database id.</th>\n";
		if ($conf['read_only'] == 0) {
			echo "\t\t<th colspan=5>Actions</th>\n";
		}
		echo "\t\t<th>Host</th>\n";
		echo "\t\t<th>Port</th>\n";
		echo "\t\t<th>Db Name</th>\n";
		echo "\t\t<th>#runs</th>\n";
		echo "\t\t<th>Locked ?</th>\n";
		echo "\t\t<th>Description</th>\n";
		echo "\t</tr>\n";

// Display each row.
		for ($i = 0; $i < $nbRows; $i++) {
			$row = pg_fetch_assoc($res);
			$style = "even"; if ($i % 2 != 0) {$style = "odd";}

			echo "\t<tr class='${style}'>\n";
			echo "\t\t<td>" . htmlspecialchars($row['tdb_id']) . "</td>\n";
			if ($conf['read_only'] == 0) {
				if ($conf['exec_command'] == 1 && $row['tdb_locked'] <> 't') {
					echo "\t\t<td class=\"action\"><a href=databases.php?a=newRun&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/runAction.svg\" title=\"New run\"/></a></td>\n";
				} else {
					echo "\t\t<td></td>\n";
				}
				if ($conf['exec_command'] == 1 && $row['tdb_locked'] <> 't') {
					echo "\t\t<td class=\"action\"><a href=databases.php?a=checkDb&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/checkAction.svg\" title=\"Check\"/></a></td>\n";
				} else {
					echo "\t\t<td></td>\n";
				}
				if ($row['tdb_locked'] <> 't') {
					echo "\t\t<td class=\"action\"><a href=databases.php?a=lockDb&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/unlocked.svg\" title=\"Lock\"/></a></td>\n";
				} else {
					echo "\t\t<td class=\"action\"><a href=databases.php?a=unlockDb&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/locked.svg\" title=\"Unlock\"/></a></td>\n";
				}
				echo "\t\t<td class=\"action\"><a href=databases.php?a=editDb&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/editAction.svg\" title=\"Edit\"/></a></td>\n";
				if ($row['nb_run'] == 0) {
					echo "\t\t<td class=\"action\"><a href=databases.php?a=confirmDeleteDb&tdbId=${row['tdb_id']}>" .
							"<img src=\"img/deleteAction.svg\" title=\"Delete\"/></a></td>\n";
				} else {
					echo "\t\t<td></td>\n";
				}
			}
			echo "\t\t<td>" . htmlspecialchars($row['tdb_host']) . "</td>\n";
			echo "\t\t<td>" . $row['tdb_port'] . "</td>\n";
			echo "\t\t<td>" . htmlspecialchars($row['tdb_dbname']) . "</td>\n";
			echo "\t\t<td>" . $row['nb_run'] . "</td>\n";
			if ($row['tdb_locked'] == 't') {
				echo "\t\t<td>Yes</td>\n";
			} else {
				echo "\t\t<td>No</td>\n";
			}
			echo "\t\t<td class='alignLeft'>" . htmlspecialchars($row['tdb_description']) . "</td>\n";
			echo "\t</tr>\n";
		}
		echo "</table>\n";
	}
}
// The newDb() function displays a form to define the new database to register into the target_database table
function newDb() {

// Display the form.
	mainTitle('', "New target database", '');
	displayDbForm('confirmNewDb', '', 'localhost', 5432, '', '');
}

// The displayDbForm() function displays the form used to either create a new database or edit an existing database.
function displayDbForm($action, $tdbId, $tdbHost, $tdbPort, $tdbDbname, $tdbDescription){

	echo "<div id=\"dbForm\">\n";
	echo "<form name=\"newDb\" action=\"databases.php\" method=\"get\">\n";
	echo "\t<div class=\"form-container\">\n";
	echo "\t\t<input type=\"hidden\" name=\"a\" value=\"$action\">\n";

	echo "\t\t<div class=\"form-label\">Database identifier (must be unique)</div>";
	($tdbId == '') ? $attr = 'required' : $attr = 'readonly';
	echo "\t\t<div class=\"form-input\"><input name=\"tdbId\" $attr value=\"" . htmlspecialchars($tdbId) . "\"></div>";

	echo "\t\t<div class=\"form-label\">IP address or host name</div>";
	echo "\t\t<div class=\"form-input\"><input name=\"tdbHost\" size=20 required value=\"" . htmlspecialchars($tdbHost) . "\"></div>\n";

	echo "\t\t<div class=\"form-label\">IP port</div>";
	echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"tdbPort\" size=5 min=0 max=65535 required value=$tdbPort></div>\n";

	echo "\t\t<div class=\"form-label\">Database name</div>";
	echo "\t\t<div class=\"form-input\"><input name=\"tdbDbname\" required value=\"" . htmlspecialchars($tdbDbname) . "\"></div>";

	echo "\t\t<div class=\"form-label\">Description</div>";
	echo "\t\t<div class=\"form-input\"><input name=\"tdbDescription\" size=60 value=\"" . htmlspecialchars($tdbDescription) . "\"></div>\n";

	echo "\t</div>\n";
	echo "<p>";
	echo "\t<input type=\"submit\" name=\"OK\" value=\"OK\">\n";
	echo "\t<input type=\"reset\" value=\"Reset\">\n";
	echo "\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='databases.php?a=display';\">\n";
	echo "</p></form>\n";
	echo "</div>\n";
}

// The confirmNewDb() function asks for a confirmation to register a new target database.
function confirmNewDb() {
	global $tdbId;

	$tdbHost = @$_GET["tdbHost"];
	$tdbPort = @$_GET["tdbPort"];
	$tdbDbname = @$_GET["tdbDbname"];
	$tdbDescription = @$_GET["tdbDescription"];

// Check that the database to create doesn't already exist, either with its id or its combination of host, port and dbname.
	$res = sql_existDatabaseId($tdbId);
	if (pg_fetch_result($res, 0, 0) == 't') {
		displayDb("Error: the database $tdbId already exists");
	} else {
		$res = sql_existDatabase($tdbHost, $tdbPort, $tdbDbname);
		if (pg_fetch_result($res, 0, 0) == 't') {
			displayDb("Error: a database already exists for $tdbDbname on $tdbHost:$tdbPort");
		} else {

// The database is really new, so display the confirmation form.
			mainTitle('', "Please confirm the target database creation", '');
			echo "<div id=\"newDb\">\n";
			echo "\t<p>Target database <b>$tdbId</b></p>\n";
			echo "\t<p>Mapping the database <b>$tdbDbname</b> on <b>$tdbHost:$tdbPort</b></p>\n";
			if ($tdbDescription != '') echo "\t<p>Description: $tdbDescription</p>\n";
			echo "\t<p><form name=\"confirmNewDb\" action='databases.php' method='get'>\n";
			echo "\t\t<input type='hidden' name='a' value='doNewDb'>\n";
			echo "\t\t<input type='hidden' name='tdbId' value='$tdbId'>\n";
			echo "\t\t<input type='hidden' name='tdbHost' value='$tdbHost'>\n";
			echo "\t\t<input type='hidden' name='tdbPort' value='$tdbPort'>\n";
			echo "\t\t<input type='hidden' name='tdbDbname' value='$tdbDbname'>\n";
			echo "\t\t<input type='hidden' name='tdbDescription' value='$tdbDescription'>\n";
			echo "\t\t<input type='submit' value='OK'>\n";
			echo "\t\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='databases.php?a=display';\">\n";
			echo "\t</form></p>\n";
			echo "</div>\n";
		}
	}
}

// The doNewDb() function effectively register the new target database into the target_database table.
function doNewDb() {
	global $tdbId;

	$tdbHost = @$_GET["tdbHost"];
	$tdbPort = @$_GET["tdbPort"];
	$tdbDbname = @$_GET["tdbDbname"];
	$tdbDescription = @$_GET["tdbDescription"];

// Perform the insertion
	$res = sql_insertDatabase($tdbId, $tdbHost, $tdbPort, $tdbDbname, $tdbDescription);

	if (pg_affected_rows($res) <> 1) {
		$msg = "Error: internal error while recording the new database.";
	} else {
		$msg = "The new database has been registered.";
	}

// Display the modified databases list.
	displayDb($msg);
}

// The newRun() function displays a form to define the run to spawn.
function newRun() {
	global $conf, $tdbId;

// Get the target database characteristics.
	$res = sql_getDatabase($tdbId);
	$msg = '';
	if (pg_num_rows($res) <> 1) {
		$msg = "Error: internal error while getting the $tdbId database characteristics.";
	} else {
		$db = pg_fetch_assoc($res);

// Get the available batch ids using a "data2pg.pl --action check" command.
		$shellConn = shellOpen();
		$cmd = $conf['schedulerPath'] .
			" --host ${conf['data2pg_host']} --port ${conf['data2pg_port']} --action check --target ${db['tdb_id']} 2>&1";
		$outputCheck = shellExec($shellConn, $cmd);

// Check the data2pg.pl report.
		if (preg_match('/Error: Error while logging on the target database \((.*?)\)/m', $outputCheck, $matches) == 1) {
			$msg = "Error: The target database '$tdbId' cannot be reached (${matches[1]}).";
		} elseif (preg_match('/Configured batches:/m', $outputCheck) != 1) {
			$msg = "Error: An error occurred while checking the access to the target database '$tdbId'.<br><pre>$outputCheck</pre>";
		} else {
			preg_match_all('/ (\S+?)\s*\| (\S+?)\s*\| (\S+?)\s*\| Yes/m', $outputCheck, $matches);
			$migrations = $matches[1];
			$batchNames = $matches[2];
			$batchTypes = $matches[3];
			$nbBatch = count($matches[2]);
		}
		shellClose($shellConn);
	}

	if ($msg <> '') {
// An error occurred, so return to the databases list.
		displayDb($msg);
	} else {
// The available batches list has been built from the target database, so display the form.
		mainTitle('', "New run for the database '" . htmlspecialchars($tdbId) . "'", '');
		echo "<div id=\"newRun\">\n";
		echo "\t<form name=\"newRun\" action=\"databases.php\" method=\"get\">\n";
		echo "<div class=\"form-container\">\n";
		echo "\t\t<input type=\"hidden\" name=\"a\" value=\"doNewRun\">\n";
		echo "\t\t<input type=\"hidden\" name=\"tdbId\" value=\"$tdbId\">\n";

		echo "\t\t<div class=\"form-label\">Batch</div>";
		echo "\t\t<div class=\"form-input\"><select name=\"batch\">";
		for ($i = 0; $i < $nbBatch; $i++) {
			echo "<option value=\"${batchNames[$i]}\">${batchNames[$i]} (type ${batchTypes[$i]} in '${migrations[$i]}')</option>";
		}
		echo "</select></div>";

		echo "\t\t<div class=\"form-label\">Max sessions</div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"maxSession\" size=3 min=0 max=999 value=${conf['max_sessions_default']}></div>\n";

		echo "\t\t<div class=\"form-label\">Sessions in cost ascending order</div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"ascSession\" size=3 min=0 max=999 value=${conf['asc_sessions_default']}></div>\n";

		echo "\t\t<div class=\"form-label\">Comment</div>";
		echo "\t\t<div class=\"form-input\"><input name=\"comment\" size=60></div>\n";

		echo "\t\t<div class=\"form-label\">Reference run for steps cost estimates</div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"refRunId\" size=5></div>\n";

		echo "\t</div>\n";
		echo "\t<p>Step options</p>";

		echo "<div class=\"form-container\">\n";

		echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COPY_MAX_ROWS</span></div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"copyMaxRows\" size=6 min=1></div>\n";

		if ($conf['development_mode']) {
			echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COPY_SLOW_DOWN (µs/row)</span></div>";
			echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"copySlowDown\" size=6 min=0></div>\n";
		}

		echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_TRUNCATE_DIFF</span></div>";
		echo "\t\t<div class=\"form-input\"><input type=\"checkbox\" name=\"compareTruncateDiff\"></div>\n";

		echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_MAX_DIFF</span></div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"compareMaxDiff\" size=6 min=1></div>\n";

		echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">COMPARE_MAX_ROWS</span></div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"compareMaxRows\" size=6 min=1></div>\n";

		echo "\t\t<div class=\"form-label\"><span class=\"stepOption\">DISCOVER_MAX_ROWS</span></div>";
		echo "\t\t<div class=\"form-input\"><input type=\"number\" name=\"discoverMaxRows\" size=6 min=1></div>\n";

		echo "\t</div>\n";

		echo "\t<p>\n";
		echo "\t\t<input type=\"submit\" name=\"OK\" value='Start and Monitor'>\n";
		echo "\t\t<input type=\"reset\" value=\"Reset\">\n";
		echo "\t\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='databases.php?a=display'\">\n";
		echo "\t</p></form>\n";
		echo "</div>\n";
	}
}

// The doNewRun() function effectively spawns a new Data2Pg run, using the input parameters.
function doNewRun() {
	global $conf, $tdbId;

	$batch = @$_GET["batch"];
	$maxSession = @$_GET["maxSession"];
	$ascSession = @$_GET["ascSession"];
	$comment = @$_GET["comment"];
	$refRunId = @$_GET["refRunId"];
	$copyMaxRows = @$_GET["copyMaxRows"];
	$copySlowDown = @$_GET["copySlowDown"];
	$compareMaxRows = @$_GET["compareMaxRows"];
	$compareMaxDiff = @$_GET["compareMaxDiff"];
	$compareTruncateDiff = @$_GET["compareTruncateDiff"];
	$discoverMaxRows = @$_GET["discoverMaxRows"];

// Check the state of a potential previous run for this same database and batch.
	$res = sql_getPreviousRun($tdbId, $batch);
	if (pg_num_rows($res) > 0) {
		$prevRun = pg_fetch_assoc($res);
		if ($prevRun['run_status'] <> 'Completed' && $prevRun['run_status'] <> 'Suspended' && $prevRun['run_status'] <> 'Aborted') {
			displayDb("Error: A previous run (<a href=\"run.php?a=runDetails&runId=${prevRun['run_id']}\">#${prevRun['run_id']}</a>) for the database '$tdbId' and batch '$batch' is in '${prevRun['run_status']}' state.");
			exit;
		}
	}

// Check the reference run if, if supplied.
	if ($refRunId <> '') {
		$res = sql_getReferenceRun($tdbId, $batch, $refRunId);
		if (pg_num_rows($res) > 0) {
			$refRun = pg_fetch_assoc($res);
			if ($refRun['run_status'] <> 'Completed') {
				displayDb("Error: The reference run (<a href=\"run.php?a=runDetails&runId=$refRunId\">#$refRunId</a>) is in '${refRun['run_status']}' state.");
				exit;
			}
		} else {
			displayDb("Error: No reference run #$refRunId has been found for the database '$tdbId' and batch '$batch'.");
			exit;
		}
	}

// Define the log file name.
	$logFile = $conf['scheduler_log_dir'] . '/' . date('Ymd_His');

// Create the log file on the scheduler server.
// The directory tree is previously created, if needed.
	$shellConn = shellOpen();
	$cmd = "mkdir -p ${conf['scheduler_log_dir']}; touch $logFile; chmod 666 $logFile 2>&1";
	$outputTouch = shellExec($shellConn, $cmd);

	if ($outputTouch != '') {
		displayDb("Error: trying to create the scheduler log file failed.<pre style=\"text-align:left;\">$outputTouch</pre>");
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
				   ' --action run ' . ' --run ' . $newRunId . ' --target ' . $tdbId .
				   ' --batch ' . $batch . ' --sessions ' . $maxSession . ' --asc_sessions ' . $ascSession;
		if ($stepOptions <> '') {
			$bashCmd .= ' --step_options "' . $stepOptions . '"';
		}
		if ($comment <> '') {
			// Add a \ before ", if any
			$bashCmd .= ' --comment "' . str_replace('"', '\"', $comment) . '"';
		}
		if ($refRunId <> '') {
			$bashCmd .= ' --ref_run ' . $refRunId;
		}
		if ($conf['development_mode']) {
			$bashCmd .= ' --verbose';
		}
		// Add a \ before any \ and " in the command to spawn
		$cmd = 'nohup bash -c "' . str_replace(array('\\', '"'), array('\\\\', '\\"'), $bashCmd) . '" 1>' . $logFile . ' 2>&1 &';
		$outputRun = shellExec($shellConn, $cmd);

		shellClose($shellConn);

// Register the run start attempt.
// TODO: the scheduler address and account need to be register
		sql_insertExternalRunStart($newRunId, NULL, NULL, $bashCmd, $logFile);

// Wait until the run starts and display run details.
		$isRunVisible = sql_waitForRunStart($newRunId);
		if ($isRunVisible) {
			echo "<script language='Javascript'>window.location.href=\"run.php?a=runDetails&runId=$newRunId\";</script>\n";
		} else {
// The run is not visible. This is probably abnormal. So display the log file name so that the user can get the run results.
			displayDb("A scheduler run has been spawned (id = $newRunId). But the run is not yet visible. Its output file is located at $logFile.");
		}
	}
}

// The checkDb() function executes the scheduler with the function check to test the connection to the target database and get the configured batches.
function checkDb() {
	global $tdbId, $conf;

// Get the target database characteristics.
	$res = sql_getDatabase($tdbId);
	if (pg_num_rows($res) <> 1) {
		$msg = "Error: internal error while getting the $tdbId database characteristics.";
	} else {
		$db = pg_fetch_assoc($res);

// Test the access using a "data2pg.pl --action check" command.
		$shellConn = shellOpen();
		$cmd = $conf['schedulerPath'] .
			" --host ${conf['data2pg_host']} --port ${conf['data2pg_port']} --action check --target ${db['tdb_id']} 2>&1";
		$outputCheck = shellExec($shellConn, $cmd);
// Check the data2pg.pl report.
		if (preg_match('/Error: Error while logging on the target database \((.*?)\)/m', $outputCheck, $matches) == 1) {
			$msg = "Error: The target database '$tdbId' cannot be reached (${matches[1]}).";
		} elseif (preg_match('/Configured batches:/m', $outputCheck) != 1) {
			$msg = "Error: An error occurred while checking the access to the target database '$tdbId'.<br><pre>$outputCheck</pre>";
		} else {
			preg_match_all('/( .+? \| .+? \| .+? \| Yes)/m', $outputCheck, $matches);
			$msg = "The target database '$tdbId' is accessible and contains " . count($matches[1]) . " configured batches.";
		}
		shellClose($shellConn);
	}

	displayDb($msg);
}

// The lockDb function registers the target database as locked.
function lockDb() {
	global $tdbId;

	sql_updateLockDatabase($tdbId, 'TRUE');
	displayDb();
}

// The unlockDb function registers the target database as unlocked.
function unlockDb() {
	global $tdbId;

	sql_updateLockDatabase($tdbId, 'FALSE');
	displayDb();
}

// The editDb() function displays a form to edit the characteristics of a target database.
function editDb() {
	global $tdbId;

// Get the target database characteristics.
	$res = sql_getDatabase($tdbId);
	if (pg_num_rows($res) <> 1) {
		displayDb("Error: internal error while getting the $tdbId database characteristics.");
	} else {
		$db = pg_fetch_assoc($res);

// Display the form.
		mainTitle('', "Edit a target database", '');
		displayDbForm('doEditDb', $tdbId, $db['tdb_host'], $db['tdb_port'], $db['tdb_dbname'], $db['tdb_description']);
	}
}

// The doEditDb() function effectively updates the database in the target_database table.
function doEditDb() {
	global $tdbId;

	$tdbHost = @$_GET["tdbHost"];
	$tdbPort = @$_GET["tdbPort"];
	$tdbDbname = @$_GET["tdbDbname"];
	$tdbDescription = @$_GET["tdbDescription"];

// Perform the update.
	$res = sql_updateDatabase($tdbId, $tdbHost, $tdbPort, $tdbDbname, $tdbDescription);

	if (pg_affected_rows($res) <> 1) {
		$msg = "Error: internal error while updating the database.";
	} else {
		$msg = "The database $tdbId has been updated.";
	}

// Display the modified databases list.
	displayDb($msg);
}

// The confirmDeleteDb() function asks the user to confirm the deletion of a database from the target_database table.
// It checks that the provided database name exists.
function confirmDeleteDb() {
	global $tdbId;

// Get data about the target database to process.
	$res = sql_getDatabase($tdbId);

// Check that the database still exists.
	if (pg_num_rows($res) <> 1) {
		displayDb("Error: the database $tdbId doesn't exist anymore.");
	} else {

// Check that no run is associated to the database.
		$db = pg_fetch_assoc($res);
		if ($db['nb_run'] <> 0) {
			displayDb("Error: the database $tdbId cannot be deleted because at least one run is linked to it.");
		} else {

// Ask for the confirmation.

			mainTitle('', "Please confirm the target database deletion", '');
			echo "<div id=\"dbToDelete\">\n";
			echo "\t<p>Target database <b>$tdbId</b></p>\n";
			echo "\t<p>Mapping the database <b>${db['tdb_dbname']}</b> on <b>${db['tdb_host']}:${db['tdb_port']}</b></p>\n";
//			if ($db['nb_run'] > 0) {
//				echo "\t<p>Beware, the database deletion will also delete ${db['nb_run']} recorded runs</p>\n";
//			}
			echo "\t<p><form name=\"confirmDeleteDb\" action='databases.php' method='get'>\n";
			echo "\t\t<input type='hidden' name='a' value='doDeleteDb'>\n";
			echo "\t\t<input type='hidden' name='tdbId' value='$tdbId'>\n";
			echo "\t\t<input type='submit' value='OK'>\n";
			echo "\t<input type=\"button\" value=\"Cancel\" onClick=\"window.location.href='databases.php?a=display';\">\n";
			echo "\t</form></p>\n";
			echo "</div>\n";
		}
	}
}

// The doDeleteDb() function effectively suppress the database from the target_database table.
function doDeleteDb() {
	global $tdbId;

// Perform the deletion.
	$res = sql_deleteDatabase($tdbId);

	if (pg_affected_rows($res) <> 1) {
		$msg = "Error: internal error while deleting the new database.";
	} else {
		$msg = "The database $tdbId has been deleted.";
	}

// Display the modified databases list.
	displayDb($msg);
}

?>
