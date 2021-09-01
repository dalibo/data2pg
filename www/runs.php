<?php
// runs.php
// This file belongs to the Data2Pg web client

	session_start();
	$pageId="runs.php";

	require_once('inc/constants.php');
	require_once('conf/config.inc.php');
	require_once('inc/htmlcommon.php');
	require_once('inc/dbaccess.php');
	require_once('inc/ssh2.php');

	PageHeader();

	$a = @$_GET["a"];

	$conn = sql_connect();

	switch ($a) {
	  case "displayAllRuns":
		displayRuns('all');
		break;
	  case "displayInProgressRuns":
		displayRuns('inProgress');
		break;
	  default:
		echo "!!! The action $a is unknown !!!;";
		break;
	}
	sql_close($conn);

	if (isset($runId)) $_SESSION["runid"] = $runId;

	PageFooter();

// The displayRuns function display a runs list
function displayRuns($listType, $msg = '') {

// Display the message, if passed as parameter
	if ($msg != '') {
		$class = (substr($msg, 0, 5) == 'Error') ? "errMsg" : "msg";
		echo "\t\t\t<p class=\"$class\">$msg</p>\n";
	}

// Display the page title
	if ($listType == 'all') {
		mainTitle('', "All runs", '');
	} else {
		mainTitle('', "In progress runs", '');
	}

// Read the runs table.
	if ($listType == 'all') {
		$res = sql_getAllRuns();
	} else {
		$res = sql_getInProgressRuns();
	}
	$nbRows = pg_num_rows($res);

	if ($nbRows == 0) {
// No run to display.
		echo "<p align=center>No run to display.</p>\n";
	} else {

// Display the runs sheet.
// Header
		echo "<table class='tbl'>\n";
		echo "\t<tr>\n";
		echo "\t\t<th>Run id.</th>\n";
		echo "\t\t<th>Target Db</th>\n";
		echo "\t\t<th>Batch name</th>\n";
		echo "\t\t<th>Batch type</th>\n";
		if ($listType == 'all') {
			echo "\t\t<th>Status</th>\n";
		}
		echo "\t\t<th>Start</th>\n";
		if ($listType == 'all') {
			echo "\t\t<th>End</th>\n";
		}
		echo "\t\t<th>Elapse</th>\n";
		echo "\t\t<th>Max sessions</th>\n";
		if ($listType != 'all') {
			echo "\t\t<th>Completed Steps</th>\n";
		}
		echo "\t\t<th>Comment</th>\n";
		if ($listType == 'all') {
			echo "\t\t<th>Error message</th>\n";
		}
		echo "\t</tr>\n";

// Display each row.
		for ($i = 0; $i < $nbRows; $i++) {
			$row = pg_fetch_assoc($res);
			$style = "even"; if ($i % 2 != 0) {$style = "odd";}

			echo "\t<tr class='${style}'>\n";
			echo "\t\t<td><a href=\"run.php?a=runDetails&runId=${row['run_id']}\">";
			echo $row['run_id'] . "</a></td>\n";
			echo "\t\t<td>" . htmlspecialchars($row['run_database']) . "</td>\n";
			echo "\t\t<td>" . htmlspecialchars($row['run_batch_name']) . "</td>\n";
			echo "\t\t<td>" . htmlspecialchars($row['run_batch_type']) . "</td>\n";
			if ($listType == 'all') {
				echo "\t\t<td>" . htmlspecialchars($row['run_status']);
				if ($row['run_status'] == 'Restarted') {
					echo " (" . $row['run_restart_id'] . ")";
				}
			}
			echo "</td>\n";
			echo "\t\t<td>" . htmlspecialchars($row['run_start_ts']) . "</td>\n";
			if ($listType == 'all') {
				echo "\t\t<td>" . htmlspecialchars($row['run_end_ts']) . "</td>\n";
			}
			echo "\t\t<td>" . htmlspecialchars($row['elapse']) . "</td>\n";
			echo "\t\t<td>" . $row['run_max_sessions'] . "</td>\n";
			if ($listType != 'all') {
				echo "\t\t<td>${row['completed_steps']} / ${row['total_steps']}</td>\n";
			}
			echo "\t\t<td class='alignLeft'>" . htmlspecialchars($row['run_comment']) . "</td>\n";
			if ($listType == 'all') {
				echo "\t\t<td class='alignLeft'>" . htmlspecialchars($row['run_error_msg']) . "</td>\n";
			}
			echo "\t</tr>\n";
		}
		echo "</table>\n";
	}
}
?>
