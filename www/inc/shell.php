<?php
// shell.php
// This file belongs to the Data2Pg web client. It contains functions to execute shell commands.

	require_once('inc/ssh2.php');

// The shellOpen() function initializes a ssh connection, when required.
function shellOpen($ip = '', $port = '', $user = '', $pwd = '') {
	global $conf;

// When the scheduler and the web client are located on the same server, there is no need for a ssh connection.
	if ($conf['scheduler_location'] == 'local') {
// So return nothing.
		return null;
	}

// When the scheduler is located on a central point but not on the same server as the web client, open a ssh connection using the global configuration.
	if ($conf['scheduler_location'] == 'central') {
		return new Ssh2($conf['scheduler_ip'], $conf['scheduler_user'], $conf['scheduler_pwd'], $conf['scheduler_port']);
	}

// When the scheduler is located on a various point, use the provided parameters to open a ssh connection.
	if ($conf['scheduler_location'] == 'distributed') {
		return new Ssh2($ip, $user, $pwd, $port);
	}
}

// The shellExec() function executes a shell command either directly or through an opened ssh connection.
function shellExec($conn, $command, $async = '') {

	if (is_null($conn)) {
// Execute the command directly.
//TODO: how to process asynchronous command exec ?
		$output = shell_exec($command);
	} else {
// Execute the command through the ssh connection.
		$output=$conn->ExecCmd($cmd, $async);
	}
	return $output;
}

// The shellClose() function closes a shell connection.
function shellClose($conn) {

	if (! is_null($conn)) {
// unset the ssh connection handler, if any.
		unset($conn);
	}
}
?>
