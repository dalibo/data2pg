<?php

	/**
	 * Central Data2Pg web client configuration.
	 */

	// Current Data2Pg version.
	$conf['version']				= '0.1';			// Do not change this value

	// Connection parameters to reach the data2pg database.
	// Both database and role names are fixed (and equal 'data2pg').
	$conf['data2pg_host']			= "database";     // The ip address must be accessible by the data2pg web client and all data2pg.pl schedulers
	$conf['data2pg_port']			= 5432;
	$conf['data2pg_pwd']			= "gp2atad";

	// Is the web application allowed to perform actions or do the users only look at the data2pg database content ?
	$conf['read_only']		    	= 0;				// If set to 1, no action is allowed

	// Is the web application allowed to execute shell commands ? 0 = no / 1 = yes
	$conf['exec_command']			= 1;				// The parameter is only meaninful when $conf['read_only'] equals 0

	// Default paralellism level when submitting a new batch run
	$conf['max_sessions_default']	= 3;				// The suggested maximum sessions number
	$conf['asc_sessions_default']	= 1;				// The suggested number of sessions executing steps in cost ascending order

	// The Data2Pg scheduler location.
	$conf['scheduler_location'] 	= 'local';
										// 'local' means that the scheduler and the web client are on the same server
										// 'central' means that the scheduler is located on a single server but different as the server hosting the web client
										// 'distributed' means that the scheduler is located on different server depending on the target databases

	// The OS account to use to reach the Data2Pg scheduler when the 'scheduler_location' is set to 'central'.
	$conf['scheduler_ip']			= 'database';
	$conf['scheduler_port'] 		= 22;
	$conf['scheduler_user'] 		= 'postgres';
	$conf['scheduler_pwd']			= 'postgres';

	// The command path to spawn the Data2Pg scheduler when the 'scheduler_location' is set to 'local' or 'central'.
	$conf['schedulerPath']			= 'export PERL5LIB=/home/beaudoin/perl5/lib/perl5/x86_64-linux-gnu-thread-multi && /home/beaudoin/Documents/"R&D"/data2pg/data2pg.pl';

	// The directory to store the scheduler's log when the 'scheduler_location' is set to 'local' or 'central'.
	$conf['scheduler_log_dir']		= '/tmp/data2pg';

?>
