#!/usr/bin/perl -w
# data2pg.pl
# This file belongs to data2pg, the framework that helps migrating data to PostgreSQL databases from various sources.
# It is the scheduler that drives the data migration process.

use strict;
use warnings;
use English;
use Data::Dumper;
use DBD::Pg qw(:async :pg_types);
use Getopt::Long;
use Time::HiRes 'sleep';

# Constants.
my $toolVersion = '0.1';               # Data2pg version
my $checkCompletedOpDelay = 0.5;       # Maximum delay in seconds between 2 checks for completed operations
my $sleepMinDuration = 0.01;           # Minimum sleep duration in the main loop (in second)
my $sleepMaxDuration = 0.5;            # Maximum sleep duration in the main loop (in second)
my $sleepDurationIncr = 0.01;          # Increment of the sleep duration in the main loop (in second)

my $maxSessionsRefreshDelay = 30;      # Delay in seconds between 2 accesses of the run table to detect changes in the max sessions value
my $cnxRole = 'data2pg';               # The role name used for all connections to postgres databases

# Global variables representing the arguments of the command line and the final value of the parameters.
my $options;                           # Options from the command line
my $host;                              # IP host of the data2pg database
my $port;                              # IP port of the data2pg database
my $confFile;                          # Configuration file
my $action;                            # Action to perform: 'run' / 'restart' / 'suspend' / 'abort'
my $targetDb;                          # The identifier of the database to migrate, as defined in the target_database table of the data2pg database
my $batchName;                         # The identifier of the batch
my $maxSessions;                       # Maximum number of sessions to open on the target database
my $ascSessions;                       # Number of sessions for which the steps will be assigned in estimated cost ascending order
my $comment;                           # Comment associated to the run and registered in the run table
my $help;
my $debug;

# Global variables representing the parameters read from the configuration file.
our $cfTargetDb;                       # The identifier of the database to migrate, as defined in the target_database table of the data2pg database
our $cfBatchName;                      # The identifier of the batch
our $cfMaxSessions;                    # Maximum number of sessions to open on the target database
our $cfAscSessions;                    # Number of sessions for which the steps will be assigned in estimated cost ascending order
our $cfComment;                        # Comment associated to the run and registered in the run table

# Global variables to manage the sessions and the statements to the databases.
my $pgDsn;                             # DSN to reach the target database
my $d2pDbh;                            # handle for the connection on the data2pg database
my $d2pSth;                            # handle for the statements to submit on the data2pg database connection

# Counters.
my $nbSteps = 0;                       # Number of steps to process by the run
my $nbStepsReady;                      # Number of steps ready to be processed
my $nbBusySessions;                    # Number of sessions currently running a step
my $nbStepsOK;                         # Number of steps correctly processed by the run
my $previousNbStepsOK;                 # Number of steps correctly processed by the previous restarted run

# Global variables about the previous run for the same target database and batch name.
my $previousRunExists = 0;             # Boolean indicating whether a run with the same characteristics has already been executed
my $previousRunId;                     # The id of the previous run
my $previousRunStartTime;              # The start time of the previous run
my $previousRunState;                  # The state of the previous run, as reported in the data2pg database
my $previousRunPerlPid;                # The data2pg.pl process id of the previous run
my $previousRunBatchType;              # The batch type of the previous run
my $previousRunPerlIsExecuting;        # A boolean indicating whether the previous perl pid is really in execution 
                                       #   (when the state is not 'Completed' or 'Aborted' or 'Suspended')

# Other global variables.
my $runId;                             # Run identifier
my $pgMaxCnx;                          # The maximum nomber of connections configured on the target database
my $batchType;                         # The type of the batch as defined on the target database
my $nextMaxSessionsRefreshTime;        # The next time the maxSessions parameter stored into the run table must be read

# Booleans.
my $actionRun;
my $actionRestart;
my $actionSuspend;
my $actionAbort;

# Hash structures to manage the steps processing.
my @sessions;
# It contains a structure like :
#   - $sessions[i]->{state}     => session state (0 = not connected, 1 = connected and idle, 2 = in use, 3 = to be closed)
#   - $sessions[i]->{dbh}       => handle of the session
#   - $sessions[i]->{sth}       => handle for the statements to submit through the session
#   - $sessions[i]->{step}      => current step name

# ---------------------------------------------------------------------------------------------
# Online Help.
sub usage
{
    print "Data2Pg is a migration framework to load PostgreSQL databases (version $toolVersion)\n";
    print "Usage: $0 [--help] | [<options to log on the data2pg>] --action <action> [--conf <configuration_file>] [<other options>]]\n\n";
    print "  --host         : IP host of the data2pg database (default = PGHOST env. var.)\n";
    print "  --port         : IP port of the data2pg database (default = PGPORT env. var.)\n";
    print "  --action       : 'run' | 'restart' | 'suspend' | 'abort' (no default)\n";
    print "  --conf         : configuration file (default = no configuration file)\n";
    print "  --target       : target database identifier (mandatory for the 'run' action) (*)\n";
    print "  --batch        : batch name for the run (mandatory for the 'run' action) (*)\n";
    print "  --sessions     : number of parallel sessions (default = 1) (*)\n";
    print "  --asc_sessions : number of sessions for which steps will be assigned in estimated cost ascending order (default = 0) (*)\n";
    print "  --comment      : comment describing the run (between single or double quotes to include spaces) (*)\n";
    print "(*) overides the configuration file content, if any\n";
}

# ---------------------------------------------------------------------------------------------
# Technical function to log messages into the stdout file in debug mode.
sub printDebug
{
    my ($msg) = @_;

    if ($debug) {
        my ($sec,$min,$hour) = localtime();
        printf("Debug (%02d:%02d:%02d): %s\n", $hour, $min, $sec, $msg);
    }
}

# ---------------------------------------------------------------------------------------------
# Technical function that aborts the run. If created, the run status in the database is set to "aborted".
sub abort
{
    my ($msg) = @_;
    my $sql;             # SQL statement
    my $ret;             # SQL result
    my $quotedMsg;       # Error message properly quoted to be included into a SQL statement

# If a run record has been created, change its state and record the end timestamp and the error message.
    if (defined $runId) {
        # Rollback any uncommited changes, if any.
        $d2pDbh->rollback()
            or die "Error while rollbacking the current transaction at abort time ($DBI::errstr)\n";

        # Update the in-progress run,
        $quotedMsg = $d2pDbh->quote($msg, { pg_type => PG_VARCHAR });
        $sql = qq(
          UPDATE data2pg.run
              SET run_status = 'Aborted', run_end_ts = clock_timestamp(), run_error_msg = $quotedMsg
              WHERE run_id = $runId
        );
        $ret = $d2pDbh->do($sql)
            or die "Error while updating the current run status at abort time ($DBI::errstr)\n";
        # ... and commit this last change.
        $d2pDbh->commit()
            or die "Error while commiting the run status update at abort time ($DBI::errstr)\n";

#### TODO: abort shell script in execution, if any
    }

# data2pg abort with an error message.
    die "Error: $msg\n";
}

# ---------------------------------------------------------------------------------------------
# Parse the comand line and get the options.
sub parseCommandLine
{
    $help = 0;
    $debug = 0;

# Parse.
    $options = GetOptions(
        "help"           => \$help,
        "host=s"         => \$host,
        "port=s"         => \$port,
        "conf=s"         => \$confFile,
        "action=s"       => \$action,
        "target=s"       => \$targetDb,
        "batch=s"        => \$batchName,
        "sessions=s"     => \$maxSessions,
        "asc_sessions=s" => \$ascSessions,
        "comment=s"      => \$comment,
        "debug"          => \$debug,
        );

# Help!
    if ($help) {
        usage();
        exit 1;
    }

    if ($debug) {printDebug("Command line parsed");}
}

# ---------------------------------------------------------------------------------------------
# Read the configuration file and set the parameters.
sub parseConfigurationFile
{

my %parameters = (                     # Link between the config file parameters and the parameter variables
    'TARGET_DATABASE'    => 'cfTargetDb',
    'BATCH_NAME'         => 'cfBatchName',
    'MAX_SESSIONS'       => 'cfMaxSessions',
    'ASC_SESSIONS'       => 'cfAscSessions',
    'COMMENT'            => 'cfComment',
    );

    # Open the configuration file and process each line.
    open CONF, $confFile
        or abort("Error while opening the configuration file ($confFile)");
    while (my $line = <CONF>)
    {
        $line =~ s/#.*//;         # Remove the comment at the end of the line, if any
        $line =~ s/\s+$//;        # Remove the trailing spaces
        next if ($line =~ /^$/);  # Empty line once comments and spaces have been suppressed
        $line =~ /^(.*?)\s*=\s*(.*)$/
            or abort("In the configuration file ($confFile), format error on the line \n$line\n");
        my ($param, $value) = ($1, $2);
        no strict 'refs';          # Temporarily use variable references by name
        unless (defined $parameters{$param}) {
            abort("In the configuration file ($confFile), the parameter $param is unknown.");
        }
        my $param_name = $parameters{$param};
        $$param_name = $value;
        use strict 'refs';
    }
    close CONF;
    if ($debug) {printDebug("Configuration file ($confFile) properly processed");}
}

# ---------------------------------------------------------------------------------------------
# Check parameters read from the configuration file and assign the default values for those not already set.
sub checkParameters
{
    # Check the action.
    if (!defined ($action)) { abort("The --action parameter is not set."); }

    if ($action ne 'run' && $action ne 'restart' && $action ne 'suspend' && $action ne 'abort') {
        abort("The action '$action' is invalid. Possible values are 'run', 'restart', 'suspend' or 'abort'.");
    }

    $actionRun = ($action eq 'run');
    $actionRestart = ($action eq 'restart');
    $actionSuspend = ($action eq 'suspend');
    $actionAbort = ($action eq 'abort');

    # Use the parameter values read from the configuration file when they are not set as command line options.
    $targetDb = $cfTargetDb       if (!defined($targetDb) && defined($cfTargetDb));
    $batchName = $cfBatchName     if (!defined($batchName) && defined($cfBatchName));
    $maxSessions = $cfMaxSessions if (!defined($maxSessions) && defined($cfMaxSessions));
    $comment = $cfComment         if (!defined($comment) && defined($cfComment));

    # Use hard coded default values, if needed.
    $maxSessions = 1         unless (defined ($maxSessions));
    $ascSessions = 0         unless (defined ($ascSessions));
    $comment = ''            unless (defined ($comment));

    # Check mandatory parameters.
    if (!defined ($targetDb)) { abort("Neither the 'TARGET_DATABASE' parameter nor the --target option is set."); }
    if (!defined ($batchName)) { abort("Neither the 'BATCH_NAME' parameter nor the --batch option is set."); }

    # Check numeric values.
    if ($actionRun || $actionRestart) {
        # Check that the MAX_SESSIONS and MASC_SC_SESSIONS parameters are an integer.
        if ($maxSessions !~ /^\d+$/ || $maxSessions == 0) { abort("The 'MAX_SESSIONS' parameter or the --sessions option must be a positive integer."); }
        if ($ascSessions !~ /^\d+$/) { abort("The 'ASC_SESSIONS' parameter or the --asc_sessions option must be an integer."); }

#### TODO: checks directories used to manage shell scripts executions
    }

    if ($debug) {printDebug("Parameters checks OK");}
}

# ---------------------------------------------------------------------------------------------
sub arePidsExecuting {
# This function checks whether process ids from a comma separated list with a given command name are still in execution.
    my ($pids, $cdeName) = @_;
    my @psCommandReturn;         # The result of the ps linux command

    @psCommandReturn = `ps --pid $pids -o args --no-headers |grep '$cdeName'`;
    return 1 if ($#psCommandReturn >= 0);     # at least 1 pid found with this command name
    return 0;
}

# ---------------------------------------------------------------------------------------------
sub logonData2pg {
# The function opens the connection on the data2pg database.
    my $d2pDsn;          # The DSN to reach the data2pg database
    my $sql;             # SQL statement
    my $schemaFound;     # boolean used to check the existence of the data2pg schema on the data2pg database
    my $quotedTargetDb;  # target database quoted to be included into a SQL statement
    my $row;             # returned row

# Set the data2pg database connection DSN.
    $d2pDsn = "dbi:Pg:dbname=data2pg";
    $d2pDsn .= ";host=$host" if defined $host;
    $d2pDsn .= ";port=$port" if defined $port;

# Open the connection on the data2pg database.
    my $p = scalar reverse $cnxRole;
    if ($debug) {printDebug("Trying to connect on the data2pg database");}
    $d2pDbh = DBI->connect($d2pDsn, $cnxRole, $p, {AutoCommit=>0})
          or abort("Error while logging on the data2pg database ($DBI::errstr).");
    $d2pDbh->{RaiseError} = 1;

# Check that the data2pg schema exists.
    $sql = qq(
        SELECT 1 FROM pg_namespace WHERE nspname = 'data2pg'
    );
    ($schemaFound) = $d2pDbh->selectrow_array($sql)
        or abort("The 'data2pg' schema does not exist in the data2pg database.");

# Get the connection parameters for the target database.
    $quotedTargetDb = $d2pDbh->quote($targetDb, { pg_type => PG_VARCHAR });
    $sql = qq(
        SELECT tdb_host, tdb_port, tdb_dbname, tdb_locked
            FROM data2pg.target_database
            WHERE tdb_id = $quotedTargetDb
    );
    $row = $d2pDbh->selectrow_hashref($sql)
        or abort("The target database '$targetDb' from the configuration file has not been found in the target_database table.");
# Check that the migration lock is not set on the database
    if ($row->{'tdb_locked'}) {
        abort("The target database '$targetDb' is locked. No batch can be run for it.");
    }

# Build the DSN of the target database.
    $pgDsn = "dbi:Pg:";
    $pgDsn .= "dbname=" . $row->{'tdb_dbname'} if defined $row->{'tdb_dbname'};
    $pgDsn .= ";host=" . $row->{'tdb_host'} if defined $row->{'tdb_host'};
    $pgDsn .= ";port=" . $row->{'tdb_port'} if defined $row->{'tdb_port'};

    if ($debug) {printDebug("Connection on the data2pg database opened and target database found");}
}

# ---------------------------------------------------------------------------------------------
sub logoffData2pg {
# The function closes the connection on the data2pg database.

    $d2pDbh->disconnect();
    if ($debug) {printDebug("Connection on the data2pg database closed");}
}

# ---------------------------------------------------------------------------------------------
sub getPreviousRunStatus() {
# This function gets the status of the previous run on the same target database and for the same batch.

    my $quotedTargetDb;  # targetDb properly quoted for the SQL
    my $quotedBatchName; # Batch name properly quoted for the SQL
    my $sql;             # SQL statement
    my $row;             # SQL result row
    my $debugMsg;        # Final message displayed in debug mode

# Get the previous run, if any.
	$quotedTargetDb = $d2pDbh->quote($targetDb, { pg_type => PG_VARCHAR });
	$quotedBatchName = $d2pDbh->quote($batchName, { pg_type => PG_VARCHAR });
    $sql = qq(
        SELECT run_id, run_start_ts, run_status, run_perl_pid, run_batch_type
            FROM data2pg.run
            WHERE run_database = $quotedTargetDb AND run_batch_name = $quotedBatchName
            ORDER BY run_id DESC LIMIT 1
    );
    $row = $d2pDbh->selectrow_hashref($sql);

    if (defined($row)) {
        # A previous run has been found.
        $previousRunExists = 1;
        $previousRunId = $row->{run_id};
        $previousRunStartTime = $row->{run_start_ts};
        $previousRunState = $row->{run_status};
        $previousRunPerlPid = $row->{run_perl_pid};
        $previousRunBatchType = $row->{run_batch_type};

        # If the previous run may be still in execution, check it.
        if ($previousRunState ne 'Completed' && $previousRunState ne 'Suspended' && $previousRunState ne 'Aborted') {
            $previousRunPerlIsExecuting = arePidsExecuting($previousRunPerlPid, 'data2pg.pl');
        }
        $debugMsg = "(id = #$previousRunId, started at $previousRunStartTime, state = $previousRunState)";
    } else {
        $debugMsg = "(no previous run found)";
    }

    if ($debug) {printDebug("Status of the previous run retrieved $debugMsg");}
}

# ---------------------------------------------------------------------------------------------
sub getBackendPids {
# Build a comma separated list of the PG backends pids for a given run.

    my ($runId) = @_;
    my $sql;             # SQL statement
    my $row;             # SQL result row

    # Build the pid list.
    $sql = qq(
        SELECT string_agg(ses_backend_pid::TEXT, ',') AS pid_list
            FROM data2pg.session
            WHERE ses_run_id = $runId
              AND ses_backend_pid IS NOT NULL
    );
    $row = $d2pDbh->selectrow_hashref($sql);
    if (defined($row->{pid_list})) {
        return $row->{pid_list};
    } else {
        abort("getBackendPids: internal error while looking for backend pids for the run $runId.");
    }
}

# ---------------------------------------------------------------------------------------------
sub initRun {
# Intialize the run.
# The function opens the connection on the data2pg database and create the 'run'.
    my $sql;             # SQL statement
    my $sth;             # Statement handle
    my $ret;             # SQL result
    my $row;             # SQL result row
    my $quotedTargetDb;  # targetDb properly quoted for the SQL
    my $quotedBatchName; # Batch name properly quoted for the SQL
    my $batchType;       # Batch type returned by the get_batch_ids() function call
    my $migrationName;   # Migration name returned by the get_batch_ids() function call
    my $isMigCompleted;  # Flag representing the migration configuration state as returned by the get_batch_ids() function call
    my $quotedComment;   # Comment properly quoted for the SQL
    my $quotedBatchType; # Batch type properly quoted for the SQL
    my $errorMsg;        # Error message reported by the check_batch_id() function
    my $pidList;         # List the postgres backend pid of the previous run when in restart

# Perform checks for the normal run mode.
    if ($actionRun) {
        # Check the previous run state
        if ($previousRunExists && $previousRunState ne 'Completed' && $previousRunState ne 'Suspended' && $previousRunState ne 'Aborted') {
            if ($previousRunPerlIsExecuting) {
                abort("The previous run for this batch is running.");
            } else {
                abort("The previous run for this batch doesn't seem to be running. Rerun with '--action abort' to cleanup this state");
            }
        }
    }
# Perform checks for the restart mode.
    if ($actionRestart) {
        # Check the run state
        if ($previousRunExists) {
            if ($previousRunState ne 'Suspended' && $previousRunState ne 'Aborted') {
                abort("The previous run for this batch must be either in 'Suspended' or 'Aborted' state to be restarted.");
            }
        } else {
            abort("There is no run to restart for this database and this batch.");
        }

        # Check that no postgres backend from the previous run is still alive
        $pidList = getBackendPids($previousRunId);
        if (defined($pidList) && arePidsExecuting($pidList, 'postgres')) {
            abort("Some postgres backend processes from the previous run seem to be still in execution.");
        }
    }

# Create the new run into the data2pg.run table and get its id.
    $quotedComment = $d2pDbh->quote($comment, { pg_type => PG_VARCHAR });
    $quotedTargetDb = $d2pDbh->quote($targetDb, { pg_type => PG_VARCHAR });
    $quotedBatchName = $d2pDbh->quote($batchName, { pg_type => PG_VARCHAR });

    $sql = qq(
        INSERT INTO data2pg.run
              (run_database, run_batch_name, run_init_max_ses, run_init_asc_ses, run_perl_pid, run_max_sessions, run_asc_sessions, run_comment)
        VALUES
              ($quotedTargetDb, $quotedBatchName, $maxSessions, $ascSessions, $$, $maxSessions, $ascSessions, $quotedComment)
        RETURNING run_id
    );
    ($runId) = $d2pDbh->selectrow_array($sql);

    $nextMaxSessionsRefreshTime = time() + $maxSessionsRefreshDelay;

# In restart mode ...
    if ($actionRestart) {
# ... register the new run id as restart run id for the restarted run.
        $sql = qq(
            UPDATE data2pg.run
                SET run_status = 'Restarted', run_restart_id = $runId
                WHERE run_id = $previousRunId
        );
        $ret = $d2pDbh->do($sql);
# ... and set the previous run id for the current run
        $sql = qq(
            UPDATE data2pg.run
                SET run_restarted_id = $previousRunId
                WHERE run_id = $runId
        );
        $ret = $d2pDbh->do($sql);
    }

# Commit the creation of the run.
    $d2pDbh->commit();

#### TODO: create needed directories to handle scripts executions

# Prepare the requested sessions and open the first one on the target database.
    adjustSessions();
    openSession(1);

# Check that the batch name exists on the target database and that its "migration" is in a correct state.
    $sql = qq(
        SELECT bi_batch_type, bi_mgr_name, bi_mgr_config_completed
            FROM data2pg.get_batch_ids()
            WHERE bi_batch_name = $quotedBatchName
    );
    ($batchType, $migrationName, $isMigCompleted) = $sessions[1]->{dbh}->selectrow_array($sql);
    if (!defined($batchType)) {
        abort("The batch name '$batchName' doesn't exist inside the target database.");
    }
    if (!$isMigCompleted) {
        abort("The batch name '$batchName' exists inside the target database. But the configuration of its migration '$migrationName' is not completed.");
    }

# Build the working plan, depending on the action.
    buildWorkingPlan() if ($actionRun);
    copyWorkingPlan() if ($actionRestart);

# Update the run state and the batch type
    $quotedBatchType = $d2pDbh->quote($batchType, { pg_type => PG_VARCHAR });
    $sql = qq(
        UPDATE data2pg.run
            SET run_status = 'In_progress', run_batch_type = $quotedBatchType
            WHERE run_id = $runId
    );
    $ret = $d2pDbh->do($sql);

# Commit this change.
    $d2pDbh->commit();

    print "The run #$runId is in progress...\n";
    print "  processing the batch $batchName of type $batchType for the database $pgDsn\n";
}

# ---------------------------------------------------------------------------------------------
sub adjustSessions
# Prepare the session structure to match the current requested maxSessions.
# If the max_sessions parameter has decreased, close the idle sessions.
{
    my $firstSession;    # First session to initialize

# Prepare the needed sessions.
    if ($#sessions == -1) {
        $firstSession = 1;
    } else {
        $firstSession = $#sessions + 1;
    }
    for (my $i = $firstSession; $i <= $maxSessions; $i++) {
        $sessions[$i]->{state} = 0;
    }
# Close the useless sessions.
    for (my $i = $maxSessions + 1; $i <= $#sessions; $i++) {
        if ($sessions[$i]->{state} == 2) {
        # The in-used sessions are just mark as "to be closed just after the step's completion"
            $sessions[$i]->{state} = 3;
        }
        # The idle sessions can be directly closed.
        if ($sessions[$i]->{state} == 1) {
            closeSession($i);
        }
    }
    if ($debug) {printDebug("Sessions adjusted to reach $maxSessions");}
}

# ---------------------------------------------------------------------------------------------
sub openSession
# Open a session on the target database.
# Record the timestamp of the session start.
{
    my ($i) = @_;
    my $sql;             # SQL statement
    my $schemaFound;     # boolean used to check the existence of the data2pg schema on the target database
    my $backendPid;      # pid of the postgres backend created at connection start

# Try to connect
    my $p = scalar reverse $cnxRole;
    $sessions[$i]->{dbh} = DBI->connect($pgDsn, $cnxRole, $p, {AutoCommit=>0})
          or abort("Error while logging on the target database ($DBI::errstr).");
    $sessions[$i]->{dbh}->{RaiseError} = 1;
    $sessions[$i]->{state} = 1;

# For the first connection, perform some specific checks.
    if ($i == 1) {
# Check that the data2pg schema exists.
        $sql = qq(
            SELECT 1 FROM pg_namespace WHERE nspname = 'data2pg'
        );
        ($schemaFound) = $sessions[$i]->{dbh}->selectrow_array($sql)
            or abort("The 'data2pg' schema does not exist in the target database.");

# Get the maximum number of sessions that could be opened,
        $sql = qq(
            SELECT current_setting('max_connections')::int - current_setting('superuser_reserved_connections')::int AS max_cnx
        );
        ($pgMaxCnx) = $sessions[$i]->{dbh}->selectrow_array($sql);

# ... and check that we are not too greedy.
        if ($maxSessions > $pgMaxCnx) {
            abort("The requested number of sessions ($maxSessions) exceeds the maximum number of connections that the targeted database can handle ($pgMaxCnx).");
        }
    }

# Set the application name to the just opened session and get the pid of the Postgres backend.
    $sql = qq(SET application_name = 'data2pg');
    $sessions[$i]->{dbh}->do($sql);

# Register the session in the data2pg database.
    $backendPid = $sessions[$i]->{dbh}->{pg_pid};
    $sql = qq(
        INSERT INTO data2pg.session
                (ses_run_id, ses_id, ses_backend_pid)
            VALUES ($runId, $i, $backendPid)
            ON CONFLICT (ses_run_id, ses_id) DO UPDATE
              SET ses_status = 'Opened', ses_backend_pid = $backendPid, ses_end_ts = NULL
    );
    $d2pDbh->do($sql);

    if ($debug) {printDebug("Session $i on the target database opened");}
}

# ---------------------------------------------------------------------------------------------
sub closeSession
# Close a session on the target database.
# Record the timestamp of the session end.
{
    my ($i) = @_;
    my $sql;             # SQL statement

# Disconnect.
    $sessions[$i]->{dbh}->disconnect();
    $sessions[$i]->{state} = 0;
# Record the status and the end timestamp of the session on the target database.
    $sql = qq(
        UPDATE data2pg.session
        SET ses_status = 'Closed', ses_end_ts = current_timestamp
        WHERE ses_run_id = $runId AND ses_id = $i
    );
    $d2pDbh->do($sql);

    if ($debug) {printDebug("Session $i to the targeted database closed.");}
}

# ---------------------------------------------------------------------------------------------
sub buildWorkingPlan {
# Build the working plan by executing a dedicated function on the target database.

    my $quotedBatchName; # The batch name quoted to be safely included into a SQL statement
    my $sql;             # SQL statement
    my $row;             # Row returned by a statement
    my $rows;            # Returned rows hash

# Prepare the INSERT into the step table.
    $sql = qq(
        INSERT INTO data2pg.step (stp_run_id, stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_parents, stp_blocking)
            VALUES ($runId, ?, ?, ?, ?, ?, ?)
    );
    $d2pSth = $d2pDbh->prepare($sql);

# Get the working plan from the target database (on the first connection).
    $quotedBatchName = $sessions[1]->{dbh}->quote($batchName, { pg_type => PG_VARCHAR });
    $sql = qq(
        SELECT wp_name, wp_sql_function, wp_shell_script, wp_cost, wp_parents
          FROM data2pg.get_working_plan($quotedBatchName)
    );
    $rows = $sessions[1]->{dbh}->selectall_arrayref($sql, {Slice => {}});
# Insert returned steps into the step table of the data2pg database.
    foreach my $row (@$rows) {
        $d2pSth->bind_param(1, $row->{wp_name});
        $d2pSth->bind_param(2, $row->{wp_sql_function});
        $d2pSth->bind_param(3, $row->{wp_shell_script});
        $d2pSth->bind_param(4, $row->{wp_cost});
        $d2pSth->bind_param(5, $row->{wp_parents});
        $d2pSth->bind_param(6, $row->{wp_parents});
        $d2pSth->execute();
        $nbSteps++;
    }
    $d2pSth->finish();

# Check that there is at least 1 step to execute.
    if ($nbSteps == 0) {
        abort("There is no step to execute.");
    }

# Compute the total cumulative cost for each step, taking into account the cost of all children steps for each parent step.
    $sql = qq(
        WITH RECURSIVE step_cte (stp_name, stp_cost) AS (
            SELECT stp_name, stp_cost
                FROM data2pg.step
                WHERE stp_run_id = $runId
          UNION ALL
            SELECT stp_one_parent, step.stp_cost
                FROM step_cte, step, unnest(stp_parents) AS stp_one_parent
                WHERE step.stp_name = step_cte.stp_name
                  AND step.stp_run_id = $runId
        ), step_cum AS (
            SELECT stp_name, sum(stp_cost) AS cum_cost
                FROM step_cte
                GROUP BY stp_name
        )
        UPDATE step
            SET stp_cum_cost = cum_cost
            FROM step_cum
            WHERE stp_run_id = $runId
              AND step.stp_name = step_cum.stp_name;
    );
    $d2pDbh->do($sql);

# Set the status of steps.
# Steps are set to 'Ready' when they have no parent.
    $sql = qq(
        UPDATE data2pg.step
            SET stp_status = 'Ready'
            WHERE stp_run_id = $runId AND stp_parents IS NULL
    );
    $nbStepsReady = $d2pDbh->do($sql);
    if ($nbStepsReady == 0) {
        abort("There is no 'Ready' step.");
    }

# Initialize the counter of successfuly completed steps.
    $nbStepsOK = 0;

    if ($debug) {printDebug("Working plan registered, with $nbSteps steps");}
}

# ---------------------------------------------------------------------------------------------
sub copyWorkingPlan {
# Build the working plan for the restart run.
    my $sql;             # SQL statement

# Copy the plan from the restarted run, after a cleanup of steps that were started but not completed.
# For the 'In_progress' steps, reset the status to 'Ready', delete the start timestamp and the assigned session_id.
    $sql = qq(
        INSERT INTO data2pg.step
                  (stp_run_id, stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_parents, stp_cum_cost,
                   stp_status, stp_blocking, stp_ses_id, stp_start_ts, stp_end_ts)
            SELECT $runId, stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_parents, stp_cum_cost,
                   CASE WHEN stp_status = 'In_progress' THEN 'Ready' ELSE stp_status END,           -- stp_status column
                   stp_blocking,
                   CASE WHEN stp_status = 'In_progress' THEN NULL ELSE stp_ses_id END,              -- stp_ses_id column
                   CASE WHEN stp_status = 'In_progress' THEN NULL ELSE stp_start_ts END,            -- stp_start_ts column
                   stp_end_ts
        FROM data2pg.step
        WHERE stp_run_id = $previousRunId
    );
    $nbSteps = $d2pDbh->do($sql);
    # No step to copy, because the previous run had not completed its initilization.
    if ($nbSteps == 0) {
        abort("The session to restart had not completed its initialisation. Abort the run (--action abort) and respawn (--action run).");
    }

# Copy the completed step results from the previous run
    $sql = qq(
        INSERT INTO data2pg.step_result
            SELECT $runId, sr_step, sr_indicator, sr_value, sr_rank, sr_is_main_indicator
        FROM data2pg.step_result
        WHERE sr_run_id = $previousRunId
    );
    $d2pDbh->do($sql);

# Recompute the number of 'Ready' and already 'Completed' steps.
    $sql = qq(
        SELECT count(*) FILTER (WHERE stp_status = 'Completed') AS nb_completed_steps,
               count(*) FILTER (WHERE stp_status = 'Ready') AS nb_ready_steps
            FROM data2pg.step 
            WHERE stp_run_id = $runId
    );
    ($nbStepsOK, $nbStepsReady) = $d2pDbh->selectrow_array($sql);
    $previousNbStepsOK = $nbStepsOK;

    if ($debug) {printDebug("Working plan registered for the restarted session, with $nbSteps steps and $nbStepsOK already completed");}
}

# ---------------------------------------------------------------------------------------------
sub processRun
# This is the main loop.
# If a session is available and something has to be done, use it for the next operation to perform.
# Then look for the in progress operations just completed.
{
    my $previousMaxSessions;     # The maxSessions value before refreshing the parameter
    my $runCompleted = 0;        # Boolean indicating that the run is completed
    my $runSuspended = 0;        # Boolean indicating that the run is suspended
    my $i;
    my $sleepDuration = 0;       # Timer delay for the main loop, computed from the steps costs
                                 #   Its value is between $sleepMinDuration and $sleepMaxDuration
                                 #   Between these limis, it is incremented by $sleepDurationIncr at every loop
                                 #   It is reset to 0 when a new step is started or a step is detected as completed

    while (!$runCompleted && !$runSuspended) {

# Reread the maxSessions parameter if it's time to do it.
        if (time() > $nextMaxSessionsRefreshTime) {
            $previousMaxSessions = $maxSessions;
            reReadMaxSessions();
# If more or less sessions are requested, prepare for open or close and close those which can be immediately closed.
            if ($maxSessions != $previousMaxSessions) {
                adjustSessions();
            }
        }

# Sleep if needed before going on,
        if ($sleepDuration > 0) {
            Time::HiRes::sleep $sleepDuration;
# ... and increment the sleep duration if the max has not been reached.
            $sleepDuration = $sleepDuration + $sleepDurationIncr if ($sleepDuration < $sleepMaxDuration);
        } else {
            $sleepDuration = $sleepMinDuration;
        }

# If there are Ready steps, feed available sessions, if any.
        for (my $i = 1; $i <= $maxSessions; $i++) {
            if ($nbStepsReady > 0 && $sessions[$i]->{state} <= 1) {        # A session is available for a Ready step
                startStep($i);
                if ($sessions[$i]->{state} == 2) {    # A step has started
                    $sleepDuration = 0;
                }
            }
        }

#### TODO: Look at shell scripts in execution
####        pidShellInExecution();

# Detect completed steps.
        $nbBusySessions = 0;
        for (my $i = 1; $i <= $#sessions; $i++) {     # Look at all known sessions (even greater than the maxSession current value)
            if ($sessions[$i]->{state} >= 2) {        # A session was in progress (usualy state = 2 or state = 3 if the session must be closed)
                checkStep($i);
                if ($sessions[$i]->{state} <= 1) {    # A step has ended
                    $sleepDuration = 0;
                } else {                              # A step is in execution
                    $nbBusySessions++;
                }
            }
        }

# Evaluate the end loop conditions : the run is either completed or suspended.
# - run completed: the number of steps to process equals the number of completed steps
        $runCompleted = ($nbSteps == $nbStepsOK);
# - run suspended: all steps are not completed and max_sessions is set to 0 and no step in execution anymore
        $runSuspended = ($nbSteps != $nbStepsOK && $maxSessions == 0 && $nbBusySessions == 0);

# Detect error cases when it remains steps to perform and there is no in progress step but none being ready to start.
        if (!$runCompleted && $nbBusySessions == 0 && $nbStepsReady == 0) {
            abort("Steps remain to be run but none are ready to start. This is probably a planning error.");
        }
    }
}

# ---------------------------------------------------------------------------------------------
sub reReadMaxSessions
# Read the run row from the data2pg database to detect any change in the MAX_SESSIONS parameter.
{
    my $sql;             # SQL statement

# Reread the run table.
    $sql = qq(
        SELECT run_max_sessions, run_asc_sessions FROM data2pg.run
        WHERE run_id = $runId
    );
    ($maxSessions, $ascSessions) = $d2pDbh->selectrow_array($sql);

# Check the new max sessions value.
    if ($maxSessions > $pgMaxCnx) {
        abort("The requested number of sessions ($maxSessions) exceeds the maximum number of connections that the targeted database can handle ($pgMaxCnx).");
    }

# Set the timer for the next call.
    $nextMaxSessionsRefreshTime = time() + $maxSessionsRefreshDelay;

    if ($debug) {printDebug("Sessions parameters refreshed (Max=$maxSessions Asc=$ascSessions)");}
}

# ---------------------------------------------------------------------------------------------
sub startStep
# Start the next candidate step on the available session.
{
    my ($session) = @_;
    my $order;           # sort order
    my $sql;             # SQL statement
    my $step;            # Selected step name 
    my $quotedBatchName; # Batch name properly quoted for the SQL
    my $quotedStep;      # Quoted step so that it can be used in a SQL statement
    my $sqlFunction;     # The SQL function to call, if supplied
    my $shellScript;     # The shell sctipt to spawn, if supplied
    my $cost;            # The cost of the selected step
    my $cumCost;         # The cumlative cost of the selected step

# Look for the next step to process.
# Set the order the steps will be retrieved, depending on the session number and the ASC_SESSIONS parameter.
    $order = ($session <= $ascSessions) ? 'ASC' : 'DESC';
# Get the first step in Ready state
    $sql = qq(
        SELECT stp_name, stp_sql_function, stp_shell_script, stp_cost, stp_cum_cost
          FROM data2pg.step
          WHERE stp_run_id = $runId AND stp_status = 'Ready'
          ORDER BY stp_cum_cost $order, stp_name
          LIMIT 1
    );
    ($step, $sqlFunction, $shellScript, $cost, $cumCost) = $d2pDbh->selectrow_array($sql);
	$quotedBatchName = $d2pDbh->quote($batchName, { pg_type => PG_VARCHAR });
    $quotedStep = $d2pDbh->quote($step, { pg_type => PG_VARCHAR });

# Update the step state, the assigned session id and the start time.
    $sql = qq(
        UPDATE data2pg.step
          SET stp_status = 'In_progress', stp_start_ts = clock_timestamp(), stp_ses_id = $session
          WHERE stp_run_id = $runId AND stp_name = $quotedStep
    );
    $d2pDbh->do($sql);
    $nbStepsReady--;
# Commit the step start.
    $d2pDbh->commit();

    if (defined($sqlFunction)) {
# The step is a SQL function.
# Open the connection if needed.
        if ($sessions[$session]->{state} == 0) {
            openSession($session);
        }
# And execute the function asynchronously.
        $sql = qq(
            SELECT * FROM $sqlFunction($quotedBatchName, $quotedStep) AS t
        );
        $sessions[$session]->{sth} = $sessions[$session]->{dbh}->prepare($sql, {pg_async => PG_ASYNC});
        $sessions[$session]->{sth}->execute();
    } else {

# The step is a shell script. Spawn it.
#### TODO: code it
        die "Not yet implemented\n";
    }

# Keep in memory the step and the session state.
    $sessions[$session]->{step} = $step;
    $sessions[$session]->{state} = 2;

    if ($debug) {printDebug("On session $session, start the step $step (cost = $cost ; cum.cost = $cumCost)");}
}

# ---------------------------------------------------------------------------------------------
sub checkStep
# Check if an in_progress session is completed.
{
    my ($session) = @_;
    my $insertValues;    # The VALUES clause for the INSERT statement that records the step results.
    my $resultsList;     # The list of the step results to display in debug mode.
    my $ret;             # SQL result
    my $i;               # Result rows counter
    my $step;            # Selected step name 
    my $sql;             # SQL statement
    my $row;             # Row returned by a statement
    my $indicatorValue;  # The value associated to the indicator returned by the step execution
    my $indicatorRank;   # The indicator rank returned by the step execution
    my $isMainIndicator; # Boolean indicating whether the indicators returned by the function are main indicators (to display by the monitoring clients)
    my $quotedStep;      # Quoted step so that it can be used in a SQL statement
    my $quotedIndicator; # The indicator quoted to be used in a SQL statement

# Check if the statement of the session is completed.
    if ($sessions[$session]->{dbh}->pg_ready) {
# The step is completed.
        $step = $sessions[$session]->{step};
        $quotedStep = $d2pDbh->quote($step, { pg_type => PG_VARCHAR });

# Get the returned values.
        $insertValues = ''; $resultsList = '';
        $ret = $sessions[$session]->{dbh}->pg_result()
            or abort("Error while processing the step $step ($DBI::errstr)");
# Get and process each row of the step result.
        for ($i = 1; $i <= $ret; $i++) {
            $row = $sessions[$session]->{sth}->fetchrow_hashref();
            $quotedIndicator = $d2pDbh->quote($row->{sr_indicator}, { pg_type => PG_VARCHAR });
            $indicatorValue = $row->{sr_value};
            $indicatorRank = $row->{sr_rank};
            $isMainIndicator = $row->{sr_is_main_indicator} == 1 ? 'TRUE' : 'FALSE';
# Build the rows to insert into the step_result table
            $insertValues .= "($runId, $quotedStep, $quotedIndicator, $indicatorValue, $indicatorRank, $isMainIndicator)";
            $resultsList .= "$row->{sr_indicator}=$indicatorValue ";
        }
        $sessions[$session]->{sth}->finish()
            or abort("Error while processing the step $step ($DBI::errstr)");
# Add a ',' character between each row to insert.
        $insertValues =~ s/\)\(/\),\(/g;

# Commit the step on the target database.
        $sessions[$session]->{dbh}->commit();

# Insert the step result into the data2pg database.
        if ($insertValues ne '') {
            $sql = qq(
                INSERT INTO data2pg.step_result VALUES $insertValues
            );
            $d2pDbh->do($sql);
        }

# Update the step in the data2pg database (status and end time).
        $quotedStep = $d2pDbh->quote($step, { pg_type => PG_VARCHAR });
        $sql = qq(
            UPDATE data2pg.step
                SET stp_status = 'Completed', stp_end_ts = clock_timestamp()
                WHERE stp_run_id = $runId AND stp_name = $quotedStep
        );
        $ret = $d2pDbh->do($sql);
        if ($ret != 1) {
            abort("Internal error while updating the step status for the session $i : nb rows = $ret");
        }

# Un-block other steps whose parents array contains the just completed step.
        # remove the just completed step from the blocking steps array of the other steps of the run.
        $sql = qq(
            UPDATE data2pg.step
                SET stp_blocking = array_remove(stp_blocking, $quotedStep)
                WHERE stp_run_id = $runId
                  AND $quotedStep = ANY(stp_blocking)
        );
        $ret = $d2pDbh->do($sql);
        # if some changes have been performed, infer the potential changes in the blocked steps state.
        if ($ret > 0) {
            $sql = qq(
                UPDATE data2pg.step
                    SET stp_status = 'Ready'
                    WHERE stp_run_id = $runId
                      AND stp_status = 'Blocked'
                      AND stp_blocking = '{}'
            );
            $ret = $d2pDbh->do($sql);
            $nbStepsReady = $nbStepsReady + $ret;
        }

# Validate the changes.
        $d2pDbh->commit();

# Change the state of this session and increment counters.
        if ($sessions[$session]->{state} == 3) {
            closeSession($session);
        } else {
            $sessions[$session]->{state} = 1;
        }
        $sessions[$session]->{step} = '';
        $nbStepsOK++;

        if ($debug) {
            $resultsList =~ s/ $//;                 # delete the last space
            printDebug("On session $session, the step $step is completed ($resultsList)");
        }
    }
}

# ---------------------------------------------------------------------------------------------
sub endRun
# End of the run.
# The run must be marked as 'completed' and the session on the data2pg database can be closed.
{
    my $sql;             # SQL statement
    my $state;           # Final state of the run
    my $ret;             # SQL result

# Close each opened sessions to the target database.
    for (my $i = 1; $i <= $#sessions; $i++) {
        if ($sessions[$i]->{state} != 0) {
            closeSession($i);
        }
    }

# Set the run status to 'Completed' or 'Suspended'.
    $state = 'Completed'; $state = 'Suspended' if ($nbSteps != $nbStepsOK);
    $sql = qq(
        UPDATE data2pg.run
            SET run_status = '$state', run_end_ts = current_timestamp
            WHERE run_id = $runId
    );
    $ret = $d2pDbh->do($sql);
    if ($ret != 1) {
        abort("Internal error while updating the run at endRun(): nb rows = $ret");
    }

# Commit the run closing transaction.
    $d2pDbh->commit();

# Display the summary final report.
    finalReport();

    if ($debug) {printDebug("Run ended in $state state.");}
}

# ---------------------------------------------------------------------------------------------
# Compute and display the final report of the run.
sub finalReport {
    my $sql;             # SQL statement
    my $ret;             # SQL result
    my $rowRun;          # Result row for the statement on the run table
    my $elapseTime;      # Total elapse time of the run
    my $stepResults;     # The aggregated step results
    my $state;           # Final state of the run

# Compute the summary figures.
    # The run elapse time.
    $sql = qq(
        SELECT run_end_ts - run_start_ts AS "Elapse"
        FROM data2pg.run
        WHERE run_id = $runId
    );
    ($elapseTime) = $d2pDbh->selectrow_array($sql);

    # The aggregated step results.
    $sql = qq(
        SELECT sr_indicator, sr_rank, sum(sr_value) AS "total"
        FROM data2pg.step_result
        WHERE sr_run_id = $runId
        GROUP BY sr_indicator, sr_rank
        ORDER BY sr_rank
    );
    $stepResults = $d2pDbh->selectall_arrayref($sql, { Slice => {} });

# Display the report.
    $state = 'completed'; $state = 'suspended' if ($nbSteps != $nbStepsOK);
    print "========================================================================================\n";
    print "The run #$runId is $state. (The operation details are available in the data2pg database).\n";
    print "Run elapse time           : $elapseTime\n";
    if ($state eq 'suspended') {
        print "Number of scheduled steps : $nbSteps\n";
    }
    print "Number of processed steps : $nbStepsOK\n";
    print "Aggregated indicators\n";
    foreach my $indicator ( @$stepResults ) {
        printf("     %-20.20s : %d\n", $indicator->{sr_indicator}, $indicator->{total});
    }
    if ($actionRestart) {
      print "Number of steps processed by the previous restarted run #$previousRunId : $previousNbStepsOK\n";
    }
    print "========================================================================================\n";
}

# ---------------------------------------------------------------------------------------------
sub abortRun {
# Abort the current run for the batch.
    my $sql;             # SQL statement
    my $pgPidsToKill;    # Pids array of postgres backends to kill
    my $pgKilledPids;    # Pids array of effectively killed postgres backends
    my $dbh;             # A connection on the target database, to kill potential backend still in execution
    my $ret;             # SQL result

# Check the previous run state, if any.
    if ($previousRunExists) {
        if ($previousRunState eq 'Completed' || $previousRunState eq 'Aborted') {
            abort("The previous run for this batch is already in 'Completed' or 'Aborted' state.");
        }
    } else {
        abort("There is no run to abort for this database and this batch.");
    }

# If the previous run is in execution, kill it.
    if ($previousRunPerlIsExecuting) {
        `kill $previousRunPerlPid`;
        if ($debug) {printDebug("Process perl $previousRunPerlPid killed.");}
    }

# Kill the Postgres backend corresponding to the opened sessions, if any.
# Get the list of pids to terminate
    $sql = qq(
      SELECT array_agg(ses_backend_pid)::TEXT AS pg_pid_array
          FROM data2pg.session
          WHERE ses_run_id = $previousRunId
            AND ses_status = 'Opened'
    );
    ($pgPidsToKill) = $d2pDbh->selectrow_array($sql);

    if (defined($pgPidsToKill)) {
# If there are some process to kill, open a session on the target database,
        my $p = scalar reverse $cnxRole;
        $dbh = DBI->connect($pgDsn, $cnxRole, $p, {AutoCommit=>0})
              or abort("Error while logging on the target database ($DBI::errstr).");

# ... ask for the backends termination,
        $sql = qq(
          SELECT data2pg.terminate_data2pg_backends('$pgPidsToKill')
        );
        ($pgKilledPids) = $dbh->selectrow_array($sql);
        if ($debug) {printDebug("PostgreSQL backends terminated: $pgPidsToKill.");}

# ... close the connection,
        $dbh->disconnect();

# ... and set the session state to 'Aborted'.
        $sql = qq(
          UPDATE data2pg.session
              SET ses_status = 'Aborted', ses_end_ts = current_timestamp
              WHERE ses_run_id = $previousRunId
                AND ses_status = 'Opened'
        );
        $d2pDbh->do($sql);
    }

# Set the run state to 'Aborted'.
    $sql = qq(
      UPDATE data2pg.run
          SET run_status = 'Aborted', run_end_ts = current_timestamp, run_error_msg = 'Aborted by a "data2pg.pl --action abort" command'
          WHERE run_id = $previousRunId
    );
    $ret = $d2pDbh->do($sql);
    if ($ret != 1) {
        abort("Internal error while updating the run at abortRun(): nb rows = $ret");
    }

# Commit the change.
    $d2pDbh->commit();

    if ($debug) {printDebug("Run #$previousRunId set to 'Aborted'.");}

# Final report.
    print "========================================================================================\n";
    print "The run #$previousRunId has been aborted.\n";
    print "========================================================================================\n";
}

# ---------------------------------------------------------------------------------------------
sub suspendRun {
# Suspend the current run for the batch.
    my $sql;             # SQL statement
    my $ret;             # SQL result

# Check the previous run state, if any.
    if (!$previousRunExists || !$previousRunPerlIsExecuting ||
        $previousRunState eq 'Completed' || $previousRunState eq 'Aborted' || $previousRunState eq 'Suspended') {
        abort("There is no run currently in execution for this batch.");
    }

# Set the max_sessions to 0 for the run.
    $sql = qq(
      UPDATE data2pg.run
          SET run_max_sessions = 0
          WHERE run_id = $previousRunId
    );
    $ret = $d2pDbh->do($sql);
    if ($ret != 1) {
        abort("Internal error while updating the run at suspendRun(): nb rows = $ret");
    }

# Commit the change.
    $d2pDbh->commit();

# Final report.
    print "========================================================================================\n";
    print "The run #$previousRunId will be set in 'Suspended' state as soon as all the steps in execution will be completed.\n";
    print "========================================================================================\n";
}
# ---------------------------------------------------------------------------------------------
# The main function.

print " Data2Pg - version $toolVersion\n";
print "-----------------------\n";

# Parse the command line.
parseCommandLine();

# Load the configuration file, if specified in the command line.
if ($confFile) {
    parseConfigurationFile();
}

# Set and check the parameter values.
checkParameters();

# Log on the data2pg database and get the status of the previous run on the same target database and for the same batch.
logonData2pg();
getPreviousRunStatus();

# Execute the requested action.
if ($actionAbort) {
    abortRun();
} elsif ($actionSuspend) {
    suspendRun();
} else {              # $actionRun or $actionRestart
# Initialize the run.
    initRun();
# Main processing.
    processRun();
# End of the run.
    endRun();
}
# Log off the data2pg database.
logoffData2pg();

# ---------------------------------------------------------------------------------------------
