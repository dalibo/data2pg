#! /usr/bin/perl -w
# data2pg_monitor.pl
# This perl module belongs to the data2pg project.
#
# It monitors data migrations towards PostgreSQL performed by the data2pg scheduler.

use warnings;
use strict;

use Getopt::Long;
use DBI;
use POSIX qw(strftime floor);
use vars qw($VERSION $PROGRAM $APPNAME);

$VERSION = '0.1';
$PROGRAM = 'data2pg_monitor.pl';
$APPNAME = 'data2pg_monitor';

# Initialize parameters with their default values.
my $options;                              # Options from the command line
my $host;                                 # IP host of the data2pg database
my $port;                                 # IP port of the data2pg database
my $help = 0;
my $version = 0;
my $lines = 0;                            # --lines (0 means no page size limit)
my $delay = 5;                            # --delay (in seconds, default=5s)
my $run = undef;                          # --run identifier

# Global variables to manage the sessions and the statements to the databases.
my $cnxRole = 'data2pg';                  # The role name used for all connections to postgres databases
my $d2pDbh;                               # handle for the connection on the data2pg database
my $d2pSth;                               # handle for the statements to submit on the data2pg database connection

# Other variables.

# ---------------------------------------------------------------------------------------------
# Parse the comand line and get the options.
sub parseCommandLine
{

# Parse.
    $options = GetOptions(
        "help"      => \$help,
        "version"   => \$version,
        "host=s"    => \$host,
        "port=s"    => \$port,
        "lines=s"   => \$lines,
        "run=s"     => \$run,
        "delay=s"   => \$delay,
        );

# Help!
    if ($help) {
        print_help();
        exit 1;
    }

    if ($version) {
        print_version();
        exit 1;
    }
}

# ---------------------------------------------------------------------------------------------
sub print_help {
  print qq{$PROGRAM belongs to the data2pg project (version $VERSION).
It monitors data migrations towards PostgreSQL performed by the data2pg scheduler.

Usage:
  $PROGRAM [OPTION]...

Options:
  --host      IP host of the data2pg database (default = PGHOST env. var.)
  --port      IP port of the data2pg database (default = PGPORT env. var.)
  --lines     maximum number of lines to display (0 = default = no page size limit)
  --run       optional run id to examine. If no specific run is specified, display the latest runs
                registered into the data2pg database
  --delay     delay in seconds between 2 displays of details for a specific 'Initializing',
                'In_progress' or 'Ending' run (default = 3)
  --help      shows this help, then exit
  --version   outputs version information, then exit

Examples:
  $PROGRAM --host localhost --port 5432 --lines 40 --delay 10
              displays the list of the latest registered run, with no more than 40 generated lines
  $PROGRAM --host localhost --port 5432 --lines 40 --run 58 --delay 10
              monitors the run #58 and refresh the screen every 10 seconds
};
}

# ---------------------------------------------------------------------------------------------
sub print_version {
  print ("This version of $PROGRAM belongs to the data2pg project. Its current version is $VERSION.\n");
  print ("Type '$PROGRAM --help' to get usage information\n\n");
}

# ---------------------------------------------------------------------------------------------
# Check parameters read from the configuration file and assign the default values for those not already set.
sub checkParameters
{
    if ($lines < 0) {
        die("Number of lines to display (".$lines.") must be >= 0, 0 means no page size limit !\n");
    }
    if (defined($run) && $delay <= 0) {
        die("Refresh delay (".$delay.") must be > 0 !\n");
    }
}

# ---------------------------------------------------------------------------------------------
sub logonData2pg {
# The function opens the connection on the data2pg database.
    my $d2pDsn;          # The DSN to reach the data2pg database
    my $sql;             # SQL statement
    my $sth;             # Statement handle
    my $ret;             # SQL result

# Set the data2pg database connection DSN.
    $d2pDsn = "dbi:Pg:dbname=data2pg";
    $d2pDsn .= ";host=$host" if defined $host;
    $d2pDsn .= ";port=$port" if defined $port;

# Open the connection on the data2pg database.
    my $p = scalar reverse $cnxRole;
    $d2pDbh = DBI->connect($d2pDsn, $cnxRole, $p)
        or die("Error while logging on the data2pg database ($DBI::errstr).");

# Set the application_name.
    $d2pDbh->do("SET application_name to '".$APPNAME."'")
        or die('Set the application_name failed '.$DBI::errstr."\n");

# Check that the data2pg schema exists.
    $sql = qq(SELECT 0 FROM pg_namespace WHERE nspname = 'data2pg');
    $sth = $d2pDbh->prepare($sql);
    $ret = $sth->execute();
    ($ret == 1) or die("The 'data2pg' schema does not exist in the data2pg database.");
}

# ---------------------------------------------------------------------------------------------
sub logoffData2pg {
# The function closes the connection on the data2pg database.

    $d2pDbh->disconnect();
}

# ---------------------------------------------------------------------------------------------
sub showLatestRuns {
    my $sql;             # SQL statement
    my $sth;             # Statement handle
    my $row;             # result row
    my $limit;

    system("clear");
    print (strftime('%d/%m/%Y - %H:%M:%S',localtime) . "     $APPNAME (version $VERSION)\n");

# Retrieve the latest runs.
    if ($lines > 2) {
        $limit = 'LIMIT ' . ($lines - 2);
    } else {
        $limit = '';
    }
    $sql = qq(
        SELECT run_id, run_database, run_batch_name, run_status, run_max_sessions, run_start_ts,
               coalesce(run_end_ts::TEXT,'') AS run_end_ts, coalesce((run_end_ts - run_start_ts)::TEXT,'') AS run_elapse,
               coalesce(run_error_msg,'') AS run_error_msg
          FROM data2pg.run ORDER BY run_id DESC $limit
    );
    $sth = $d2pDbh->prepare($sql);
    $sth->execute()
        or die('Access to the run table failed '.$DBI::errstr."\n");

# Display results.
    $sth->{pg_expand_array} = 0;
    print "  Run Target database      Batch_name        Status   Sessions   Start                 End              Elapse           Error_msg\n";
    while ($row = $sth->fetchrow_hashref()) {
        printf("%5u %-20.20s %-15.15s %-11.11s",$row->{'run_id'}, $row->{'run_database'}, $row->{'run_batch_name'}, $row->{'run_status'});
        printf(" %3u %-19.19s %-19.19s %15.15s",$row->{'run_max_sessions'}, $row->{'run_start_ts'}, $row->{'run_end_ts'}, $row->{'run_elapse'});
        printf(" %-30.30s", $row->{'run_error_msg'});
        print "\n";
  }
    $sth->finish;

}

# ---------------------------------------------------------------------------------------------
sub showRunDetails {
    my $loop;            # Boolean used to manage the main loop
    my $currLine;        # Current line number
    my $sql;             # SQL statement
    my $limit;           # Maximum number of steps to retrieve
    my $sth;             # Statement handle
    my $ret;             # SQL result
    my $row;             # Result row
    my $previousStatus;  # The status of the previous step 

# Loop every delay seconds.
    $loop = 1;
    while ($loop) {
        $currLine = 0;
        system("clear");
# Header line.
        print ("$APPNAME (version $VERSION)     Run: $run at " . strftime('%d/%m/%Y - %H:%M:%S',localtime) . "\n");
        $currLine++;
# Get global statistics for the run.
        $sql = qq(
            SELECT run_id, run_database, run_batch_name, run_batch_type, run_status, run_max_sessions, run_start_ts,
                   coalesce(run_end_ts::TEXT,'') AS run_end_ts,
                   to_char(coalesce(run_end_ts, current_timestamp) - run_start_ts, 'HH24:MI:SS') AS run_elapse,
                   coalesce(run_error_msg,'') AS run_error_msg, coalesce(run_wording,'') AS run_wording, run_restart_id,
                   count(step.*) AS total_steps, sum(stp_cost) AS total_cost,
                   count(step.*) FILTER (WHERE stp_status = 'Completed') AS completed_steps,
                   sum(stp_cost) FILTER (WHERE stp_status = 'Completed') AS completed_cost,
                   count(step.*) FILTER (WHERE stp_status = 'In_progress') AS in_progress_steps,
                   sum(stp_cost) FILTER (WHERE stp_status = 'In_progress') AS in_progress_cost
                FROM data2pg.run
                     JOIN data2pg.step ON (stp_run_id = run_id)
                WHERE run_id = $run
                GROUP BY run_id
        );
        $sth = $d2pDbh->prepare($sql);
        $ret = $sth->execute()
            or die('Access to the run table failed '.$DBI::errstr."\n");
        if ($ret != 1) {
            die("Run $run not found\n");
        }
        $row = $sth->fetchrow_hashref();
        $loop = 0 if ($row->{run_status} =~ /Completed|Aborted|Suspended|Restarted/);
# Display general information about the run.
        printf("Database: %s Batch: %s Type: %s Status: '%s' Sessions: %u ",
               $row->{'run_database'}, $row->{'run_batch_name'}, $row->{'run_batch_type'},
               $row->{'run_status'}, $row->{'run_max_sessions'});
        if ($row->{'run_wording'} ne '') {
            printf(" Wording: %s",$row->{'run_wording'});
        }
        print "\n"; $currLine++;
#
        if ($row->{'run_error_msg'} ne '') {
            printf("Abort: %s\n",$row->{'run_error_msg'});
            $currLine++;
#### TODO: count newlines from the error message.
        }
# 
        printf("Started: %-19.19s",
               $row->{'run_start_ts'});
        if ($row->{'run_end_ts'} ne '') {
            printf(" Ended:%-19.19s", $row->{'run_end_ts'});
        }
        if ($row->{run_status} ne 'Aborted') {
            printf(" Elapse:%-19.19s", $row->{'run_elapse'});
        }
        print "\n"; $currLine++;
#
        printf("Steps Total: %d Completed: %d (%d%%/%d%%)",
               $row->{'total_steps'}, $row->{'completed_steps'},
               $row->{'completed_steps'}*100/$row->{'total_steps'},
               $row->{'completed_cost'}*100/$row->{'total_cost'}
              );
        if ($row->{run_status} eq "In_progress") {
            printf(" In_progress: %d (%d%%/%d%%) Other: %d (%d%%/%d%%)",
                   $row->{'in_progress_steps'},
                   $row->{'in_progress_steps'}*100/$row->{'total_steps'},
                   floor($row->{'in_progress_cost'}*100/$row->{'total_cost'}),
                   floor($row->{'total_steps'} - $row->{'completed_steps'} - $row->{'in_progress_steps'}),
                   100 - floor($row->{'completed_steps'}*100/$row->{'total_steps'}) - floor($row->{'in_progress_steps'}*100/$row->{'total_steps'}),
                   100 - floor($row->{'completed_cost'}*100/$row->{'total_cost'}) - floor($row->{'in_progress_cost'}*100/$row->{'total_cost'}),
                  );
        }
        print "\n"; $currLine++;

# Get information about steps.
        if ($lines > 2) {
            $limit = 'LIMIT ' . ($lines - $currLine);
        } else {
            $limit = '';
        }
        $sql = qq(
            SELECT stp_name, stp_cost, stp_status, array_length(stp_blocking, 1) AS nb_blocking,
                   stp_ses_id, stp_start_ts, stp_stop_ts, stp_return_value,
                   CASE WHEN stp_status = 'Completed' THEN to_char(stp_stop_ts - stp_start_ts, 'HH24:MI:SS.US')
                        WHEN stp_status = 'In_progress' THEN to_char(current_timestamp - stp_start_ts, 'HH24:MI:SS')
                        ELSE NULL
                   END AS stp_elapse
            FROM data2pg.step,
                 (VALUES ('In_progress',1),('Ready',2),('Blocked',3),('Completed',4)) AS state(state_name, state_order)
            WHERE state_name = stp_status::TEXT
              AND stp_run_id = $run
            ORDER BY state_order, stp_start_ts DESC, stp_cost DESC, stp_name
            $limit
        );
        $sth = $d2pDbh->prepare($sql);
        $sth->execute()
            or die('Access to the step table failed '.$DBI::errstr."\n");

# Display results.
        $sth->{pg_expand_array} = 0;
        $previousStatus = '';
        while (($lines == 0 || $currLine <= $lines) && ($row = $sth->fetchrow_hashref())) {
            if ($row->{'stp_status'} ne $previousStatus) {
                if ($row->{'stp_status'} eq 'In_progress') {
                    print "    'In_progress' steps               Estim.Cost Sess.      Start         Elapse";
                } elsif ($row->{'stp_status'} eq 'Ready') {
                    print "    'Ready' steps                     Estim.Cost ";
                } elsif ($row->{'stp_status'} eq 'Blocked') {
                    print "    'Blocked' steps                   Estim.Cost Blocking";
                } elsif ($row->{'stp_status'} eq 'Completed') {
                    print "    'Completed' steps                 Estim.Cost Sess.       Start               Stop            Elapse      Return Value";
                }
                print "\n"; $currLine++;
            }
            if ($row->{'stp_status'} eq 'In_progress') {
                printf("%-35.35s %12u %3u %-19.19s %-12.12s",
                    $row->{'stp_name'}, $row->{'stp_cost'}, $row->{'stp_ses_id'}, $row->{'stp_start_ts'}, $row->{'stp_elapse'});
            } elsif ($row->{'stp_status'} eq 'Ready') {
                printf("%-35.35s %12u",
                    $row->{'stp_name'}, $row->{'stp_cost'});
            } elsif ($row->{'stp_status'} eq 'Blocked') {
                printf("%-35.35s %12u  %3u",
                    $row->{'stp_name'}, $row->{'stp_cost'}, $row->{'nb_blocking'});
            } elsif ($row->{'stp_status'} eq 'Completed') {
                printf("%-35.35s %12u %3u %-19.19s %-19.19s %15.15s %12u",
                    $row->{'stp_name'}, $row->{'stp_cost'}, $row->{'stp_ses_id'},
                    $row->{'stp_start_ts'}, $row->{'stp_stop_ts'}, $row->{'stp_elapse'}, $row->{'stp_return_value'});
            }
            print "\n"; $currLine++;
            $previousStatus = $row->{'stp_status'};
        }
        $sth->finish;

        sleep $delay if ($loop);
    }
}

# ---------------------------------------------------------------------------------------------
#
# Main function.
#

# Parse and check the command line.
parseCommandLine();
checkParameters();

# Log on the data2pg database.
logonData2pg();

# Depending whether the --run parameter is set or not, call the appropriate function.
if (defined $run) {
    showRunDetails();
} else {
    showLatestRuns();
}

# Log off the data2pg database.
logoffData2pg();
