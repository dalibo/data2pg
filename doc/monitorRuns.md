# How to monitor runs

There are two ways to monitor in-progress or completed batch runs: either using the web client interface or running the data2pg_monitor client in a terminal.

The shell command syntax can be displayed with the --help option.

```sh
perl data2pg_monitor.pl --help
data2pg_monitor.pl belongs to the data2pg project (version 0.5).
It monitors data migrations towards PostgreSQL performed by the data2pg scheduler.

Usage:
  data2pg_monitor.pl [OPTION]...

Options:
  --host      IP host of the data2pg database (default = PGHOST env. var.)
  --port      IP port of the data2pg database (default = PGPORT env. var.)
  --lines     maximum number of lines to display (0 = default = no page size limit)
  --run       optional run id to examine. If no specific run is specified, display the latest runs
                registered into the data2pg database
  --delay     delay in seconds between 2 displays of details for a specific 'Initializing',
                'In_progress' or 'Ending' run (default = 5)
  --help      shows this help, then exit
  --version   outputs version information, then exit

Examples:
  data2pg_monitor.pl --host localhost --port 5432 --lines 40 --delay 10
              displays the list of the latest registered run, with no more than 40 generated lines
  data2pg_monitor.pl --host localhost --port 5432 --lines 40 --run 58 --delay 10
              monitors the run #58 and refresh the screen every 10 seconds
```

