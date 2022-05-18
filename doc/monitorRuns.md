# How to monitor runs

There are two ways to monitor in-progress or completed batch runs: either using the web client interface or running the data2pg_monitor client in a terminal.

The shell command syntax can be displayed with the --help option.

```sh
perl data2pg_monitor.pl --help
data2pg_monitor.pl belongs to the data2pg project (version 0.6).
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

The details of a completed batch of type COMPARE looks like:

```
data2pg_monitor (version 0.6)          Run: 4          18/05/2022 - 08:15:39
Database: pg_test_db    Batch: COMPARE_ALL    Type: COMPARE    Sessions: 4    Step options: {"COMPARE_MAX_DIFF": 20, "COMPARE_MAX_ROWS": 50, "COMPARE_TRUNCATE_DIFF": true}
Status: 'Completed'    Start: 2022-05-18 08:14:37   End:2022-05-18 08:14:37   Elapse: 00:00:00           
Steps Total: 26  Completed: 26 (100%/100%)
        'Completed' steps                     Estim.Cost Sess.       Start               End             Elapse      Main indicator
END_PG's db                                            1   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.011980
phil's schema3.phil's seq\1                           10   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.012576              0
phil's schema3.myTbl2\_col21_seq                      10   4 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007539              0
myschema2.myTbl3_col31_seq                            10   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007256              0
myschema2.MYSEQ2                                      10   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007361              0
phil's schema3.mytbl4                                  8   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007039              0
myschema2.myseq1                                      10   4 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.009654              0
myschema1.myTbl3_col31_seq                            10   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.009515              0
myschema1.mytbl2b_col20_seq                           10   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.009682              0
phil's schema3.myTbl2\                                 8   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007275              0
myschema1.mytbl2                                      48   4 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007677              0
myschema1.mytbl2b                                     56   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007405              0
myschema2.mytbl2                                      72   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007109              0
myschema2.mytbl8                                       8   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.006675              0
myschema2.mytbl7                                     360   4 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007857              0
myschema2.myTbl3                                     512   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007544              0
myschema1.mytbl1                                     512   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.007228              0
myschema1.MYTBL4                                       8   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.006838              0
phil's schema3.phil's tbl1                          1584   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.016433              0
myschema2.mytbl1.4                                  5608   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.016075              0
myschema1.myTbl3                                       8   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.015517              0
myschema2.mytbl1.3                                  5608   4 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.040619              0
myschema2.mytbl1.2                                  5608   3 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.027410              0
myschema2.mytbl1.1                                  5608   2 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.035835              0
myschema2.mytbl4                                       0   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.034206              0
INIT_PG's db                                           1   1 2022-05-18 08:14:37 2022-05-18 08:14:37 00:00:00.013162
```

For the same batch run, the web client run details page looks like

![Web client run details page](../img/webClientRunDetails.png)
