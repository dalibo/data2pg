# How to test Data2Pg

Just for testing purpose, the proposed migration migrates a source PostgreSQL database into another PostgreSQL target database. The following steps must be executed:

  * Initialize the administration data2pg database by executing the `data2pg_init_db.sh` shell script (once the environment variables adjusted)
  ```
  bash data2pg_init_db.sh
  ```
  * Initialize both postgres source and target databases by executing the `test_pg/1-init.sh` shell script (once environment variables adjusted)
  ```
  bash test_pg/1-init.sh
  ```
  * Initialize the data2pg extension into the target database by executing the `data2pg_init_schema.sh` shell script (once environment variables adjusted)
  ```
  bash data2pg_init_extension.sh
  ```
  * Configure the `migration` by executing the `test_pg/3-configure.sh` shell script (once the environment variables adjusted)
  ```
  bash test_pg/3-configure.sh
  ```
  * Run the scheduler with commands like
  ```
  export PGHOST=localhost
  perl data2pg.pl --conf test_pg/batch0.conf --action run
  perl data2pg.pl --conf test_pg/batch1.conf --action run
  perl data2pg.pl --conf test_pg/batch_tables_check.conf --action run
  perl data2pg.pl --conf test_pg/batch_compare.conf --action run
  ```
  * Run the monitor with commands like
  ```
  export PGHOST=localhost
  perl data2pg_monitor.pl -r <run_id>
  ```
