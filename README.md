Data2Pg
=======

Data2Pg is a tools framework that helps migrating non-PostgreSQL database contents into PostgreSQL.

Version: 0.3

License
-------

Data2Pg is distributed under the GNU General Public License.

Objectives
----------

The main goal of Data2Pg is to help in database migration projects moving data from a non-PostgreSQL RDBMS into PostgreSQL. Three functions are available:

 * check source database contents before migrating them into PostgreSQL ;
 * copy database contents from various databases into PostgreSQL ;
 * compare non PostgreSQL databases and their equivalent Postgres one to help non regression test campaigns.

These 3 functions use the same infrastructure. The source database is directly accessed from PostgreSQL using a Foreign Data Wrapper.

Architecture
------------

The Data2Pg framework has several components:

  * an extension installed into the target Postgres database
  * a dedicated data2pg database that is generaly installed in a central point of the migration project
  * a scheduler that performs the requested actions
  * a monitor tool that allows to report the migration progress in real time or display detailed information of past migrations.

How to install and use Data2Pg
------------------------------

Go to the extension directory and install files

```sh
cd ext/
make PG_CONFIG=<path/to/pg_config> install
```

On the administration database which stores migrations projects, install the `data2pg_admin` extension.

```sql
CREATE EXTENSION data2pg_admin SCHEMA data2pg;
```

On target databases, deploy the `data2pg` extension.

```sql
CREATE EXTENSION data2pg SCHEMA data2pg;
```

The scheduler and the monitor clients are written in perl and use the DBI and DBD::Postgres modules. These components may need to be installed on your machine.

If you want to use the web client (it is optional), the data2pg/www subdirectory must be accessible for a web server with php activated. A data2pg/www/conf/config.inc.php must be created using the config.inc.php-dist template.

How to test Data2Pg
-------------------

The following steps must be executed:

  * Initialize the administration data2pg database by executing the `data2pg_init_db.sh` shell script (once the environment variables adjusted)
  ```
  bash data2pg_init_db.sh
  ```
  * Initialize both postgres source and target databases by executing the `test_pg/1-init.sh` shell script (once the environment variables adjusted)
  ```
  bash test_pg/1-init.sh
  ```
  * Initialize the data2pg extension into the target database by executing the `data2pg_init_schema.sh` shell script (once the environment variables adjusted)
  ```
  bash data2pg_init_schema.sh
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
  perl data2pg.pl --conf test_pg/batch_compare.conf --action run
  ```
  * Run the monitor with commands like
  ```
  export PGHOST=localhost
  perl data2pg_monitor.pl -r <run_id>
  ```

Contributing
------------

Any contribution on the project is welcome.

Support
-------

<TO BE DEFINED>
