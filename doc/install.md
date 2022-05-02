# How to install Data2Pg

The Data2Pg installation can be splitted into 4 steps.

## Installing the software

As Data2Pg uses a Foreign Data Wrapper to access data located on the source database, a Foreign Data Wrapper extension needs to be installed into the target PostgreSQL instance and database. It depends on the source database type. Refer to its documentation for its installation.

To install both `data2pg` and `data2pg_admin` extensions, go to the extension directory and install files.

```sh
cd ext/
make PG_CONFIG=<path/to/pg_config> install
```

The scheduler and the monitor clients are written in perl and use the DBI and DBD::Postgres modules. These components may need to be installed on your machine.

## Creating the administration database

Data2Pg needs its own adminstration database. This database may be created into the same PostgreSQL instance as a database to populate. Its size and SQL load are very low. It will be dropped once the migration project will be completed.

This database contains a `target_database` table that describes all PostgreSQL databases concerned by the migration project. This table can be fed by the Web client. It can also be populated at creation time by loading a `target_database.dat` file. In this case, the provided template file must be adjusted. The first line is a header and must be left as is. Each subsequent lines describes a target database. It contains the following fields:

   * tdb_id : a Data2Pg target database identifier (this name will be used for Data2Pg operations)
   * tdb_host : the IP address to reach the PostgreSQL target database
   * tdb_port : the IP port to reach the PostgreSQL target database
   * tdb_dbname : the PostgreSQL database name
   * tdb_description : a textual description of the database (optional)
   * tdb_locked : a flag to protect the database against unattended data copy ; set it to FALSE to run batches of type COPY.

The administration database can be created using the supplied `data2pg_init_db.sh` shell script. Before running it, adjust the environment variables defined at the beginning of the script (see below the note about the *data2pg* role password). Then type:

```sh
./data2pg_init_db.sh
```

The script:

   * creates a database named `data2pg`;
   * creates a role named `data2pg`, if it does not exist yet;
   * creates the `data2pg_admin` extension inside the `data2pg` database;
   * loads the file that populates the `target_database` table.

## Installing the Web client

In order to install the optional Web client, the `data2pg/www` subdirectory must be accessible for a web server with php activated.

Then the `data2pg/www/conf/config.inc.php` must be created using the `config.inc.php-dist` template.

## Creating the data2pg extension

On each target database, the `data2pg` extension must be created using the supplied `data2pg_init_extension.sh` shell script. Before running it, adjust the environment variables defined at the beginning of the script. Then type:

```sh
./data2pg_init_extension.sh
```

The script:

   * creates a role named `data2pg`, if it does not already exist in the instance;
   * creates the `data2pg` extension inside the target database;
   * loads custom components that may be needed for specific migration steps, by executing the `data2pg_addons.sql` SQL script file.

## Managing the data2pg role password

Most operations accessing PostgreSQL databases use a dedicated *data2pg* role. In this repo files, the password is set to `gp2atad`. IT HAS TO BE CHANGED!

This concerns the `CREATE ROLE` statement in both *data2pg_init_admin_db.sh* and *data2pg_init_extension.sh* script files.

The *data2pg.pl* scheduler only relies on the `.pgpass` file content to reach the Data2Pg administration database as well as the target databases. The *data2pg_monitor.pl* monitor clients also uses the `.pgpass` file content to reach the Data2Pg administration database (it never logs on the target databases). So the `~/.pgpass` file of the OS account used to run these commands must be adjusted accordingly.

The web client only accesses the Data2Pg administration database. It uses parameters set into its `config.inc.php` configuration file, including the *data2pg* password (see above).
