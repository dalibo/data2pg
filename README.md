Data2Pg
=======

Data2Pg is a tools framework that helps migrating non-PostgreSQL database contents into PostgreSQL.

*Please note:*

Last status of this project is *Alpha* and is now *inactive*, so please use it in an isolated, controlled environment and for the Web interface, please consider adding authentication via .htaccess or other means to mitigate security issue. 

Version: 0.7 (alpha and currently inactive)

# License

Data2Pg is distributed under the GNU General Public License.

# Objectives

The main goal of Data2Pg is to help in database migration projects moving data from a non-PostgreSQL RDBMS into PostgreSQL. Three functions are available:

 * **discover** source database contents before migrating them into PostgreSQL ;
 * **copy** database contents from various databases into PostgreSQL ;
 * **compare** non PostgreSQL databases and their equivalent Postgres one to help non regression test campaigns.

These 3 functions use the same infrastructure. The source database is directly accessed from PostgreSQL using a Foreign Data Wrapper.

# Architecture and concepts

See [Data2Pg architecture and concepts](doc/architectureConcept.md).

# How to install Data2Pg

See [Install Data2Pg](doc/install.md).

# How to configure migrations

See [Configure migrations](doc/configureMigration.md).

# How to start the scheduler

See [Start the scheduler](doc/startScheduler.md).

# How to get detailed results

See [Get detailed results](doc/getDetailedResults.md).

# How to monitor runs

See [Monitor runs](doc/monitorRuns.md).

# How to test Data2Pg

See [Test Data2Pg](doc/test.md).

# How to uninstall Data2Pg

See [Uninstall Data2Pg](doc/uninstall.md).

# Contributing

Any contribution on the project is welcome.

