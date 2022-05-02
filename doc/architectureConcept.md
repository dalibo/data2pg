# Data2Pg architecture and concepts

## Architecture

The Data2Pg framework has several components:

  * an extension installed into the target Postgres database
  * a dedicated data2pg database that is generaly installed in a central point of the migration project
  * a scheduler that performs the requested actions
  * a monitor tool that reports the migration progress in real time and display detailed information about past migrations
  * a web client that may be used to ease the scheduler run and the batches monitoring.

![Data2pg architecture](./img/architecture.png)

## Concepts

A few concepts must be defined:

  * Database: a target PostgreSQL database to migrate
  * Migration: the global migration operation of a Database, feeded with data from a single source database located on a single foreign server and accessed by a single user mapping
  * Batch: a set of tables and sequences to process; a Migration may consist in several Batches
  * Batch Run: a scheduler launch to execute one Batch
  * Step: an elementary operation to process a table, a sequence, etc; it executes a single SQL function on the target Database into a single transaction
  * Working plan: the set of Steps that constitutes a Batch
  * Session: a scheduler connection on the target Database to execute Steps

