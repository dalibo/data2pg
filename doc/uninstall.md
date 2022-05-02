# How to uninstall Data2Pg

## Completing a database migration

Once a database is definitely migrated, its Data2Pg components can be dropped.

First log on the target database with a SUPERUSER role and execute the following SQL commandes:

```sql
SELECT drop_migration(<migration_name>);
DROP EXTENSION data2pg;
DROP SCHEMA data2pg CASCADE;
```

The `drop_migration()`function drops:

  * the *FOREIGN TABLEs* et the schemas that hold them;
  * the *USER MAPPING* and the *FOREIGN SERVER* associated to the migration.

However, it kepts the *FOREIGN DATA WRAPPER* object that may be used for other purposes. If it needs to be dropped, just type:

```sql
DROP EXTENSION <fdw_extension_name> CASCADE;
```

It the `data2pg` role is not used anymore by other databases of the instance it can be dropped also:

```sql
DROP ROLE data2pg;
```

## Completing the migration program

If all migration works are over, the Data2Pg administration database can be dropped. This can be done with the shell commands:

```shell
dropdb data2pg -h ... -p ... -U postgres
dropuser data2pg -h ... -p ... -U postgres
```

If the Web client has been installed, it can also be uninstalled from the web server.
