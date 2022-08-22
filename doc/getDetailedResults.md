# How to get detailed results

For some batch types, additional results can be examined.

## Check steps of COPY batches

Table content checks record the computed aggregates like the number of rows into a `counter` table located into the *data2pg* extension schema, named `data2pg` by default. Its structure is:

   * cnt_schema             : The schema of the target table
   * cnt_table              : The target table name
   * cnt_database           : The examined database ('S' for Source or 'D' for Destination)
   * cnt_counter            : The counter name
   * cnt_value              : The counter value
   * cnt_timestamp          : The transaction timestamp of the table check

When several checks are performed for the same table, the last one overrides the previous results.

In order to ease the detection of discrepancies, a view is also available, named `counter_diff` and located into the *data2pg* extension schema. It only reports counters for which the values are different between both databases. Its structure is:

   * cnt_schema             : The schema of the target table
   * cnt_table              : The target table name
   * cnt_counter            : The counter name
   * cnt_source_value       : The counter value on the source database
   * cnt_dest_value         : The counter value on the destination database
   * cnt_source_ts          : The transaction timestamp of the source table check
   * cnt_dest_ts            : The transaction timestamp of the destination table check

## DISCOVER batches

Batches of type DISCOVER record their data into 3 tables located into the *data2pg* extension schema:

   * `discovery_table`      : contains 1 row per analysed table. It may be interested to be examined to get the the last analysis timestamp and the number of processed rows
   * `discovery_column`     : contains 1 row per analysed table column
   * `discovery_message`    : contains generated messages. It is the most interesting table.

When several analysis are performed for the same table, the last one overrides the previous results.

The `discovery_message` table has the following structure:

   * dscv_schema            : The name of the schema holding the foreign table
   * dscv_table             : The analyzed foreign table name
   * dscv_column            : The column associated to the advice, or NULL for table level piece of advice
   * dscv_column_num        : The column rank in the pg_attribute table (i.e. its attnum)
   * dscv_msg_type          : The message type (A = Advice, C = Corruption, E = Error W = Warning)
   * dscv_msg_code          : A coded representation of the message 'TOO_SHORT_INTEGER' for the message "column ... cannot hold smallest or largest values"
   * dscv_msg_text          : The textual representation of the message that can be infered from collected data

The **Error** messages reports the following cases:

   * NULL_VALUES: The source column contains NULL but the target column is declared NOT NULL.
   * x00_CHARACTERS: The source column contains x00 characters that arenot allowed in postgres textual values.
   * TOO_SHORT_STRING: The textual target column is too small to receive the largest data.
   * TOO_SHORT_BINARY: The largest binary data to store into the BYTEA column is over 1 Go
   * TOO_SHORT_INTEGER: The target column is of type SMALLINT, INTEGER or BIGINT but some data content do not fit the values limits.
   * TOO_SHORT_NUMERIC: The target column is of type NUMERIC but the integer part is not large enough.
   * MONEY: The target column is of type MONEY, but the integer part does't fit the type limits.

The **Corruption** messages reports the following cases:

   * TIME: The target column is of type DATE but needs to hold time components. It should be of type TIMESTAMP to not loose the time components.
   * NON_INTEGER: The target column is of type SMALLINT, INT or BIGINT, but needs to receive non integer values. The fractional parts will be lost.
   * TOO_SHORT_SCALE: The target column is of type NUMERIC or MONEY, but has a too short scale. Some fractional parts will be truncated.
   * PRECISION_LOSS: The target column is of type REAL ou DOUBLE PRECISION, but the source content copy will lead to precision loss.

The **Advice** messages reports the following cases:

   * CONSIDER_NOT_NULL: The column does not contain any NULL value. It could perhaps be declared NOT NULL (if the data life cycle allows it).
   * CONSIDER_SHORTER_STRING: The column max size is greater than 10 and is more than twice as large as the largest content. It could perhaps be declared shorter.
   * CONSIDER_LO: The column contains binary data over 50 Mb. It could perhaps be handled as LARGE OBJECT.
   * CONSIDER_DATE: The target column is of type TIMESTAMP but could be of type DATE.
   * CONSIDER_INTEGER: The target column is a BIGINT but the lowest and largest values could be hold by an INTEGER.

The **Warning** messages reports the following cases:

   * EMPTY_TABLE: The table is empty.
   * NOT_ALL_ROWS: The table has un-analyzed rows, because of the DISCOVER_MAX_ROWS option set at batch run.
   * SKIPPED_COLUMNS: The latest columns of the table have not been analyzed (because the number of columns exceeds what can be analysed by a single SQL statement on the source database).
   * NOT_ANALYZED: The column has not been analysed (because it is populated by an expression or its type is not handled by the analysis).
   * ONLY_NULL: The column contains nothing but NULL. So nothing can be deducted.

## COMPARE batches

For batches of type COMPARE, the steps record the detected differences between source and destination tables or sequences content into a `content_diff` table, located into the *data2pg* extension schema.  It has the following structure:

   * diff_timestamp          : The transaction timestamp of the relation comparison
   * diff_schema             : The name of the destination schema
   * diff_relation           : The name of the destination table or sequence used for the comparison
   * diff_rank               : A difference number for the relation
   * diff_database           : The database the rows comes from ('S' for Source or 'D' for Destination)
   * diff_key_cols           : The JSON representation of the table key, for rows that are different between both databases
   * diff_other_cols         : The JSON representation of the table columns not in key, for rows that are different between both databases or the sequence characteristics that are different between both databases

Several comparisons results can be registered for the same table. Diff data are accumulated into the *content_diff* table until the *COMPARE_TRUNCATE_DIFF* step option is set to *TRUE* at COMPARE batch start.
