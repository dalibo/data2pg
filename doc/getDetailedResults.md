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

Batches of type DISCOVER records their data into 3 tables located into the *data2pg* extension schema:

   * `discovery_table`      : contains 1 row per analysed table. It may be interested to be examined to get the the last analysis timestamp and the number of processed rows
   * `discovery_column`     : contains 1 row per analysed table column
   * `discovery_advice`     : contains generated pieces of advice. It is the most interesting table.

When several analysis are performed for the same table, the last one overrides the previous results.

The `discovery_advice` has the following structure:

   * dscv_schema            : The name of the schema holding the foreign table
   * dscv_table             : The analyzed foreign table name
   * dscv_column            : The column associated to the advice, or NULL for table level piece of advice
   * dscv_column_num        : The column rank in the pg_attribute table (i.e. its attnum)
   * dscv_advice_type       : The advice type (W = Warning, A = Advice)
   * dscv_advice_code       : A coded representation of the advice, like 'INTEGER' for the message "column ... could be an INTEGER"
   * dscv_advice_msg        : The textual representation of the piece of advice that can be infered from collected data

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

