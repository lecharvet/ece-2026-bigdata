# Lab PySpark silver - results

- Silver (Spark): users 50, orders 2829 (2832 with 3 simulated duplicates, 2829 after row_number() dedup); DataFrame API == Spark SQL.
- Outputs: Parquet in silver/spark/, Iceberg tables lab.silver.stg_users / stg_orders (hadoop catalog); only the metadata/ directories are committed (warehouse/).
- dbt vs Spark: same counts (2829 orders, quantity sum 8615, 50 users), EXCEPT in both directions = 0. Schema difference: ordered_at is TIMESTAMP WITH TIME ZONE (dbt/DuckDB) vs TIMESTAMP (Spark); UUIDs are VARCHAR in both.
- Ex1: 5 invalid birthdates (birthdate later than first order), identical in DataFrame API and SQL.
- Ex2 (10k rows, 3 runs): regexp_extract ~0.57-0.95 s, pandas_udf ~0.72-2.6 s, python UDF ~0.77-3.6 s. Built-in functions stay in the JVM and are optimized by Catalyst; the first UDF run pays the Python worker startup.
- Ex3: without cache ~0.33-0.51 s per count, with cache 1.24 s (fills the cache, 230 KB, 1 partition) then ~0.25 s.
- Ex4: Iceberg append -> 2830 rows, 2 snapshots (overwrite, append); VERSION AS OF the first snapshot -> 2829. A Parquet overwrite would have lost the previous state.
- Environment note: the vscode-pyspark pod is capped at 2 GB / 2 CPU; Spark had to run with local[2], 800 MB driver, 4 shuffle partitions.
