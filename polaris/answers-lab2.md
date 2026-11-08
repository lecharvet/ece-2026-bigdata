# Lab 09.2 - Govern Iceberg tables with Polaris - results

Date: 2026-11-08. Catalog `user-a-charvet-ece`. Spark 4.1.1 in `local[2]` (driver 800m),
Iceberg runtime 1.11.0. Scripts in `polaris/lab2/`, filtered outputs in `polaris/lab2/output/`.

## Prerequisite (deviation)

The bucket only contained `bronze/`, and the Spark lab had named its tables
`lab.silver.stg_users` / `lab.silver.stg_orders`. `00_rebuild_silver.py` rebuilt
`lab.silver.users` (50 rows) and `lab.silver.orders` (2829 rows, quantity 8615) from
bronze with the transformations of `spark_labs/silver_part1.py`. Our bronze data has
2829 orders, not the 5,000 shown in the lab text.

## SparkSession with the Polaris catalog

- `spark.hadoop.fs.s3.impl` = `org.apache.hadoop.fs.s3a.S3AFileSystem`.
- `SHOW NAMESPACES IN lab` -> `silver`; `SHOW NAMESPACES IN polaris` -> 0 rows.
- Which component answered? `lab` is answered by Spark itself: the Hadoop catalog lists
  directories under `s3a://user-a-charvet-ece/warehouse`, no server involved. `polaris`
  is answered by the Polaris server through the REST API (`http://polaris:8181/api/catalog`).
  The results differ (`silver` vs empty), which shows two different sources.
- Where is the credential used? It is set in the Spark session, and sent only to Polaris
  in an OAuth2 `client_credentials` request; the returned token is attached to each REST
  call. It is not used for the bucket: files are read with the S3A STS keys (no
  credential vending, `HadoopFileIO`).

## Register the Silver table

- `version-hint.text` = 1, current file `v1.metadata.json` (1928 B).
- `register_table` -> (8106780798736354479, 2829 records, 1 data file);
  `count(*)` on `polaris.silver.orders` = 2829; one snapshot, `overwrite`, 2026-11-08 12:45:41.
- What did it write? Only an entry in Polaris (table name -> metadata file, stored in
  Polaris' PostgreSQL). Objects under `warehouse/silver/orders/`: 5 before, 5 after.
- Why not write through `lab` anymore? Each catalog keeps its own pointer. A commit through
  `lab` writes `v2.metadata.json` and moves `version-hint.text`, while Polaris still
  points to v1: Polaris readers see a stale table, a later commit through Polaris starts
  from v1 and the histories diverge (lost updates), and Polaris RBAC is bypassed.

## Create tables as `dev`

- `SHOW TABLES IN polaris.silver` -> `orders`; `polaris.silver.users` created from
  `lab.silver.users`: 50 rows.
- New objects: `silver/users/data/00000-3-...parquet`, `metadata/00000-62c94108-...metadata.json`,
  `00001-551d1a05-...metadata.json`, `2ead71b7-...-m0.avro`, `snap-4225236112650687740-...avro`.
- Where and why? In `s3://user-a-charvet-ece/warehouse/silver/users/`, i.e.
  `<default-base-location>/<namespace>/<table>`: the table was created without LOCATION, so
  Polaris derived it from the catalog base location. It is the same directory as the Hadoop
  table `lab.silver.users` (`v1.metadata.json`, `version-hint.text`); both sets of files
  coexist, Polaris names its files `0000N-<uuid>.metadata.json`.
- Registered vs created: for `orders`, data and metadata were written by another catalog and
  Polaris only adopted an existing metadata file. For `users`, Polaris chose the location
  and every metadata commit goes through Polaris.

## Schema change as `dev`

- `DESCRIBE`: 11 columns, the last one `country string`.
- Recorded in a new file, `00001-551d1a05-...metadata.json`; not the file used by
  `register_table` (`orders/metadata/v1.metadata.json`). No data file was rewritten.
- Privilege: Polaris authorizes the operation `ADD_TABLE_SCHEMA` (name in the analyst
  error) with privilege `TABLE_ADD_SCHEMA`. In Polaris 1.8.0 (`PolarisAuthorizerImpl`) it
  is also satisfied by `TABLE_WRITE_PROPERTIES`, `TABLE_WRITE_DATA`, `TABLE_MANAGE_STRUCTURE`,
  `TABLE_FULL_METADATA` or `CATALOG_MANAGE_CONTENT`; `dev` has the last one.

## Read as the analyst

- `SHOW TABLES IN polaris.silver` worked (orders, users): in Polaris 1.8.0, `TABLE_LIST`
  is implied by `TABLE_READ_DATA` and `TABLE_READ_PROPERTIES`, which `catalog_reader` has.
- Orders by product: cookie 466, drink 484, bread 458, brioche 468, donut 462,
  croissant 491 (total 2829).
- INSERT: `ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles
  '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not
  authorized for op ADD_TABLE_SNAPSHOT`.
- `ALTER TABLE ... ADD COLUMN phone`: same error, op `ADD_TABLE_SCHEMA`.
- Missing privilege for INSERT: operation `ADD_TABLE_SNAPSHOT` needs `TABLE_ADD_SNAPSHOT`,
  normally granted through `TABLE_WRITE_DATA`. `catalog_reader` provides
  `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES`.
- As `dev` afterwards: still 1 snapshot (8106780798736354479), 2829 rows, and no new
  object under `warehouse/silver/` (listing diff empty). The failed INSERT created nothing.

## Commit the Iceberg table metadata

`aws s3 sync` downloaded 15 files into `./warehouse/`; only the `metadata/` directories of
`silver/orders` and `silver/users` are committed (no Parquet data).

## Exercises

1. Drop `country` (as `dev`): before, 1 snapshot (4225236112650687740) and 2 metadata
   files; after, still 1 snapshot and 3 metadata files (new `00002-b0294a17-...metadata.json`);
   `DESCRIBE` shows 10 columns. It is a metadata operation: Iceberg tracks columns by field
   id, so dropping one only writes a new schema in a new metadata file. Data files are not
   rewritten and no snapshot is created, readers just ignore the old column.
2. `ex2_silver_reader.sh` (as root, every call HTTP 201): catalog role `silver_reader` with
   `NAMESPACE_LIST`, `TABLE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_READ_PROPERTIES`,
   `TABLE_READ_DATA` on namespace `silver` only; principal role `silver_analyst`;
   principal `silver_tester`. As `dev`: namespace `polaris.gold` and table
   `gold.orders_by_product` (6 rows, e.g. croissant 491 orders / 1523 units).
   As `silver_tester`: `SHOW TABLES IN silver` OK, `count(*)` of `silver.orders` = 2829 OK;
   `SHOW TABLES IN gold` -> Forbidden `LIST_TABLES`; reading the gold table -> Forbidden
   `LOAD_TABLE`; INSERT into `silver.orders` -> Forbidden `ADD_TABLE_SNAPSHOT`.

## Cleanup

- Dropped `polaris.silver.users`, then `polaris.gold.orders_by_product` (created by
  exercise 2, needed to drop the namespace), then namespace `gold`. Left: namespace
  `silver` with `orders` (kept for the Trino lab).
- `DROP TABLE` without PURGE only removes the catalog entry: 21 objects under `warehouse/`
  before and after.
- Not removed (not asked by the lab): `silver_tester`, `silver_analyst`, `silver_reader`.

## What Polaris cannot enforce on this storage

There is no credential vending: every session reads and writes files with the student's
own S3 keys. Polaris only controls catalog operations (load, commit). Anyone holding the
bucket keys, including the analyst session, could still read or overwrite the Parquet and
metadata files directly in S3, outside Polaris RBAC.
