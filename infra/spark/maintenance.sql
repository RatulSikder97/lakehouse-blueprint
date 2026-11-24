-- Heavy-duty Iceberg maintenance with Spark stored procedures.
-- Equivalent to `make maintain` (Trino) but with sort-aware compaction and manifest rewrites,
-- which is what you would schedule on EMR / Glue for large tables.

CALL lakehouse.system.rewrite_data_files(
  table => 'bronze.yellow_trips',
  strategy => 'binpack',
  options => map('min-input-files', '2', 'target-file-size-bytes', '134217728')
);

CALL lakehouse.system.rewrite_position_delete_files(table => 'bronze.yellow_trips');

CALL lakehouse.system.rewrite_manifests(table => 'bronze.yellow_trips');

CALL lakehouse.system.expire_snapshots(table => 'bronze.yellow_trips', retain_last => 5);

SELECT 'bronze.yellow_trips' AS table_name, count(*) AS snapshots FROM lakehouse.bronze.yellow_trips.snapshots;

SELECT source_month, count(*) AS trips FROM lakehouse.bronze.yellow_trips GROUP BY source_month ORDER BY source_month;
