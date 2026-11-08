# Exercise 1: drop the `country` column, compare snapshots and metadata files.
# Exercise 2 (dev part): create the gold namespace and one gold table.
from session import build, p, q

spark = build("dev")
SNAP = "SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots"
LOG = "SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries"
q(spark, "ex1 users snapshots BEFORE", SNAP)
q(spark, "ex1 users metadata log BEFORE", LOG)
q(spark, "ex1 DROP COLUMN country", "ALTER TABLE polaris.silver.users DROP COLUMN country")
q(spark, "ex1 users snapshots AFTER", SNAP)
q(spark, "ex1 users metadata log AFTER", LOG)
q(spark, "ex1 DESCRIBE users", "DESCRIBE polaris.silver.users")

q(spark, "ex2 CREATE NAMESPACE gold", "CREATE NAMESPACE IF NOT EXISTS polaris.gold")
q(spark, "ex2 CREATE gold.orders_by_product", """
    CREATE TABLE polaris.gold.orders_by_product AS
    SELECT product, count(*) AS orders, sum(quantity) AS quantity
    FROM polaris.silver.orders GROUP BY product""")
q(spark, "ex2 dev reads gold", "SELECT * FROM polaris.gold.orders_by_product ORDER BY product")
p("DONE")
spark.stop()
