# Back as dev: did the failed INSERT of the analyst create a snapshot?
from session import build, p, q

spark = build("dev")
q(spark, "orders snapshots",
  "SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")
q(spark, "count polaris.silver.orders", "SELECT count(*) FROM polaris.silver.orders")
p("DONE")
spark.stop()
