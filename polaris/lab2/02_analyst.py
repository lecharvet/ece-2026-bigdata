# Section: read as the analyst. Writes are expected to be Forbidden.
from session import build, p, q

spark = build("analyst")
q(spark, "SHOW TABLES IN polaris.silver", "SHOW TABLES IN polaris.silver")
q(spark, "orders by product",
  "SELECT product, count(*) FROM polaris.silver.orders GROUP BY product")
q(spark, "INSERT as analyst",
  "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
q(spark, "ALTER as analyst", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
p("DONE")
spark.stop()
