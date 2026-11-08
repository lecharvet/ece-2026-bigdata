# Exercise 2: principal silver_tester -> silver_analyst -> silver_reader
# (privileges granted on namespace `silver` only).
from session import build, p, q

spark = build("silver_tester")
q(spark, "SHOW TABLES IN polaris.silver", "SHOW TABLES IN polaris.silver")
q(spark, "read silver.orders", "SELECT count(*) FROM polaris.silver.orders")
q(spark, "SHOW TABLES IN polaris.gold", "SHOW TABLES IN polaris.gold")
q(spark, "read gold.orders_by_product", "SELECT * FROM polaris.gold.orders_by_product")
q(spark, "write silver.orders",
  "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
p("DONE")
spark.stop()
