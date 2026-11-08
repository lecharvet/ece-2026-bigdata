# Cleanup of the lab, as dev. silver.orders stays registered (Trino lab).
# gold.orders_by_product was created by exercise 2, so it is dropped first.
from session import build, p, q

spark = build("dev")
q(spark, "DROP silver.users", "DROP TABLE IF EXISTS polaris.silver.users")
q(spark, "DROP gold.orders_by_product", "DROP TABLE IF EXISTS polaris.gold.orders_by_product")
q(spark, "DROP NAMESPACE gold", "DROP NAMESPACE IF EXISTS polaris.gold")
q(spark, "SHOW NAMESPACES IN polaris", "SHOW NAMESPACES IN polaris")
q(spark, "SHOW TABLES IN polaris.silver", "SHOW TABLES IN polaris.silver")
p("DONE")
spark.stop()
