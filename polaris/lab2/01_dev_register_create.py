# Sections: SparkSession, register the Silver table, create tables and
# schema change, all as principal `dev`. argv[2] = metadata file name.
import os
import sys
from session import build, p, q

spark = build("dev")
B = os.environ["LAB_BUCKET_NAME"]
META = sys.argv[2]

q(spark, "SHOW NAMESPACES IN lab", "SHOW NAMESPACES IN lab")
q(spark, "SHOW NAMESPACES IN polaris", "SHOW NAMESPACES IN polaris")

q(spark, "CREATE NAMESPACE polaris.silver", "CREATE NAMESPACE IF NOT EXISTS polaris.silver")
q(spark, "register_table", f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{B}/warehouse/silver/orders/metadata/{META}'
    )""")
q(spark, "count polaris.silver.orders", "SELECT count(*) AS orders FROM polaris.silver.orders")

q(spark, "SHOW TABLES IN polaris.silver", "SHOW TABLES IN polaris.silver")
q(spark, "orders snapshots",
  "SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots")

stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()
q(spark, "count polaris.silver.users", "SELECT count(*) AS users FROM polaris.silver.users")

q(spark, "ALTER users ADD country", "ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
q(spark, "DESCRIBE polaris.silver.users", "DESCRIBE polaris.silver.users")
q(spark, "users metadata_log_entries",
  "SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries")
p("DONE")
spark.stop()
