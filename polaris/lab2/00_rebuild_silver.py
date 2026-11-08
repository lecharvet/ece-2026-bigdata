# Prerequisite: rebuild lab.silver.users / lab.silver.orders (Hadoop catalog)
# from bronze, same transformations as spark_labs/silver_part1.py.
import os
from pyspark.sql import functions as F, Window
from session import build, p, q

spark = build("dev")
B = os.environ["LAB_BUCKET_NAME"]
rd = lambda t: (spark.read.option("header", True).option("multiLine", True)
                .option("quote", '"').option("escape", '"')
                .csv(f"s3a://{B}/bronze/{t}.csv"))
users, orders = rd("users"), rd("orders")
nz = lambda c: F.when(c != "", c)
addr = F.regexp_replace("address", r"\r?\n|\\n", "|")
rest = F.trim(F.split(addr, r"\|")[1])
pat = r"^(.*?),?\s*([A-Z]{2})\s+(\d{5})$"
stg_users = users.select(
    F.trim("uuid").alias("user_id"), F.trim("username").alias("username"),
    F.trim("name").alias("name"), F.upper(F.trim("sex")).alias("sex"),
    F.lower(F.trim("mail")).alias("email"),
    F.col("birthdate").cast("date").alias("birthdate"),
    F.trim(F.split(addr, r"\|")[0]).alias("street"),
    nz(F.trim(F.regexp_extract(rest, pat, 1))).alias("city"),
    nz(F.regexp_extract(rest, pat, 2)).alias("state"),
    nz(F.regexp_extract(rest, pat, 3)).alias("zip")).filter("user_id is not null")
stg_o = orders.select(
    F.trim("uuid").alias("order_id"), F.trim("user_uuid").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product")).filter("order_id is not null")
w = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = stg_o.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
q(spark, "lab.silver.users count", "SELECT count(*) FROM lab.silver.users")
q(spark, "lab.silver.orders count/quantity",
  "SELECT count(*), sum(quantity) FROM lab.silver.orders")
p("DONE")
spark.stop()
