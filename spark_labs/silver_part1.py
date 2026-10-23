import os
from pyspark.sql import SparkSession, functions as F, Window

B = os.environ["LAB_BUCKET_NAME"]
ep = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
ep = ep if ep.startswith("http") else "https://" + ep

spark = (SparkSession.builder.master("local[*]").appName("lab-silver")
    .config("spark.hadoop.fs.s3a.endpoint", ep)
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{B}/warehouse")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
def p(*a): print("##", *a, flush=True)

p("ETAPE 3 : bronze")
rd = lambda t: (spark.read.option("header", True).option("multiLine", True)
                .option("quote", '"').option("escape", '"').csv(f"s3a://{B}/bronze/{t}.csv"))
users, orders = rd("users"), rd("orders")
p("users:", users.count(), "orders:", orders.count())
p("users cols:", users.columns, "| orders cols:", orders.columns)

p("ETAPE 4 : silver users")
nz = lambda c: F.when(c != "", c)
addr = F.regexp_replace("address", r"\r?\n|\\n", "|")
rest = F.trim(F.split(addr, r"\|")[1])
pat = r"^(.*?),?\s*([A-Z]{2})\s+(\d{5})$"
stg_users = users.select(
    F.trim("uuid").alias("user_id"), F.trim("username").alias("username"),
    F.trim("name").alias("name"), F.upper(F.trim("sex")).alias("sex"),
    F.lower(F.trim("mail")).alias("email"), F.col("birthdate").cast("date").alias("birthdate"),
    F.trim(F.split(addr, r"\|")[0]).alias("street"),
    nz(F.trim(F.regexp_extract(rest, pat, 1))).alias("city"),
    nz(F.regexp_extract(rest, pat, 2)).alias("state"),
    nz(F.regexp_extract(rest, pat, 3)).alias("zip")).filter("user_id is not null")
p("stg_users:", stg_users.count(), "| city null:", stg_users.filter("city is null").count(), "| zip null:", stg_users.filter("zip is null").count())
for r in stg_users.limit(2).collect(): p(tuple(r))

p("ETAPE 5 : silver orders + doublons simules")
stg_o = orders.select(
    F.trim("uuid").alias("order_id"), F.trim("user_uuid").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product")).filter("order_id is not null")
dup = stg_o.unionByName(stg_o.limit(3))
w = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = dup.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
p("orders:", stg_o.count(), "avec doublons:", dup.count(), "apres dedup:", stg_orders.count())
dup.createOrReplaceTempView("orders_dup")
sql_orders = spark.sql("""SELECT order_id, user_id, ordered_at, quantity, product FROM (
  SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) rn FROM orders_dup) WHERE rn = 1""")
same = stg_orders.exceptAll(sql_orders).count() == 0 and sql_orders.exceptAll(stg_orders).count() == 0
p("DataFrame == SQL:", same)

p("ETAPE 6 : parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{B}/silver/spark/stg_users")
stg_orders.write.mode("overwrite").parquet(f"s3a://{B}/silver/spark/stg_orders")
p("relu users:", spark.read.parquet(f"s3a://{B}/silver/spark/stg_users").count(),
  "orders:", spark.read.parquet(f"s3a://{B}/silver/spark/stg_orders").count())

p("ETAPE 7 : iceberg")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.stg_users").createOrReplace()
stg_orders.writeTo("lab.silver.stg_orders").createOrReplace()
for t in ("stg_users", "stg_orders"):
    for r in spark.sql(f"SELECT snapshot_id, operation, committed_at FROM lab.silver.{t}.snapshots").collect(): p(t, "snapshot", tuple(r))
    for r in spark.sql(f"SELECT file_path, record_count FROM lab.silver.{t}.files").collect(): p(t, "file", r[0].split('/')[-1], r[1])
p("DONE")
spark.stop()
