import os, re, time, json, urllib.request
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, pandas_udf

B = os.environ["LAB_BUCKET_NAME"]
ep = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
ep = ep if ep.startswith("http") else "https://" + ep
spark = (SparkSession.builder.master("local[2]").appName("lab-silver-ex")
    .config("spark.sql.session.timeZone", "UTC")
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
sc = spark.sparkContext
sc.setLogLevel("ERROR")
def p(*a): print("##", *a, flush=True)
rd = lambda t: (spark.read.option("header", True).option("multiLine", True)
                .option("quote", '"').option("escape", '"').csv(f"s3a://{B}/bronze/{t}.csv"))
users_s = spark.read.parquet(f"s3a://{B}/silver/spark/stg_users")
orders_s = spark.read.parquet(f"s3a://{B}/silver/spark/stg_orders")

p("EX1 : birthdate_is_valid")
first = orders_s.groupBy("user_id").agg(F.min(F.col("ordered_at").cast("date")).alias("first_order_date"))
valid = F.col("birthdate").isNotNull() & (F.col("first_order_date").isNull() | (F.col("birthdate") <= F.col("first_order_date")))
df_v = users_s.join(first, "user_id", "left").withColumn("birthdate_is_valid", valid)
df_v = df_v.withColumn("birthdate", F.when(F.col("birthdate_is_valid"), F.col("birthdate")))
p("DataFrame API - invalides:", df_v.filter(~F.col("birthdate_is_valid")).count())
users_s.createOrReplaceTempView("u"); orders_s.createOrReplaceTempView("o")
sql_v = spark.sql("""WITH f AS (SELECT user_id, min(CAST(ordered_at AS DATE)) AS first_order_date FROM o GROUP BY user_id)
  SELECT u.*, (u.birthdate IS NOT NULL AND (f.first_order_date IS NULL OR u.birthdate <= f.first_order_date)) AS birthdate_is_valid
  FROM u LEFT JOIN f USING (user_id)""")
p("Spark SQL     - invalides:", sql_v.filter("NOT birthdate_is_valid").count())

p("EX2 : zip UDF vs pandas_udf vs regexp_extract (10000 lignes)")
big = rd("users").select("address").crossJoin(spark.range(200).select())
pat = r"(\d{5})\s*$"
@udf("string")
def zip_udf(a):
    m = re.search(pat, a or ""); return m.group(1) if m else None
cols = {"python_udf": zip_udf("address"), "regexp_extract": F.nullif(F.regexp_extract("address", pat, 1), F.lit(""))}
try:
    @pandas_udf("string")
    def zip_pd(s):
        return s.str.extract(pat)[0]
    cols["pandas_udf"] = zip_pd("address")
except Exception as e:
    p("pandas_udf indisponible:", e)
def t(col):
    s = time.perf_counter(); big.select(col.alias("z")).agg(F.count("z")).collect(); return round(time.perf_counter() - s, 3)
for name, col in cols.items():
    p(name, [t(col) for _ in range(3)])

p("EX3 : cache bronze")
ob = rd("orders")
def tc(df, n=3):
    r = []
    for _ in range(n):
        s = time.perf_counter(); df.count(); r.append(round(time.perf_counter() - s, 3))
    return r
p("sans cache:", tc(ob))
ob.cache()
p("avec cache:", tc(ob))
try:
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd"
    for r in json.load(urllib.request.urlopen(base)):
        p("storage:", r["name"][:50], "| memoire", r["memoryUsed"], "o | partitions en cache", r["numCachedPartitions"])
except Exception as e:
    p("UI api indisponible:", e)
ob.unpersist()

p("EX4 : iceberg append + time travel")
snaps = lambda: [r[0] for r in spark.sql("SELECT snapshot_id FROM lab.silver.stg_orders.snapshots ORDER BY committed_at").collect()]
s1 = snaps()
p("avant:", spark.table("lab.silver.stg_orders").count(), "| snapshots:", len(s1))
spark.sql("INSERT INTO lab.silver.stg_orders VALUES ('ex-append-1', 'fake-user-id', TIMESTAMP '2026-10-23 12:00:00', 3, 'bread')")
s2 = snaps()
p("apres:", spark.table("lab.silver.stg_orders").count(), "| snapshots:", len(s2))
for r in spark.sql("SELECT snapshot_id, operation, summary['added-records'] FROM lab.silver.stg_orders.snapshots ORDER BY committed_at").collect(): p("snapshot", tuple(r))
p("VERSION AS OF", s1[0], ":", spark.sql(f"SELECT count(*) FROM lab.silver.stg_orders VERSION AS OF {s1[0]}").collect()[0][0],
  "| version courante:", spark.sql("SELECT count(*) FROM lab.silver.stg_orders").collect()[0][0])
p("DONE")
spark.stop()
