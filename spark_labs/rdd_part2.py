import re, time, json, urllib.request
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("lab-rdd-2").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_raw = sc.textFile("users.csv")
orders_raw = sc.textFile("orders.csv")
uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-")

print("== ETAPE 5 : users sans schema ==")
print("lignes brutes users:", users_raw.count())
print(users_raw.take(3))
users_lines = users_raw.filter(lambda l: bool(uuid_re.match(l)))
print("users apres filtre UUID:", users_lines.count())

print("== ETAPE 6 : parsing manuel orders ==")
header = orders_raw.first()
def parse(l):
    f = l.split(",")
    return {"order_id": f[0], "user_id": f[1], "date": f[2],
            "quantity": int(f[3]), "product": f[4].strip().lower()}
orders_clean = orders_raw.filter(lambda l: l.strip() != "" and l != header)
orders = orders_clean.map(parse).cache()
print("orders:", orders.count(), orders.first())

print("== ETAPE 7 : reduceByKey vs groupByKey ==")
kv = orders.map(lambda o: (o["user_id"], o["quantity"]))
t_reduce = kv.reduceByKey(lambda a, b: a + b)
t_group = kv.groupByKey().mapValues(sum)
r1, r2 = dict(t_reduce.collect()), dict(t_group.collect())
print("users:", len(r1), "| resultats identiques:", r1 == r2)
print("top5:", sorted(r1.items(), key=lambda x: -x[1])[:5])
try:
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
    for s in json.load(urllib.request.urlopen(base)):
        if s["shuffleWriteBytes"] or s["shuffleReadBytes"]:
            print("stage", s["stageId"], s["name"][:45], "| shuffle write", s["shuffleWriteBytes"], "read", s["shuffleReadBytes"])
except Exception as e:
    print("UI api indisponible:", e)

print("== ETAPE 8 : dedoublonnage ==")
by_id = orders.map(lambda o: (o["order_id"], o))
dedup = by_id.reduceByKey(lambda a, b: a if a["date"] >= b["date"] else b)
print("avant:", orders.count(), "apres:", dedup.count())

print("== ETAPE 9 : join ==")
users_kv = users_lines.map(lambda l: (l.split(",")[0], l))
orders_kv = orders.map(lambda o: (o["user_id"], o))
joined = orders_kv.join(users_kv)
print("join:", joined.count())
print(joined.map(lambda kv: (kv[0][:8], kv[1][0]["product"], kv[1][1][:30])).take(2))
left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin sans user:", left.filter(lambda kv: kv[1][1] is None).count())

print("== EX1 : aggregateByKey ==")
agg = kv.aggregateByKey((0, 0), lambda a, q: (a[0] + 1, a[1] + q), lambda a, b: (a[0] + b[0], a[1] + b[1]))
print(agg.mapValues(lambda c: (c[0], c[1], round(c[1] / c[0], 2))).take(3))

print("== EX2 : faux user ==")
fake = sc.parallelize([("fake-user-id", {"order_id": "x", "user_id": "fake-user-id", "date": "2026", "quantity": 1, "product": "bread"})])
lj = orders_kv.union(fake).leftOuterJoin(users_kv)
print("commandes sans user:", lj.filter(lambda kv: kv[1][1] is None).count())

print("== EX3 : DataFrame groupBy ==")
df = spark.createDataFrame(kv, ["user_id", "quantity"])
agg_df = df.groupBy("user_id").sum("quantity")
agg_df.explain()
print("lineage RDD (reduceByKey):")
print(t_reduce.toDebugString().decode())
print("DataFrame identique:", dict(agg_df.collect()) == r1)

print("== EX4 : cache ==")
def timeit(rdd, n=3):
    ts = []
    for _ in range(n):
        t = time.perf_counter(); rdd.count(); ts.append(round(time.perf_counter() - t, 3))
    return ts
print("sans cache:", timeit(orders_clean.map(parse)))
print("avec cache:", timeit(orders_clean.map(parse).cache()))
spark.stop()
