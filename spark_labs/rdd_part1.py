from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("lab-rdd").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Etape 3 : creation RDD + partitions
users = sc.textFile("users.csv")
orders = sc.textFile("orders.csv")
print("partitions users :", users.getNumPartitions())
print("partitions orders:", orders.getNumPartitions())
orders8 = orders.repartition(8)
print("orders apres repartition(8):", orders8.getNumPartitions())

# Etape 4 : lazy evaluation (rien ne s'execute avant count)
header = orders.first()
clean = orders.filter(lambda l: l.strip() != "" and l != header)
print("--- lineage ---")
print(clean.toDebugString().decode())
print("count 1:", clean.count())
print("count 2:", clean.count())
spark.stop()
