import os
import sys
from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def p(*a):
    print("##", *a, flush=True)


def build(principal=None):
    principal = principal or (sys.argv[1] if len(sys.argv) > 1 else "dev")
    cred = os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    bucket = os.environ["LAB_BUCKET_NAME"]
    ep = os.environ["AWS_S3_ENDPOINT"]
    ep = ep if ep.startswith("http") else f"https://{ep}"
    c = {
        "spark.hadoop.fs.s3a.endpoint": ep,
        "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
        "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "spark.hadoop.fs.s3a.session.token": os.environ.get("AWS_SESSION_TOKEN", ""),
        "spark.hadoop.fs.s3a.endpoint.region": os.environ.get("AWS_REGION"),
        "spark.hadoop.fs.s3a.aws.credentials.provider":
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.jars.packages": ICEBERG_PACKAGE,
        "spark.sql.extensions":
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.lab": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.lab.type": "hadoop",
        "spark.sql.catalog.lab.warehouse": f"s3a://{bucket}/warehouse",
        "spark.sql.catalog.polaris": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.polaris.type": "rest",
        "spark.sql.catalog.polaris.uri": f"{os.environ['POLARIS_URI']}/api/catalog",
        "spark.sql.catalog.polaris.warehouse": os.environ["POLARIS_CATALOG"],
        "spark.sql.catalog.polaris.credential": cred,
        "spark.sql.catalog.polaris.scope": "PRINCIPAL_ROLE:ALL",
        "spark.sql.catalog.polaris.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    }
    b = SparkSession.builder.appName("lab-polaris")
    for k, v in c.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    p("Polaris principal:", principal)
    p("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
    return spark


def q(spark, label, sql, n=20):
    """Run one statement, print up to n rows, or the first line of the error."""
    try:
        rows = spark.sql(sql).limit(n).collect()
        p(f"{label}: OK, {len(rows)} row(s)")
        for r in rows:
            p("  ", tuple(r))
        return True
    except Exception as e:
        msg = " ".join(str(e).split())[:400]
        p(f"{label}: ERROR {type(e).__name__}: {msg}")
        return False
