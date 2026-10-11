import argparse
import os

import duckdb


def main():
    parser = argparse.ArgumentParser(prog="orders-report", description="Monthly orders report")
    parser.add_argument("-p", "--product", help="Only report this product.")
    args = parser.parse_args()

    bucket = os.environ["LAB_BUCKET_NAME"]
    path = f"s3://{bucket}/bronze/orders.csv"

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    rows = con.execute(
        """
        SELECT strftime(date_trunc('month', date), '%Y-%m') AS month,
               count(*) AS orders,
               sum(quantity) AS quantity
        FROM read_csv($path)
        WHERE $product IS NULL OR product = $product
        GROUP BY month
        ORDER BY month
        """,
        {"path": path, "product": args.product},
    ).fetchall()

    for month, orders, quantity in rows:
        print(f"{month}\t{orders}\t{quantity}")


if __name__ == "__main__":
    main()
