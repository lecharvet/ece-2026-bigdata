# Lab PySpark RDD - answers

- Default partitions (2): textFile uses minPartitions=2 for a small file; repartition() shuffles, coalesce() does not by default.
- Lazy evaluation: transformations only build the lineage; each action (count) launches a new job and re-reads the file unless cache() is used.
- No schema: textFile() splits on newlines, so quoted multiline addresses break records (101 lines for 50 users). csv(multiLine=True) understands quotes.
- reduceByKey vs groupByKey: shuffle write 2513 B vs 6230 B. reduceByKey combines per partition before the shuffle; groupByKey ships all raw pairs.
- Dedup: 2829 -> 2829 (no duplicates in the data); key = order_id, reduceByKey keeps the latest date.
- Join: 2829 matched, 0 orphan orders; with an injected fake user, leftOuterJoin finds 1 order without user.
- DataFrame groupBy gives the same result as reduceByKey; Catalyst adds the partial aggregation itself (partial_sum + Exchange + sum).
- Cache: ~0.25 s -> ~0.17 s per count on 2829 rows (small data, noisy); first count pays the cost of filling the cache.
