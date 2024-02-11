import timeit
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
ddl_file = open("./benchmark/create_table.sql")
sql_file = open("./benchmark/queries.sql")
bench_result = open("./benchmark/result.log", mode='w')

print("Create table")
spark.sql(ddl_file.read())

for c, query in enumerate(sql_file.readlines(), 1):
    print(f"{c}: {query}".rstrip(), file=bench_result, flush=True)
    if query.startswith("--"):
        continue
    for try_num in range(1):
        start = timeit.default_timer()
        results = spark.sql(query)
        results.collect()
        end = timeit.default_timer()
        print(end - start, file=bench_result, flush=True)
