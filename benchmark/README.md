## Preparation
1. Download https://github.com/ClickHouse/ClickBench?tab=readme-ov-file#data-loading
2. Move it to `./spark-warehouse/hits/`


## Bulk

```shell
./bin/comet-spark-shell pyspark ./benchmark/run.py
```


## one-by-one

```shell
./bin/comet-spark-shell pyspark --conf spark.sql.catalogImplementation=in-memory
```

```py
>> spark.sql(open("benchmark/create_table.sql").read())
>> spark.sql("...").collect()
```
