from pyspark import pipelines as dp
from pyspark.sql import functions as F 
from pyspark.sql.functions import lit, col
from pyspark.sql.types import *

taxi_schema = StructType([
    StructField("LocationID", LongType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

bronze_table = spark.conf.get("bronze_table")
silver_table = spark.conf.get("silver_table")
gold_table = spark.conf.get("gold_table")
source_table = spark.conf.get("source_table")
filter_borough = spark.conf.get("filter_borough")

autoloader_source=spark.conf.get("autoloader_source")

dp.create_streaming_table(bronze_table)
@dp.append_flow(target=bronze_table)
def autoloader():

    if autoloader_source=='newpath':
        path="abfss://poc@muaazstorage.dfs.core.windows.net/autoloader/"
    else:
        path="abfss://poc@muaazstorage.dfs.core.windows.net/autoloader1/"
        
    df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .schema(taxi_schema)
            .load(path)
            .select(
                "*",
                col("_metadata.file_name").alias("file_name"),
                col("_metadata.file_path").alias("file_path"),
                col("_metadata.file_size").alias("file_size"),
                col("_metadata.file_modification_time").alias("file_modification_time")
            )
            .withColumn("source", lit("autoloader"))
        )
    return df

@dp.append_flow(target=bronze_table)
def postgres():
    df = (
        spark.readStream
        .table("poc.bronze.taxi_zones")
        .selectExpr(
            "CAST(LocationID AS BIGINT) as LocationID",
            "Borough",
            "Zone",
            "service_zone"
        )
        .withColumn("source", lit("postgres"))
    )
    return df


@dp.table(
    name=f'poc.silver.{silver_table}',
)
def transformed_taxi_zone():
    if autoloader_source=='new_path':
        condition='Queens'
    else:
        condition='King'
    df=spark.readStream.table(f"LIVE.poc.bronze.{bronze_table}")
    df=df.filter(df["Borough"] == f'{condition}')
    return df


# @dp.table(
#     name='poc.gold.gold_taxi_zone'
# )
# def curated_taxi_zone():
#     df=spark.readStream.table("LIVE.poc.silver.silver_taxi_zone")
#     df=df.groupBy("Zone").agg(F.sum("LocationID").alias("total_count"))
#     return df

@dp.materialized_view(
    name=f'poc.gold.{gold_table}'
)
def curated_taxi_zone_mat_view():
    df=spark.read.table(f"LIVE.poc.silver.{silver_table}")
    df=df.groupBy("Zone").agg(F.sum("LocationID").alias("total_count"))
    return df
