import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from pyspark.sql import SparkSession

weekly_aggregated_query = '''
WITH transformed_currency_rates AS (
    SELECT 
        currency_code,
        amount, 
        CAST(validFor AS date) AS valid_for,
        CAST(rate AS decimal(10,5)) AS exchange_rate,
        weekofyear(CAST(validFor AS date)) AS week_num
    FROM default.currency_xchng_rates_daily
)

insert overwrite table default.exchange_rate_weekly_aggr
SELECT 
    currency_code,
    amount,
    week_num,
    AVG(exchange_rate)  AS avg_exchange_rate
FROM transformed_currency_rates
Group By currency_code, amount, week_num
Order by currency_code asc, week_num asc
'''

rolling_avg_7d_query='''
WITH transformed_currency_rates AS (
    SELECT 
        currencycode,
        amount, 
        CAST(validFor AS date) AS valid_for,
        CAST(rate AS decimal(10,5)) AS exchange_rate
    FROM finance.currency_exchange_rates_daily
)

SELECT
    currencycode,
    amount,
    valid_for,
    AVG(exchange_rate) OVER (PARTITION BY currencycode ORDER BY valid_for ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_7d
FROM transformed_currency_rates
order by currencycode , valid_for
'''

def initiate_spark():
  
        spark_builder = SparkSession.builder \
            .appName("currency_rates_weekly_aggr") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .config("spark.kryoserializer.buffer.max", "512m") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") 
        spark = spark_builder.getOrCreate()
        return spark

if (__name__ == "__main__"):
    args = getResolvedOptions(sys.argv, ["JOB_NAME",])
    spark = initiate_spark()
    spark.sql(weekly_aggregated_query)


