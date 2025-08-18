CREATE EXTERNAL TABLE finance.currency_rates_weekly_aggr (
    currency_code string,
    amount string,
    week_num string,
    avg_exchange_rate string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
LOCATION 's3://my-currency-exchange-bucket/currency_rates_weekly_aggr/'
;
