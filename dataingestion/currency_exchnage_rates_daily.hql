CREATE DATABASE finance
LOCATION 's3://my-currency-exchange-bucket/ath_db/';

CREATE EXTERNAL TABLE currency_exchange_rates_daily (
   validFor string,
   order string,
   country string,
   currency string,
   amount string,
   currencyCode string,
   rate string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
LOCATION 's3://my-currency-exchange-bucket/daily_currency_rates/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);