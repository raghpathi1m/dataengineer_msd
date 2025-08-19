# AWS Data Pipeline
This repository contains code artifacts for an AWS-based data pipeline designed to calculate the weekly average of currency exchange rates.The solution is built using an AWS Cloud-Native stack, leveraging the following services:
- AWS Lambda for data ingestion via external API
- AWS Glue (with PySpark and SQL) for data transformation
- Amazon S3 for storage
- AWS Step Functions for orchestration.
  
The pipeline is scheduled to run daily from Monday to Saturday using an Amazon EventBridge scheduler with a cron expression, ensuring timely and automated execution of each stage.

## Solution Architecture
<img width="1641" height="1235" alt="aws_data_pipeline_with_lambda,_s3,_glue,_athena" src="https://github.com/user-attachments/assets/59a6a8e7-8757-4c1d-b696-6158c8ec7790" />

## DAG orchestration
<img width="552" height="557" alt="image" src="https://github.com/user-attachments/assets/5d9f1461-fabf-4fb6-aba8-381d4a99ec32" />

## Project Folder
- **dag** : contains the definition of job orchestration using AWS Step Functions, outlining the flow of a data pipeline with the following components:
- **dataingestion**: Fetches currency rates data via an external API using AWS Lambda.
- **transformations** : Contains the scripts to execute the transformation logic , the sql logic is embedded with in the script and table definition to store the aggreagted results
  
The entire pipeline is orchestrated using AWS Step Functions and scheduled via Amazon EventBridge (cron scheduler) to run daily from Monday to Saturday, ensuring seamless coordination between the ingestion and transformation stages.

## Sample Output
`` select * from finance.currency_rates_weekly_aggr where week_num in ('32','33') ``
Ouput for weekly averages for last two weeks.
<img width="421" height="607" alt="image" src="https://github.com/user-attachments/assets/f0f57eaa-bda5-47d7-b9fd-400af2d7dbf7" />




