import json
import csv
import boto3
import requests
import io
import os
from datetime import datetime, timedelta

def get_currency_rates(url):
   # Get data from API
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    rates = data.get('rates', [])
    if not rates:
        raise Exception("No rates found in API response.")
    return rates

def convert_json_to_csv(exchange_rates):
    # Prepare CSV in memory
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=exchange_rates[0].keys())
    writer.writeheader()
    writer.writerows(exchange_rates)

    return csv_buffer.getvalue()

def write_to_s3(bucket, key, data):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType='text/csv')
    print('Data written to S3 bucket:', bucket, 'with key:', key)

def lambda_handler(event, context):
    # API endpoint
    try:
        api_url_yearly = "https://api.cnb.cz/cnbapi/exrates/daily-year?year=2025"
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')
        api_url_daily = f"https://api.cnb.cz/cnbapi/exrates/daily?date={formatted_date}"        
        # S3 bucket and key from environment variables or hardcoded
        s3_bucket = os.environ.get('S3_BUCKET', 'my-currency-exchange-bucket')
        s3_prefix = os.environ.get('S3_PREFIX', 'daily_currency_rates')
        s3_key = f"{s3_prefix}/{formatted_date}.csv"

        # Get currency rates
        exchange_rates = get_currency_rates(api_url_daily)
        # Convert to CSV
        csv_data = convert_json_to_csv(exchange_rates)
        # Write to S3
        write_to_s3(s3_bucket, s3_key, csv_data)

        return {
            'statusCode': 200,
            'body': f"Currency rates for 2025 stored in s3://{s3_bucket}/{s3_key}"
        }
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
