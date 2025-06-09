import csv
import io
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def format_date(value):
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return value.strip()

def lambda_handler(event, context):
    bucket_name = event['bucket']
    input_key = event['key']
    output_key = input_key.replace(".csv", "_transformed.csv")

    # Read CSV from S3
    response = s3.get_object(Bucket=bucket_name, Key=input_key)
    input_content = response['Body'].read().decode('utf-8')
    input_stream = io.StringIO(input_content)
    reader = csv.DictReader(input_stream)

    # Process and transform rows
    output_stream = io.StringIO()
    fieldnames = ['Product', 'Description', 'Release', 'End of Service']
    writer = csv.DictWriter(output_stream, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
            description = f"{row.get('Description', '').strip()} {row.get('Release', '').strip()}".strip()
            end_of_service = format_date(row.get('End of Service', ''))
    
            new_row = {
                'Product': row.get('Product', '').strip(),
                'Description': description,
                'Release': row.get('Release', '').strip(),
                'End of Service': end_of_service
            }
            writer.writerow(new_row)

    # Upload transformed CSV to S3
    output_stream.seek(0)
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=output_stream.getvalue().encode('utf-8'))

    return {
        'statusCode': 200,
        'body': f"File transformed and uploaded to s3://{bucket_name}/{output_key}"
    }
