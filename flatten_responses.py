import csv
import boto3
import io

s3 = boto3.client('s3')

def normalize_header(header):
    return header.strip().lower().replace('\xa0', ' ')

def flatten_versions_from_s3(input_bucket, input_key, output_bucket, output_key):
    # Download file from S3
    obj = s3.get_object(Bucket=input_bucket, Key=input_key)
    content = obj['Body'].read().decode('utf-8')
    infile = io.StringIO(content)

    reader = csv.reader(infile)
    raw_headers = next(reader)

    normalized_headers = [normalize_header(h) for h in raw_headers]
    header_map = dict(zip(normalized_headers, raw_headers))

    name_col = header_map.get("what is your name?")
    email_col = header_map.get("what is your email?")
    customer_col = header_map.get("who is the customer?")
    region_col = header_map.get("what is the region?")
    version_cols = [header_map[h] for h in normalized_headers if "version" in h]

    flattened_rows = []

    for row in reader:
        row_dict = dict(zip(raw_headers, row))
        name = row_dict.get(name_col, "").strip()
        email = row_dict.get(email_col, "").strip()
        customer = row_dict.get(customer_col, "").strip()
        region = row_dict.get(region_col, "").strip()

        for col in version_cols:
            versions = row_dict.get(col, "")
            if versions:
                for version in [v.strip() for v in versions.split(',') if v.strip()]:
                    flattened_rows.append({
                        'Name': name,
                        'Email': email,
                        'Customer': customer,
                        'Region': region,
                        'Version': version
                    })

    # Write to output as CSV in-memory
    output_csv = io.StringIO()
    writer = csv.DictWriter(output_csv, fieldnames=['Name', 'Email', 'Customer', 'Region', 'Version'])
    writer.writeheader()
    writer.writerows(flattened_rows)

    # Upload to S3
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=output_csv.getvalue().encode('utf-8')
    )

    print(f"✅ Flattened data written to s3://{output_bucket}/{output_key}")

def lambda_handler(event, context):
    # Customize this section based on event source or hard-code for testing
    input_bucket = event['input_bucket']
    input_key = event['input_key']
    output_bucket = event['output_bucket']
    output_key = event['output_key']

    flatten_versions_from_s3(input_bucket, input_key, output_bucket, output_key)
    return {
        'statusCode': 200,
        'body': f'Successfully processed and saved to {output_bucket}/{output_key}'
    }
