import csv
import smtplib
import os
from datetime import datetime, timedelta
from email.mime.text import MIMEText
import boto3

ses = boto3.client('ses', region_name=os.environ.get("SES_REGION", "us-east-2"))

# === CONFIG from environment variables ===
SMTP_SERVER = os.environ.get("SMTP_SERVER")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
FROM_EMAIL = os.environ.get("FROM_EMAIL")
S3_BUCKET = os.environ.get("S3_BUCKET")

s3 = boto3.client("s3")

def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except:
            continue
    return None

def is_within_6_months(date_str):
    date_obj = parse_date(date_str)
    if not date_obj:
        return False
    return datetime.today() <= date_obj <= (datetime.today() + timedelta(days=183))

def send_email(to_email, matched_versions):
    subject = "Upcoming Product End of Service Notification"
    body = "The following products in your inventory are reaching end of service within 6 months:\n\n"
    for desc, eos_date in matched_versions:
        body += f"  - {desc} (End of Service: {eos_date})\n"
    body += "\nPlease take appropriate action."

    try:
        response = ses.send_email(
            Source=os.environ.get("FROM_EMAIL"),
            Destination={
                'ToAddresses': [to_email]
            },
            Message={
                'Subject': {'Data': subject},
                'Body': {
                    'Text': {'Data': body}
                }
            }
        )
        print(f"✅ SES email sent to {to_email}: {response['MessageId']}")
    except Exception as e:
        print(f"❌ Failed to send SES email to {to_email}: {e}")

def read_csv_from_s3(bucket, key):
    print(f"📥 Reading {key} from S3 bucket {bucket}")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8').splitlines()
    return list(csv.DictReader(content))

def lambda_handler(event, context):
    lifecycle_key = event.get("lifecycle_key")
    response_key = event.get("response_key")

    if not lifecycle_key or not response_key:
        print("❌ Missing required keys in event JSON.")
        return

    lifecycle_data_raw = read_csv_from_s3(S3_BUCKET, lifecycle_key)
    response_data_raw = read_csv_from_s3(S3_BUCKET, response_key)

    lifecycle_data = [
        {
            'description': row.get('Description', '').lower(),
            'end_of_service': row.get('End of Service', '')
        }
        for row in lifecycle_data_raw
    ]

    response_data = [
        {
            'version': row.get('Version', '').lower(),
            'owner': row.get('Email', '')
        }
        for row in response_data_raw
    ]

    notifications = {}
    print("🔍 Matching versions with lifecycle descriptions...")

    for response in response_data:
        version = response['version']
        owner = response['owner']
        for item in lifecycle_data:
            if version and version in item['description']:
                if is_within_6_months(item['end_of_service']):
                    notifications.setdefault(owner, []).append(
                        (item['description'], item['end_of_service'])
                    )

    if not notifications:
        print("✅ No upcoming end-of-service matches found.")
    else:
        print(f"📬 Preparing to send {len(notifications)} notifications...")
        for owner_email, matches in notifications.items():
            print(f"\nTo: {owner_email}")
            for desc, eos in matches:
                print(f"  - {desc} | End of Service: {eos}")
            if owner_email:
                send_email(owner_email, matches)

    print("✅ Lambda EoS check complete.")


