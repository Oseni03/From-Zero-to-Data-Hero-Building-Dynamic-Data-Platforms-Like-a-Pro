import os
import boto3
import json
from dotenv import load_dotenv
import time
import re

load_dotenv("dev.env")


def create_sqs_queue(client, queue_name):
    try:
        response = client.create_queue(QueueName=queue_name)
        return response["QueueUrl"]
    except client.exceptions.QueueNameExists:
        print(f"Queue '{queue_name}' already exists.")
        return client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def configure_sqs_policy(client, queue_arn, queue_url, bucket, aws_account_id):
    sqs_policy = {
        "Version": "2012-10-17",
        "Id": "example-ID",
        "Statement": [
            {
                "Sid": "example-statement-ID",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "StringEquals": {
                        "aws:SourceAccount": aws_account_id
                    },
                    "ArnLike": {
                        "aws:SourceArn": f"arn:aws:s3:*:*:{bucket}"

                    }
                }
            }
        ]
    }
    client.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": json.dumps(sqs_policy)})


def configure_s3_event(client, bucket_name, queue_arn, prefix, s3_event_name):
    if prefix:
        s3_event_configurations = [
            {
                "Id": s3_event_name,
                "Events": ["s3:ObjectCreated:*"],
                "QueueArn": queue_arn,
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {
                                "Name": "prefix",
                                "Value": prefix
                            }
                        ]
                    }
                }
            }
        ]

        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"QueueConfigurations": s3_event_configurations}
        )
    else:
        s3_event_configurations = [
            {
                "Id": s3_event_name,
                "Events": ["s3:ObjectCreated:*"],
                "QueueArn": queue_arn

            }
        ]

        client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"QueueConfigurations": s3_event_configurations}
        )


def cleanup_resources(sqs_client, s3_client, aws_account_id, sqs_queue_name, bucket_name):
    # Delete the SQS queue
    try:
        queue_url = f"https://sqs.{aws_region}.amazonaws.com/{aws_account_id}/{sqs_queue_name}"
        sqs_client.delete_queue(QueueUrl=queue_url)
        print(f"Deleted SQS queue: {sqs_queue_name}")
    except Exception as e:
        print(f"Failed to delete SQS queue: {e}")

    # Remove S3 event configurations
    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"QueueConfigurations": []}
        )
        print(f"Removed S3 event configurations from bucket: {bucket_name}")
    except Exception as e:
        print(f"Failed to remove S3 event configurations: {e}")

    # Delete SQS policy
    try:
        queue_url = f"https://sqs.{aws_region}.amazonaws.com/{aws_account_id}/{sqs_queue_name}"
        sqs_client.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"Policy": ""}
        )
        print("Deleted SQS policy")
    except Exception as e:
        print(f"Failed to delete SQS policy: {e}")


def extract_bucket_name(s3_ingestion_path):
    match = re.match(r"s3://([^/]+)/", s3_ingestion_path)
    if match:
        return match.group(1)
    else:
        raise ValueError("Invalid S3 ingestion path")


def main():
    global aws_access_key, aws_secret_key, aws_region
    load_dotenv("dev.env")
    aws_access_key = os.getenv("DEV_ACCESS_KEY")
    aws_secret_key = os.getenv("DEV_SECRET_KEY")
    aws_region = os.getenv("DEV_REGION")

    json_payload = {
        "s3_ingestion_path": "s3://jt-datateam-sandbox-qa-dev/raw/customers/",
        "table_name": "customers",
        "aws_account_id": "043916019468"
    }

    json_payload['sqs_queue_name'] = f"{json_payload.get('table_name')}-ingestion-queue"
    json_payload['bucket'] = extract_bucket_name(json_payload['s3_ingestion_path'])
    json_payload['s3_event_name'] = f"{json_payload.get('table_name')}-event-forward-to--{json_payload.get('sqs_queue_name')}"
    print(json.dumps(json_payload, indent=3))

    sqs_client = boto3.client('sqs', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                              region_name=aws_region)
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                             region_name=aws_region)

    queue_url = create_sqs_queue(sqs_client, json_payload["sqs_queue_name"])
    queue_arn = f"arn:aws:sqs:{os.getenv('DEV_REGION')}:{json_payload.get('aws_account_id')}:{json_payload.get('sqs_queue_name')}"
    bucket = json_payload.get("bucket")
    time.sleep(1)

    configure_sqs_policy(sqs_client, queue_arn, queue_url, bucket, json_payload["aws_account_id"])
    time.sleep(1)

    configure_s3_event(s3_client, json_payload["bucket"], queue_arn, None, json_payload.get("s3_event_name"))
    time.sleep(1)


if __name__ == "__main__":
    main()
