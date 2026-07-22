import os
import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch")
DYNAMO = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")
CLUSTER = os.getenv("EMR_CLUSTER_ID", "")

session = boto3.Session(region_name=REGION)
kinesis = session.client("kinesis")
s3 = session.client("s3")
dynamo = session.client("dynamodb")
emr = session.client("emr")


def empty_bucket(name):
    try:
        paginator = s3.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=name):
            objs = []
            for v in page.get("Versions", []):
                objs.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []):
                objs.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            if objs:
                s3.delete_objects(Bucket=name, Delete={"Objects": objs})
        s3.delete_bucket(Bucket=name)
        print(f"bucket deleted: {name}")
    except Exception as e:
        print(f"bucket skip {name}: {e}")


def delete_stream():
    try:
        kinesis.delete_stream(StreamName=STREAM, EnforceConsumerDeletion=True)
        print(f"stream deleted: {STREAM}")
    except Exception as e:
        print(f"stream skip: {e}")


def delete_table():
    try:
        dynamo.delete_table(TableName=DYNAMO)
        print(f"dynamo deleted: {DYNAMO}")
    except Exception as e:
        print(f"dynamo skip: {e}")


def terminate_emr():
    if not CLUSTER:
        print("no EMR_CLUSTER_ID")
        return
    try:
        emr.terminate_job_flows(JobFlowIds=[CLUSTER])
        print(f"emr terminate: {CLUSTER}")
    except Exception as e:
        print(f"emr skip: {e}")


if __name__ == "__main__":
    terminate_emr()
    delete_stream()
    delete_table()
    empty_bucket(S3_RAW)
    empty_bucket(S3_BATCH)
