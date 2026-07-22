import os
import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
STREAM = os.getenv("KINESIS_STREAM", "earthquake-stream")
SHARDS = int(os.getenv("KINESIS_SHARDS", "2"))
S3_RAW = os.getenv("S3_RAW", "earthquake-pipeline-raw")
S3_BATCH = os.getenv("S3_BATCH", "earthquake-pipeline-batch")
DYNAMO = os.getenv("DYNAMO_TABLE", "earthquake-speed-view")

session = boto3.Session(region_name=REGION)
kinesis = session.client("kinesis")
s3 = session.client("s3")
dynamo = session.client("dynamodb")
emr = session.client("emr")


def create_stream():
    try:
        kinesis.create_stream(StreamName=STREAM, ShardCount=SHARDS)
        kinesis.get_waiter("stream_exists").wait(StreamName=STREAM)
        print(f"stream ready: {STREAM}")
    except kinesis.exceptions.ResourceInUseException:
        print(f"stream exists: {STREAM}")


def create_bucket(name):
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=name)
        else:
            s3.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"bucket created: {name}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"bucket exists: {name}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"bucket exists: {name}")


def create_dynamo_table():
    try:
        dynamo.create_table(
            TableName=DYNAMO,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print(f"dynamo created: {DYNAMO}")
    except dynamo.exceptions.ResourceInUseException:
        print(f"dynamo exists: {DYNAMO}")


def create_emr_cluster():
    resp = emr.run_job_flow(
        Name="earthquake-pipeline",
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
        Instances={
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 2,
            "KeepJobFlowAliveWhenNoSteps": True,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        LogUri=f"s3://{S3_BATCH}/emr-logs/",
        ManagedScalingPolicy={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": 2,
                "MaximumCapacityUnits": 8,
                "MaximumOnDemandCapacityUnits": 8,
            }
        },
    )
    cid = resp["JobFlowId"]
    print(f"emr launched: {cid}")
    print("scale-out cooldown ~60-120s, scale-in ~300s (EMR managed)")
    return cid


if __name__ == "__main__":
    create_stream()
    create_bucket(S3_RAW)
    create_bucket(S3_BATCH)
    create_dynamo_table()
    cluster_id = create_emr_cluster()
    print(f"set EMR_CLUSTER_ID={cluster_id}")
