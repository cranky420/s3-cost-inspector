"""
lambda_s3_cost_report.py
AWS Lambda handler to:
 - run Athena queries over S3 Inventory tables (Parquet)
 - calculate object counts, size and estimated cost per storage class / tier
 - aggregate total cost per (table, prefix)
 - produce top-N report (CSV) for the most expensive prefixes
 - upload the CSV to S3 and email a short summary via SES

Environment variables:
 - ATHENA_DATABASE (default: s3_inventory)
 - OUTPUT_BUCKET (where Athena writes query results and where report CSV will be stored)
 - OUTPUT_PREFIX (prefix inside OUTPUT_BUCKET for Athena outputs and reports) e.g. athena-results/
 - REPORT_PREFIX (sub-prefix for the final reports) default: reports/
 - SENDER_EMAIL (SES verified email)
 - RECIPIENT_EMAIL (destination)
 - SES_ROLE_ARN (optional) - if you want to assume a role for sending SES
 - AWS_REGION (default us-east-1)
 - TOP_N (default 15)

IAM (Lambda role) should allow:
 - athena:StartQueryExecution, athena:GetQueryExecution
 - s3:GetObject (to read Athena CSV outputs), s3:PutObject (to put final report)
 - sts:AssumeRole (if SES_ROLE_ARN used)
 - ses:SendEmail (or use role to call SES)
"""

import os
import time
import csv
import io
import math
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import boto3

# ---------- Config via Environment ----------
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "s3_inventory")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "your-output-bucket")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "athena-results/")
REPORT_PREFIX = os.environ.get("REPORT_PREFIX", "reports/")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "no-reply@example.com")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "your-email@example.com")
SES_ROLE_ARN = os.environ.get("SES_ROLE_ARN", "")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
TOP_N = int(os.environ.get("TOP_N", "15"))

# Example table list (inventory tables)
TABLE_NAMES = [
    "my_bucket_1", "my_bucket_2", "my_bucket_3"
]

# Hardcoded cost-per-GB (USD) as requested
COST_PER_GB = {
    "STANDARD": 0.0210,
    "GLACIER": 0.0036,
    "DEEP_ARCHIVE": 0.00099,
    # Intelligent tier handled by access tier
    # we'll map access tiers to prices below
}

INTELLIGENT_TIER_PRICES = {
    "FREQUENT": 0.0210,
    "INFREQUENT": 0.0125,
    "ARCHIVE_INSTANT_ACCESS": 0.0040
}

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ---------- Helpers ----------
def role_arn_to_session(**args):
    """Assume role and return a boto3.Session"""
    stsclient = boto3.client("sts", region_name=AWS_REGION)
    stsresponse = stsclient.assume_role(**args)
    return boto3.Session(
        aws_access_key_id=stsresponse["Credentials"]["AccessKeyId"],
        aws_secret_access_key=stsresponse["Credentials"]["SecretAccessKey"],
        aws_session_token=stsresponse["Credentials"]["SessionToken"],
        region_name=AWS_REGION,
    )


def send_email(subject, body_text, body_html):
    """Send simple email via SES. Uses role if SES_ROLE_ARN provided."""
    if SES_ROLE_ARN:
        session = role_arn_to_session(
            RoleArn=SES_ROLE_ARN,
            RoleSessionName="LambdaSESSession",
            DurationSeconds=900,
        )
        ses = session.client("ses", region_name=AWS_REGION)
    else:
        ses = boto3.client("ses", region_name=AWS_REGION)

    response = ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [RECIPIENT_EMAIL]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": body_text, "Charset": "UTF-8"},
                "Html": {"Data": body_html, "Charset": "UTF-8"},
            },
        },
    )
    logger.info("SES send_email response: %s", response)


def float_or_zero(val):
    try:
        return float(val)
    except Exception:
        return 0.0


def int_or_zero(val):
    try:
        return int(float(val))
    except Exception:
        return 0


def bytes_to_gb(num_bytes):
    # precise GB conversion
    return float(num_bytes) / (1024 ** 3)


# ---------- Main Lambda Handler ----------
def lambda_handler(event, context):
    athena = boto3.client("athena", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Use yesterday's partition (dt)
    yesterday = datetime.utcnow() - timedelta(days=1)
    dt_value = yesterday.strftime("%Y-%m-%d") + "-01-00"
    logger.info("Using partition dt = %s", dt_value)

    # Data structure to hold detailed rows:
    # key = (table, prefix) -> value: dict with totals and breakdown
    prefixes = {}

    query_ids = []

    # --- Step 1: Start Athena queries (per table) ---
    for table in TABLE_NAMES:
        # This query returns per-prefix, per-storage_class/tier counts, total_size, estimated cost (USD)
        # prefix extraction uses regex to get the first path segment before /
        query = f"""
        SELECT
          regexp_extract("key", '([^/]+)', 1) AS prefix,
          storage_class,
          COALESCE(intelligent_tiering_access_tier, '') AS intelligent_tiering_access_tier,
          COUNT(*) AS object_count,
          SUM("size") AS total_size,
          CASE
            WHEN storage_class = 'STANDARD' THEN SUM("size") * {COST_PER_GB['STANDARD']} / (1024 * 1024 * 1024)
            WHEN storage_class = 'GLACIER' THEN SUM("size") * {COST_PER_GB['GLACIER']} / (1024 * 1024 * 1024)
            WHEN storage_class = 'DEEP_ARCHIVE' THEN SUM("size") * {COST_PER_GB['DEEP_ARCHIVE']} / (1024 * 1024 * 1024)
            WHEN storage_class = 'INTELLIGENT_TIERING' THEN
              CASE
                WHEN COALESCE(intelligent_tiering_access_tier, '') = 'FREQUENT' THEN SUM("size") * {INTELLIGENT_TIER_PRICES['FREQUENT']} / (1024 * 1024 * 1024)
                WHEN COALESCE(intelligent_tiering_access_tier, '') = 'INFREQUENT' THEN SUM("size") * {INTELLIGENT_TIER_PRICES['INFREQUENT']} / (1024 * 1024 * 1024)
                WHEN COALESCE(intelligent_tiering_access_tier, '') = 'ARCHIVE_INSTANT_ACCESS' THEN SUM("size") * {INTELLIGENT_TIER_PRICES['ARCHIVE_INSTANT_ACCESS']} / (1024 * 1024 * 1024)
                ELSE 0
              END
            ELSE 0
          END AS estimated_cost_usd
        FROM {table}
        WHERE dt = '{dt_value}'
        GROUP BY 1, storage_class, COALESCE(intelligent_tiering_access_tier, '')
        """

        logger.info("Starting Athena query for table %s", table)
        resp = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DATABASE},
            ResultConfiguration={"OutputLocation": f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}"},
        )
        qid = resp["QueryExecutionId"]
        query_ids.append((table, qid))

    # --- Step 2: Wait for queries to finish and fetch CSV outputs ---
    for table, qid in query_ids:
        logger.info("Waiting for query %s (table %s) to finish...", qid, table)
        while True:
            qstatus = athena.get_query_execution(QueryExecutionId=qid)
            state = qstatus["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                logger.info("Query %s finished with state %s", qid, state)
                break
            time.sleep(5)

        if state != "SUCCEEDED":
            logger.warning("Skipping %s because query state is %s", table, state)
            continue

        # Athena writes result to s3://OUTPUT_BUCKET/OUTPUT_PREFIX/<qid>.csv
        result_key = f"{OUTPUT_PREFIX}{qid}.csv"
        logger.info("Fetching Athena CSV result from s3://%s/%s", OUTPUT_BUCKET, result_key)
        try:
            obj = s3.get_object(Bucket=OUTPUT_BUCKET, Key=result_key)
        except Exception as e:
            logger.exception("Failed to get Athena result for %s: %s", qid, e)
            continue

        content = obj["Body"].read().decode("utf-8")
        reader = csv.reader(io.StringIO(content))
        header = next(reader, None)  # header row

        # Expected header: prefix,storage_class,intelligent_tiering_access_tier,object_count,total_size,estimated_cost_usd
        for row in reader:
            if not row or len(row) < 6:
                continue
            prefix = row[0] or "(empty)"
            storage_class = row[1] or "UNKNOWN"
            itt = row[2] or ""
            object_count = int_or_zero(row[3])
            total_size = int_or_zero(row[4])
            estimated_cost = float_or_zero(row[5])

            key = (table, prefix)
            if key not in prefixes:
                prefixes[key] = {
                    "table": table,
                    "prefix": prefix,
                    "total_size_bytes": 0,
                    "total_objects": 0,
                    "total_cost_usd": 0.0,
                    "breakdown": []  # list of dicts per storage-class/ittier
                }

            prefixes[key]["total_size_bytes"] += total_size
            prefixes[key]["total_objects"] += object_count
            prefixes[key]["total_cost_usd"] += estimated_cost
            prefixes[key]["breakdown"].append({
                "storage_class": storage_class,
                "intelligent_tiering_access_tier": itt,
                "object_count": object_count,
                "total_size_bytes": total_size,
                "estimated_cost_usd": estimated_cost
            })

    # --- Step 3: Pick top N prefixes by total_cost_usd ---
    all_prefixes = [
        (key[0], key[1], data["total_cost_usd"], data) for key, data in prefixes.items()
    ]
    top_prefixes = sorted(all_prefixes, key=lambda x: x[2], reverse=True)[:TOP_N]

    # --- Step 4: Build CSV report with breakdown per storage class for each top prefix ---
    report_rows = []
    # Header: Rank, Table, Prefix, TotalCostUSD, TotalSizeGB, TotalObjects, storage_class, ittier, object_count, size_gb, cost_usd
    report_header = [
        "rank",
        "table",
        "prefix",
        "total_cost_usd",
        "total_size_gb",
        "total_objects",
        "storage_class",
        "intelligent_tiering_access_tier",
        "object_count",
        "size_gb",
        "estimated_cost_usd"
    ]
    rank = 1
    for table_name, prefix_name, total_cost, data in top_prefixes:
        total_size_gb = bytes_to_gb(data["total_size_bytes"])
        total_objects = data["total_objects"]
        # Add a parent/summary row (storage_class empty)
        report_rows.append([
            rank,
            table_name,
            prefix_name,
            f"{data['total_cost_usd']:.6f}",
            f"{total_size_gb:.6f}",
            total_objects,
            "",  # storage_class
            "",  # ittier
            "",  # object_count
            "",  # size_gb
            ""   # estimated_cost_usd
        ])
        # Add breakdown rows
        for b in sorted(data["breakdown"], key=lambda x: x["estimated_cost_usd"], reverse=True):
            size_gb = bytes_to_gb(b["total_size_bytes"])
            report_rows.append([
                rank,
                table_name,
                prefix_name,
                "",  # total cost is in parent row
                "",  # total size in parent row
                "",
                b["storage_class"],
                b["intelligent_tiering_access_tier"],
                b["object_count"],
                f"{size_gb:.6f}",
                f"{b['estimated_cost_usd']:.6f}"
            ])
        rank += 1

    # --- Step 5: Write CSV to S3 ---
    report_key = f"{REPORT_PREFIX}top_{TOP_N}_s3_cost_report_{dt_value}.csv"
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(report_header)
    for r in report_rows:
        writer.writerow(r)

    csv_body = csv_buffer.getvalue()
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=report_key, Body=csv_body.encode("utf-8"))
    logger.info("Uploaded report to s3://%s/%s", OUTPUT_BUCKET, report_key)

    # --- Step 6: Send summary email ---
    email_text = f"Top {TOP_N} S3 prefixes by estimated cost for dt={dt_value}:\n\n"
    email_html = f"<html><body><h3>Top {TOP_N} S3 prefixes by estimated cost for dt={dt_value}</h3><ol>"

    for i, (table_name, prefix_name, total_cost, data) in enumerate(top_prefixes, start=1):
        total_cost_fmt = f"${data['total_cost_usd']:.2f}"
        total_size_gb = bytes_to_gb(data["total_size_bytes"])
        email_text += f"{i}. Table: {table_name}, Prefix: {prefix_name}, Cost: {total_cost_fmt}, Size: {total_size_gb:.2f} GB\n"
        email_html += f"<li>Table: {table_name}, Prefix: {prefix_name}, Cost: {total_cost_fmt}, Size: {total_size_gb:.2f} GB</li>"

    email_html += "</ol>"
    email_text += "\nReport CSV written to: s3://{}/{}\n".format(OUTPUT_BUCKET, report_key)
    email_html += "<p>Report CSV written to: s3://{}/{}</p></body></html>".format(OUTPUT_BUCKET, report_key)

    send_email(
        subject=f"Top {TOP_N} S3 prefixes by estimated cost for dt={dt_value}",
        body_text=email_text,
        body_html=email_html,
    )

    return {
        "statusCode": 200,
        "body": {
            "message": f"Report generated for dt={dt_value}",
            "report_s3_path": f"s3://{OUTPUT_BUCKET}/{report_key}"
        },
    }
