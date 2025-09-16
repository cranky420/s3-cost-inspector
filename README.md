# s3-cost-inspector
AWS Lambda + Athena report generator to find top S3 buckets by estimated storage cost (CSV + SES email).
Automated S3 cost reporting using S3 Inventory (Parquet), Athena, Lambda and SES — finds your top-N most expensive S3 prefixes (by estimated storage cost) and produces a CSV report (uploaded to S3) + a concise email summary every week. Built to help you spot cost leaks before they bite.

## What this repo contains:

- lambda_s3_cost_report.py — main Lambda handler that:
- runs Athena queries per inventory table,
- computes object counts / size / estimated cost per storage-class and intelligent-tier,
- aggregates totals by (table, prefix),
- selects top-N prefixes by cost,
- writes a detailed CSV to S3 and sends a short SES email with the S3 link.
- README.md — (this file).
- examples/ — (suggested) sample env config, test scripts, Athena query snippets.
- deploy/ — (suggested) SAM / CloudFormation / Terraform snippets.

## Quick overview (TL;DR):

- Enable S3 Inventory (Parquet) for the buckets you want to monitor.
- Create Athena tables over the inventory Parquet files (Glue catalog).
- Deploy lambda_s3_cost_report.py as a Lambda function, set env vars.
- Give Lambda needed IAM permissions (Athena, S3, SES).
- Schedule Lambda with EventBridge (weekly).
- Get a CSV in S3 + a summary email every Monday. 🎉

## Features:

- Per-prefix, per-storage-class (and intelligent-tier) breakdown.
- Hard-coded cost-per-GB values in the Athena query (easy to update).
- Aggregation & ranking by estimated cost.
- Detailed CSV uploaded to S3 for auditing and sharing.
- Summary email via SES with the S3 path to the CSV.

## Prerequisites:

### AWS account with:
- Athena + Glue catalog containing S3 Inventory tables (Parquet).
- S3 buckets: one for Athena query results & final reports.
- SES configured (or role that can send emails).
- Basic AWS CLI / Console access.
- Optional: SAM / Terraform if you want IaC deployment.

## IAM permissions:

- Lambda execution role needs (at minimum):
- athena:StartQueryExecution
- athena:GetQueryExecution
- s3:GetObject (read Athena results)
- s3:PutObject (write report CSV to OUTPUT_BUCKET)
- sts:AssumeRole (only if using SES_ROLE_ARN)
- ses:SendEmail (if sending SES directly from Lambda)
- Additionally, Athena service must be able to write to the OUTPUT_BUCKET. Ensure the bucket policy allows Athena to put objects there.

## Athena requirements:

- Athena tables pointing to the S3 Inventory Parquet files.
- Tables need columns used by the query:
- key, size, storage_class, intelligent_tiering_access_tier, dt.
- If your tables are partitioned by dt, ensure partitions exist for the dt value the Lambda will use (the code uses yesterday’s UTC date formatted as YYYY-MM-DD-01-00).
- If you recently added partitions, run MSCK REPAIR TABLE <table> in Athena or ensure Glue partition metadata is up-to-date.

## How to deploy:

### Option A — Quick (Console)
- Create a Lambda function (Python 3.9/3.10/3.11).
- Upload lambda_s3_cost_report.py (as a single file or zipped package).
- Set environment variables.
- Attach the IAM role with permissions listed above.
- Increase Lambda timeout to something reasonable (e.g., 10 minutes) — Athena queries can scan.
- Test via a Lambda test invocation.

### Option B — IaC (CloudFormation / Terraform)
- Add the Lambda function resource, environment variables, IAM role and an EventBridge schedule in your template.
- Ensure OUTPUT_BUCKET exists and is referenced in the template.

## Scheduling:

### Use EventBridge (CloudWatch Events) to run weekly. Example cron (UTC):
- Every Sunday 23:59 UTC: cron(59 23 ? * SUN *)
or
- Every Monday 00:05 UTC: cron(5 0 ? * MON *)
_Pick schedule based on when your inventory partitions are available._

## Testing & Local run:

- You can test the core logic locally (requires AWS credentials and network access):
- Create examples/env.local with your env vars.
- Run a quick script examples/run_one_table.py (suggested) that:
- imports lambda_s3_cost_report.lambda_handler,
- sets TABLE_NAMES to a single small table,
- calls the handler with a mock event/context.
- Or use the Lambda console “Test” with an empty event to run end-to-end (be cautious in prod; try with a small table first).


## SES notes:

- If SES is in sandbox: verify both SENDER_EMAIL and RECIPIENT_EMAIL (or move SES out of sandbox).
- You can optionally use IAM role assumption to send via a separate SES-permissioned role (SES_ROLE_ARN).


## Troubleshooting checklist:

- Athena query shows FAILED → check query error in Athena console (likely missing column or permissions).
- No CSV in S3 → make sure Athena ResultConfiguration.OutputLocation is writable by Athena.
- Lambda times out → increase timeout or optimize queries / reduce scanned data.
- SES email not delivered → check SES sending status, sandbox, and CloudWatch logs.

## Next steps / Improvements (ideas):
- Discover inventory tables dynamically from Glue instead of maintaining a static list.
- Attach the CSV as an email attachment (MIME) instead of only linking to S3.
- Add exponential backoff and retries around Athena/S3 calls.
- Store historical reports in a curated S3 reports folder and add a tiny UI/dashboard.
- Add a Slack webhook to post top offenders automatically.

## Contributing:
### PRs welcome. Please:
- Fork the repo
- Create a branch for your change
- Open a PR with tests or manual test steps

