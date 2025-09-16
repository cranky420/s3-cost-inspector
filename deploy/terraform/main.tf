terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.0"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = var.region
}

provider "random" {}

# -------------------------
# Variables
# -------------------------
variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project prefix used to name resources"
  type        = string
  default     = "s3-cost-inspector"
}

variable "output_bucket_prefix" {
  description = "Prefix used to build the Athena output/report bucket name"
  type        = string
  default     = "s3-cost-inspector-output"
}

variable "output_prefix" {
  description = "Prefix inside the output bucket where Athena writes results"
  type        = string
  default     = "athena-results/"
}

variable "report_prefix" {
  description = "Prefix inside the output bucket for final CSV reports"
  type        = string
  default     = "reports/"
}

variable "athena_database" {
  description = "Athena database that contains the S3 inventory tables"
  type        = string
  default     = "s3_inventory"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name"
  type        = string
  default     = "s3-cost-inspector-wg"
}

variable "sender_email" {
  description = "SES verified FROM email (must be verified in SES)"
  type        = string
  default     = ""
}

variable "recipient_email" {
  description = "Email to receive the report summary (if SES in sandbox must be verified)"
  type        = string
  default     = ""
}

variable "ses_role_arn" {
  description = "Optional: ARN of role to assume when calling SES (leave empty to call SES with Lambda role)"
  type        = string
  default     = ""
}

variable "table_names" {
  description = "List of Athena table names (inventory tables) to query"
  type        = list(string)
  default     = [
    "com_mobilewalla_rtb_avro_device_id_prod", "com_mobilewalla_rtb_avro_prime_prod",
    "com_mobilewalla_rtb_device_id_prod", "com_mw_pinot", "com_mw_poi_cache",
    "com_mw_sameer", "digicenter_us_mobilewalla_com", "eskimi_us_mobilewalla_com",
    "mobilewalla_com_rtb_prod", "mobilewalla_partner_modfx", "mobilewalla_partner_quadrant",
    "mobilewalla_partner_scanbuy", "mobilewalla_partner_tamoco_us", "mw_com_ds_kajanan",
    "mw_daas_raw_intermediate", "mw_data_aggregated_standard", "mw_data_aggregated_timeseries",
    "mw_device_profile", "mw_external_devices", "mw_test", "mwstats", "partner_mobilewalla_com",
    "resources_mobilewalla_com", "reveal_mobile_backup", "veraset_us_mobilewalla_com"
  ]
}

variable "top_n" {
  description = "Number of top prefixes to include in the report (TOP_N)"
  type        = number
  default     = 15
}

variable "schedule_expression" {
  description = "EventBridge schedule expression (cron or rate). Default: every Monday 00:05 UTC"
  type        = string
  default     = "cron(5 0 ? * MON *)"
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds (max 900)"
  type        = number
  default     = 900
}

# -------------------------
# Helpers: random id and bucket name
# -------------------------
resource "random_id" "bucket_id" {
  byte_length = 4
}

locals {
  output_bucket_name = "${var.output_bucket_prefix}-${random_id.bucket_id.hex}"
}

# -------------------------
# S3: Athena output & reports bucket
# -------------------------
resource "aws_s3_bucket" "output" {
  bucket = local.output_bucket_name
  acl    = "private"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    id      = "cleanup-temp"
    enabled = true

    expiration {
      days = 365
    }
  }

  tags = {
    Name    = local.output_bucket_name
    Project = var.project
  }
}

# Allow Athena to write results and Lambda to read/write objects
resource "aws_s3_bucket_public_access_block" "output_block" {
  bucket = aws_s3_bucket.output.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -------------------------
# IAM Role & Policy for Lambda
# -------------------------
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.project}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags = {
    Project = var.project
  }
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${var.region}:*:log-group:/aws/lambda/*"]
  }

  statement {
    sid = "AthenaPermissions"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream"
    ]
    resources = ["*"]
  }

  statement {
    sid = "S3ReadWrite"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:PutObjectAcl"
    ]
    resources = [
      aws_s3_bucket.output.arn,
      "${aws_s3_bucket.output.arn}/*"
    ]
  }

  statement {
    sid = "GlueRead"
    actions = [
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetTableVersion",
      "glue:GetTableVersions",
      "glue:GetDatabase"
    ]
    resources = ["*"]
  }

  statement {
    sid = "SESAndSTS"
    actions = [
      "ses:SendEmail",
      "ses:SendRawEmail",
      "sts:AssumeRole"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "lambda_policy_attach" {
  name   = "${var.project}-lambda-policy"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_policy.json
}

# -------------------------
# Package local lambda source file into zip
# -------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/lambda_package.zip"
  source_file = "${path.module}/lambda_s3_cost_report.py"
}

# -------------------------
# Lambda Function
# -------------------------
resource "aws_lambda_function" "reporter" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project}-reporter"
  role             = aws_iam_role.lambda_exec.arn
  handler          = "lambda_s3_cost_report.lambda_handler"
  runtime          = "python3.11"
  timeout          = var.lambda_timeout_seconds
  memory_size      = 512
  publish          = true

  environment {
    variables = {
      ATHENA_DATABASE = var.athena_database
      OUTPUT_BUCKET   = aws_s3_bucket.output.bucket
      OUTPUT_PREFIX   = var.output_prefix
      REPORT_PREFIX   = var.report_prefix
      SENDER_EMAIL    = var.sender_email
      RECIPIENT_EMAIL = var.recipient_email
      SES_ROLE_ARN    = var.ses_role_arn
      AWS_REGION      = var.region
      TOP_N           = tostring(var.top_n)
      # Optionally include TABLE_NAMES; we convert the list into a JSON string
      TABLE_NAMES     = jsonencode(var.table_names)
    }
  }

  tags = {
    Project = var.project
  }

  depends_on = [
    aws_iam_role_policy.lambda_policy_attach
  ]
}

# -------------------------
# Athena Workgroup (so results default to our output prefix)
# -------------------------
resource "aws_athena_workgroup" "wg" {
  name = var.athena_workgroup_name

  configuration {
    publish_cloudwatch_metrics_enabled = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.output.bucket}/${var.output_prefix}"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Project = var.project
  }
}

# -------------------------
# EventBridge Rule to trigger Lambda on schedule
# -------------------------
resource "aws_cloudwatch_event_rule" "schedule" {
  name                = "${var.project}-schedule"
  description         = "Scheduled rule to trigger S3 cost report Lambda"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "invoke_lambda" {
  rule      = aws_cloudwatch_event_rule.schedule.name
  target_id = "invoke-lambda"
  arn       = aws_lambda_function.reporter.arn
}

# grant EventBridge permission to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reporter.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule.arn
}

# -------------------------
# Outputs
# -------------------------
output "output_bucket" {
  description = "S3 bucket created for Athena output and reports"
  value       = aws_s3_bucket.output.bucket
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.reporter.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.reporter.arn
}

output "eventbridge_rule" {
  description = "EventBridge (CloudWatch) schedule rule"
  value       = aws_cloudwatch_event_rule.schedule.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.wg.name
}

