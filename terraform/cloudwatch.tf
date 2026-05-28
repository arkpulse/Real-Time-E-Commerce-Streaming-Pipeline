# =============================================================
# Terraform: CloudWatch Alarms & Dashboards
# =============================================================
# Full observability stack: alarms for every Lambda,
# Kinesis iterator age, Glue job failures, and a
# CloudWatch dashboard for the ops team.
# =============================================================

# ── Lambda Error Alarms (all functions) ────────────────────

locals {
  lambda_functions = {
    orders      = aws_lambda_function.orders_processor.function_name
    payments    = "ecommerce-payments-processor-${var.environment}"
    clickstream = "ecommerce-clickstream-processor-${var.environment}"
    cart        = "ecommerce-cart-processor-${var.environment}"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each = local.lambda_functions

  alarm_name          = "${each.value}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = var.lambda_error_threshold
  alarm_description   = "Lambda ${each.key} error count too high"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions          = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { FunctionName = each.value }
}

resource "aws_cloudwatch_metric_alarm" "lambda_duration" {
  for_each = local.lambda_functions

  alarm_name          = "${each.value}-duration"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "Duration"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "p95"
  threshold           = var.lambda_timeout_seconds * 1000 * 0.80   # 80% of timeout
  alarm_description   = "Lambda ${each.key} p95 duration nearing timeout"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { FunctionName = each.value }
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each = local.lambda_functions

  alarm_name          = "${each.value}-throttles"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Throttles"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 20
  alarm_description   = "Lambda ${each.key} is being throttled"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { FunctionName = each.value }
}


# ── Kinesis Iterator Age Alarms (all streams) ──────────────

resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  for_each = { for k, v in aws_kinesis_stream.streams : k => v if k != "dlq" }

  alarm_name          = "${each.value.name}-iterator-age"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Maximum"
  threshold           = var.kinesis_iterator_age_threshold_ms
  alarm_description   = "Kinesis ${each.key} stream consumer is falling behind"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { StreamName = each.value.name }
}

resource "aws_cloudwatch_metric_alarm" "kinesis_write_throttle" {
  for_each = { for k, v in aws_kinesis_stream.streams : k => v if k != "dlq" }

  alarm_name          = "${each.value.name}-write-throttle"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  alarm_description   = "Kinesis ${each.key} stream write throughput exceeded"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { StreamName = each.value.name }
}


# ── Glue Job Failure Alarms ─────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_bronze_silver_failures" {
  alarm_name          = "${aws_glue_job.bronze_to_silver.name}-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 3600
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Bronze→Silver Glue job has failed tasks"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { JobName = aws_glue_job.bronze_to_silver.name }
}

resource "aws_cloudwatch_metric_alarm" "glue_silver_gold_failures" {
  alarm_name          = "${aws_glue_job.silver_to_gold.name}-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  namespace           = "Glue"
  period              = 3600
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Silver→Gold Glue job has failed tasks"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.pipeline_alerts.arn]

  dimensions = { JobName = aws_glue_job.silver_to_gold.name }
}


# ── CloudWatch Dashboard ────────────────────────────────────

resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "ecommerce-pipeline-${var.environment}"

  dashboard_body = jsonencode({
    widgets = [
      # ── Row 1: Lambda overview ─────────────────────────
      {
        type       = "metric"
        x = 0; y = 0; width = 8; height = 6
        properties = {
          title  = "Lambda Invocations (all functions)"
          period = 300
          stat   = "Sum"
          metrics = [for k, v in local.lambda_functions :
            ["AWS/Lambda", "Invocations", "FunctionName", v]
          ]
          view  = "timeSeries"
          stacked = false
        }
      },
      {
        type       = "metric"
        x = 8; y = 0; width = 8; height = 6
        properties = {
          title  = "Lambda Errors (all functions)"
          period = 300
          stat   = "Sum"
          metrics = [for k, v in local.lambda_functions :
            ["AWS/Lambda", "Errors", "FunctionName", v]
          ]
          view  = "timeSeries"
          stacked = false
        }
      },
      {
        type       = "metric"
        x = 16; y = 0; width = 8; height = 6
        properties = {
          title  = "Lambda Duration p95 (ms)"
          period = 300
          stat   = "p95"
          metrics = [for k, v in local.lambda_functions :
            ["AWS/Lambda", "Duration", "FunctionName", v]
          ]
          view = "timeSeries"
        }
      },
      # ── Row 2: Kinesis ──────────────────────────────────
      {
        type       = "metric"
        x = 0; y = 6; width = 12; height = 6
        properties = {
          title  = "Kinesis Iterator Age (ms)"
          period = 300
          stat   = "Maximum"
          metrics = [for k, v in aws_kinesis_stream.streams :
            ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds",
             "StreamName", v.name] if k != "dlq"
          ]
          view = "timeSeries"
        }
      },
      {
        type       = "metric"
        x = 12; y = 6; width = 12; height = 6
        properties = {
          title  = "Kinesis Records Put (per stream)"
          period = 300
          stat   = "Sum"
          metrics = [for k, v in aws_kinesis_stream.streams :
            ["AWS/Kinesis", "PutRecords.Records", "StreamName", v.name]
          ]
          view = "timeSeries"
          stacked = true
        }
      },
      # ── Row 3: S3 & DQ ──────────────────────────────────
      {
        type       = "metric"
        x = 0; y = 12; width = 12; height = 6
        properties = {
          title  = "DQ Pass Rate % (EcommerceDataQuality)"
          period = 300
          stat   = "Average"
          metrics = [
            ["EcommerceDataQuality", "PassRate", "Entity", "orders"],
            ["EcommerceDataQuality", "PassRate", "Entity", "payments"],
            ["EcommerceDataQuality", "PassRate", "Entity", "clickstream"],
          ]
          yAxis = { left = { min = 0, max = 100 } }
          view  = "timeSeries"
        }
      },
      {
        type       = "metric"
        x = 12; y = 12; width = 12; height = 6
        properties = {
          title  = "DQ Critical Failures"
          period = 300
          stat   = "Sum"
          metrics = [
            ["EcommerceDataQuality", "CriticalFailures", "Entity", "orders"],
            ["EcommerceDataQuality", "CriticalFailures", "Entity", "payments"],
          ]
          view = "timeSeries"
        }
      },
    ]
  })
}
