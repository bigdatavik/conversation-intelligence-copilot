# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Pipeline - Quality Analytics Aggregates
# MAGIC
# MAGIC Creates aggregate tables for quality auditing dashboards and Genie Space analytics.
# MAGIC
# MAGIC **Input Tables:**
# MAGIC - `silver_calls` - Enriched call data with AI classifications
# MAGIC - `silver_utterances` - Utterances with sentiment
# MAGIC - `bronze_agents`, `bronze_members`
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `gold_call_summary` - Denormalized call details (one row per call)
# MAGIC - `gold_agent_performance` - Agent-level quality KPIs
# MAGIC - `gold_call_reason_metrics` - Metrics by call reason
# MAGIC - `gold_compliance_analysis` - Compliance flag patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"

print(f"Source/Target: {CATALOG}.`{SCHEMA}`")

# Set current catalog and schema
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Gold Tables

# COMMAND ----------

gold_tables = ["gold_call_summary", "gold_agent_performance", "gold_call_reason_metrics", "gold_compliance_analysis"]
for table in gold_tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
print("Dropped existing gold tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Call Summary - Denormalized Call Details

# COMMAND ----------

# Create denormalized view with all call details
spark.sql("""
CREATE TABLE gold_call_summary AS
SELECT
    c.call_id,
    c.transcript_source,
    DATE(c.call_start_time) AS call_date,
    c.call_start_time,
    c.call_end_time,
    ROUND(c.duration_seconds / 60.0, 1) AS duration_minutes,

    -- Call categorization
    c.call_reason,
    c.ai_call_reason,
    c.resolution,
    c.ai_resolution,

    -- Quality metrics
    c.csat_score,
    c.compliance_score,
    c.ai_compliance_status,
    c.ai_overall_sentiment,
    c.quality_notes,
    c.compliance_flags,
    c.ai_summary,

    -- Agent details
    c.agent_id,
    c.agent_name,
    a.team AS agent_team,
    a.tenure_months AS agent_tenure_months,

    -- Member details
    c.member_id,
    c.member_name,
    c.plan_type,
    m.age AS member_age,

    -- Derived metrics for analytics
    CASE WHEN c.resolution = 'resolved' THEN 1 ELSE 0 END AS is_resolved,
    CASE WHEN c.resolution = 'escalated_resolved' THEN 1 ELSE 0 END AS is_escalated,
    CASE WHEN c.compliance_score >= 90 THEN 1 ELSE 0 END AS is_compliant,
    CASE WHEN c.compliance_score < 70 THEN 1 ELSE 0 END AS needs_review,
    CASE WHEN SIZE(c.compliance_flags) > 0 THEN 1 ELSE 0 END AS has_compliance_flags,
    SIZE(c.compliance_flags) AS compliance_flag_count

FROM silver_calls c
LEFT JOIN bronze_agents a ON c.agent_id = a.agent_id
LEFT JOIN bronze_members m ON c.member_id = m.member_id
""")

print(f"Created gold_call_summary: {spark.table('gold_call_summary').count()} rows")

# COMMAND ----------

# Sample data
spark.sql("SELECT call_id, call_reason, resolution, compliance_score, ai_compliance_status, agent_name FROM gold_call_summary LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Agent Performance - Agent-Level Quality KPIs

# COMMAND ----------

spark.sql("""
CREATE TABLE gold_agent_performance AS
SELECT
    agent_id,
    agent_name,
    agent_team,
    agent_tenure_months,

    -- Volume metrics
    COUNT(*) AS total_calls,
    SUM(is_resolved) AS resolved_calls,
    SUM(is_escalated) AS escalated_calls,

    -- Quality metrics
    ROUND(AVG(compliance_score), 1) AS avg_compliance_score,
    ROUND(AVG(csat_score), 2) AS avg_csat,
    ROUND(AVG(duration_minutes), 1) AS avg_call_duration,

    -- Performance rates
    ROUND(SUM(is_resolved) * 100.0 / COUNT(*), 1) AS resolution_rate,
    ROUND(SUM(is_compliant) * 100.0 / COUNT(*), 1) AS compliance_rate,
    ROUND(SUM(has_compliance_flags) * 100.0 / COUNT(*), 1) AS flag_rate,

    -- Compliance breakdown
    SUM(is_compliant) AS compliant_calls,
    SUM(needs_review) AS calls_needing_review,
    SUM(CASE WHEN compliance_score BETWEEN 70 AND 89 THEN 1 ELSE 0 END) AS fair_compliance_calls,

    -- Call reason distribution (top reason)
    MODE(call_reason) AS most_common_call_reason,

    -- AI-detected compliance issues
    MODE(ai_compliance_status) AS most_common_ai_status

FROM gold_call_summary
GROUP BY agent_id, agent_name, agent_team, agent_tenure_months
ORDER BY total_calls DESC
""")

print(f"Created gold_agent_performance: {spark.table('gold_agent_performance').count()} rows")

# COMMAND ----------

# Agent performance summary
print("\n=== Agent Performance Summary ===")
spark.sql("""
SELECT agent_name, agent_team, total_calls, avg_compliance_score, avg_csat, resolution_rate, compliance_rate
FROM gold_agent_performance
ORDER BY avg_compliance_score DESC
LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Call Reason Metrics - Breakdown by Call Reason

# COMMAND ----------

spark.sql("""
CREATE TABLE gold_call_reason_metrics AS
SELECT
    call_reason,

    -- Volume
    COUNT(*) AS total_calls,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total,

    -- Resolution
    SUM(is_resolved) AS resolved_count,
    ROUND(SUM(is_resolved) * 100.0 / COUNT(*), 1) AS resolution_rate,
    SUM(is_escalated) AS escalation_count,
    ROUND(SUM(is_escalated) * 100.0 / COUNT(*), 1) AS escalation_rate,

    -- Quality
    ROUND(AVG(compliance_score), 1) AS avg_compliance_score,
    ROUND(AVG(csat_score), 2) AS avg_csat,

    -- Call handling
    ROUND(AVG(duration_minutes), 1) AS avg_duration_minutes,
    ROUND(MIN(duration_minutes), 1) AS min_duration,
    ROUND(MAX(duration_minutes), 1) AS max_duration,

    -- Compliance
    ROUND(SUM(has_compliance_flags) * 100.0 / COUNT(*), 1) AS flag_rate,

    -- Transcript sources
    SUM(CASE WHEN transcript_source = 'Balto' THEN 1 ELSE 0 END) AS balto_calls,
    SUM(CASE WHEN transcript_source = 'Genesys' THEN 1 ELSE 0 END) AS genesys_calls

FROM gold_call_summary
GROUP BY call_reason
ORDER BY total_calls DESC
""")

print(f"Created gold_call_reason_metrics: {spark.table('gold_call_reason_metrics').count()} rows")

# COMMAND ----------

print("\n=== Call Reason Metrics ===")
spark.sql("""
SELECT call_reason, total_calls, pct_of_total, resolution_rate, avg_compliance_score, avg_csat, avg_duration_minutes
FROM gold_call_reason_metrics
ORDER BY total_calls DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Compliance Analysis - Compliance Flag Patterns

# COMMAND ----------

from pyspark.sql.functions import *

# Read silver_calls to explode compliance_flags
silver_calls = spark.table("silver_calls")

# Explode compliance flags and analyze
compliance_flags_df = silver_calls.select(
    "call_id",
    "agent_id",
    "agent_name",
    "call_reason",
    "compliance_score",
    "csat_score",
    explode_outer("compliance_flags").alias("flag")
).filter(col("flag").isNotNull())

compliance_flags_df.createOrReplaceTempView("compliance_flags_view")

spark.sql("""
CREATE TABLE gold_compliance_analysis AS
SELECT
    flag AS compliance_flag,

    -- Occurrence
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT call_id) AS affected_calls,
    COUNT(DISTINCT agent_id) AS agents_with_flag,

    -- Impact
    ROUND(AVG(compliance_score), 1) AS avg_compliance_when_flagged,
    ROUND(AVG(csat_score), 2) AS avg_csat_when_flagged,

    -- Call reasons where this flag appears most
    COLLECT_SET(call_reason) AS call_reasons,
    MODE(call_reason) AS most_common_call_reason,

    -- Agents who frequently have this flag
    MODE(agent_name) AS most_frequent_agent

FROM compliance_flags_view
GROUP BY flag
ORDER BY occurrence_count DESC
""")

print(f"Created gold_compliance_analysis: {spark.table('gold_compliance_analysis').count()} rows")

# COMMAND ----------

print("\n=== Compliance Flag Analysis ===")
spark.sql("""
SELECT
    compliance_flag,
    occurrence_count,
    affected_calls,
    agents_with_flag,
    avg_compliance_when_flagged,
    avg_csat_when_flagged,
    most_common_call_reason
FROM gold_compliance_analysis
ORDER BY occurrence_count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Summary of Gold tables
tables = ["gold_call_summary", "gold_agent_performance", "gold_call_reason_metrics", "gold_compliance_analysis"]

print("=== Gold Layer Summary ===")
for table in tables:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# Key metrics summary
print("\n=== Key Quality Metrics ===")
spark.sql("""
SELECT
    COUNT(*) AS total_calls,
    SUM(is_resolved) AS resolved_calls,
    ROUND(SUM(is_resolved) * 100.0 / COUNT(*), 1) AS resolution_rate,
    ROUND(AVG(compliance_score), 1) AS avg_compliance_score,
    SUM(is_compliant) AS compliant_calls,
    ROUND(SUM(is_compliant) * 100.0 / COUNT(*), 1) AS compliance_rate,
    SUM(needs_review) AS calls_needing_review,
    ROUND(AVG(csat_score), 2) AS overall_csat,
    COUNT(DISTINCT agent_id) AS active_agents
FROM gold_call_summary
""").show()

# COMMAND ----------

# Compliance distribution
print("\n=== Compliance Score Distribution ===")
spark.sql("""
SELECT
    CASE
        WHEN compliance_score >= 90 THEN 'Excellent (90+)'
        WHEN compliance_score >= 80 THEN 'Good (80-89)'
        WHEN compliance_score >= 70 THEN 'Fair (70-79)'
        ELSE 'Needs Improvement (<70)'
    END AS compliance_tier,
    COUNT(*) AS call_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM gold_call_summary
GROUP BY 1
ORDER BY 1
""").show()

# COMMAND ----------

print("\nGold pipeline complete!")
