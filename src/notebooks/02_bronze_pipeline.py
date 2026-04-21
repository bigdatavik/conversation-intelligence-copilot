# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Pipeline - Member Support Call Ingestion
# MAGIC
# MAGIC Ingests raw JSON transcripts from Volume into Bronze tables.
# MAGIC
# MAGIC **Input:** `/Volumes/humana_payer/conversation-intelligence-copilot/raw_data/transcripts/`
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `bronze_calls` - Call-level metadata with compliance scores
# MAGIC - `bronze_utterances` - Individual utterances from calls
# MAGIC - `bronze_agents` - Agent reference data
# MAGIC - `bronze_members` - Member reference data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/transcripts"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Source: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw JSON Transcripts

# COMMAND ----------

# Read all JSON files from Volume
raw_df = spark.read.option("multiLine", "true").json(f"{VOLUME_PATH}/*.json")

print(f"Total transcripts: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Calls

# COMMAND ----------

# Drop existing table to avoid schema merge conflicts
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.`{SCHEMA}`.bronze_calls")

bronze_calls = raw_df.select(
    F.col("call_id"),
    F.col("transcript_source"),
    F.col("metadata.call_start_time").cast("timestamp").alias("call_start_time"),
    F.col("metadata.call_end_time").cast("timestamp").alias("call_end_time"),
    F.col("metadata.duration_seconds").cast("int").alias("duration_seconds"),
    F.col("metadata.call_reason"),
    F.col("metadata.resolution"),
    F.col("metadata.csat_score").cast("int").alias("csat_score"),
    F.col("quality_audit.compliance_score").cast("int").alias("compliance_score"),
    F.col("quality_audit.quality_notes"),
    F.col("quality_audit.compliance_flags"),
    F.col("agent.agent_id"),
    F.col("agent.name").alias("agent_name"),
    F.col("member.member_id"),
    F.col("member.name").alias("member_name"),
    F.col("member.plan_type"),
    F.current_timestamp().alias("ingested_at")
)

bronze_calls.write.mode("overwrite").saveAsTable(f"{CATALOG}.`{SCHEMA}`.bronze_calls")
print(f"Created bronze_calls with {bronze_calls.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Utterances

# COMMAND ----------

# Drop existing table to avoid schema merge conflicts
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.`{SCHEMA}`.bronze_utterances")

# Explode utterances from each call
bronze_utterances = raw_df.select(
    F.col("call_id"),
    F.col("agent.agent_id"),
    F.col("member.member_id"),
    F.explode("utterances").alias("utterance")
).select(
    F.col("call_id"),
    F.col("agent_id"),
    F.col("member_id"),
    F.col("utterance.sequence").cast("int").alias("sequence"),
    F.col("utterance.speaker"),
    F.col("utterance.speaker_role"),
    F.col("utterance.text"),
    F.col("utterance.start_time").cast("timestamp").alias("start_time"),
    F.col("utterance.duration_seconds").cast("double").alias("duration_seconds"),
    F.current_timestamp().alias("ingested_at")
)

bronze_utterances.write.mode("overwrite").saveAsTable(f"{CATALOG}.`{SCHEMA}`.bronze_utterances")
print(f"Created bronze_utterances with {bronze_utterances.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Agents

# COMMAND ----------

# Drop existing table to avoid schema merge conflicts
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.`{SCHEMA}`.bronze_agents")

# Extract unique agents
bronze_agents = raw_df.select(
    F.col("agent.agent_id"),
    F.col("agent.name"),
    F.col("agent.team"),
    F.col("agent.tenure_months").cast("int").alias("tenure_months")
).dropDuplicates(["agent_id"])

bronze_agents.write.mode("overwrite").saveAsTable(f"{CATALOG}.`{SCHEMA}`.bronze_agents")
print(f"Created bronze_agents with {bronze_agents.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Members

# COMMAND ----------

# Drop existing table to avoid schema merge conflicts
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.`{SCHEMA}`.bronze_members")

# Extract unique members
bronze_members = raw_df.select(
    F.col("member.member_id"),
    F.col("member.name"),
    F.col("member.age").cast("int").alias("age"),
    F.col("member.plan_type"),
    F.col("member.member_since").cast("date").alias("member_since")
).dropDuplicates(["member_id"])

bronze_members.write.mode("overwrite").saveAsTable(f"{CATALOG}.`{SCHEMA}`.bronze_members")
print(f"Created bronze_members with {bronze_members.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=== Bronze Layer Summary ===")
tables = ["bronze_calls", "bronze_utterances", "bronze_agents", "bronze_members"]
for table in tables:
    count = spark.table(f"{CATALOG}.`{SCHEMA}`.{table}").count()
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# Sample data
print("\n=== Sample Call ===")
spark.table(f"{CATALOG}.`{SCHEMA}`.bronze_calls").select(
    "call_id", "call_reason", "resolution", "csat_score", "compliance_score", "agent_name"
).show(5, truncate=False)

# COMMAND ----------

# Compliance distribution
print("=== Compliance Score Distribution ===")
spark.sql(f"""
    SELECT
        CASE
            WHEN compliance_score >= 90 THEN 'Excellent (90+)'
            WHEN compliance_score >= 80 THEN 'Good (80-89)'
            WHEN compliance_score >= 70 THEN 'Fair (70-79)'
            ELSE 'Needs Improvement (<70)'
        END AS compliance_tier,
        COUNT(*) AS call_count,
        ROUND(AVG(csat_score), 2) AS avg_csat
    FROM {CATALOG}.`{SCHEMA}`.bronze_calls
    GROUP BY 1
    ORDER BY 1
""").show()

# COMMAND ----------

# Call reason distribution
print("=== Call Reason Distribution ===")
spark.sql(f"""
    SELECT
        call_reason,
        COUNT(*) AS call_count,
        ROUND(AVG(compliance_score), 1) AS avg_compliance,
        ROUND(AVG(csat_score), 2) AS avg_csat
    FROM {CATALOG}.`{SCHEMA}`.bronze_calls
    GROUP BY call_reason
    ORDER BY call_count DESC
""").show()

# COMMAND ----------

print("\nBronze pipeline complete!")
