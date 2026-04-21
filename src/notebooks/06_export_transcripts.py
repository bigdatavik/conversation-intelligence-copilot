# Databricks notebook source
# MAGIC %md
# MAGIC # Export Transcripts to TXT for Knowledge Assistant
# MAGIC
# MAGIC Converts JSON transcripts to plain text files for RAG/Knowledge Assistant integration.
# MAGIC
# MAGIC **Input:**
# MAGIC - `silver_calls` table with full transcripts
# MAGIC
# MAGIC **Output:**
# MAGIC - TXT files in `/Volumes/humana_payer/conversation-intelligence-copilot/transcripts/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"
VOLUME_NAME = "transcripts"
OUTPUT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

print(f"Output path: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Transcripts

# COMMAND ----------

# Create the transcripts volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.`{SCHEMA}`.{VOLUME_NAME}")
print(f"Created/verified volume: {CATALOG}.`{SCHEMA}`.{VOLUME_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Transcripts as TXT Files

# COMMAND ----------

import os

# Read silver_calls with full transcripts
silver_calls = spark.table(f"{CATALOG}.`{SCHEMA}`.silver_calls")
calls_data = silver_calls.select(
    "call_id",
    "transcript_source",
    "call_start_time",
    "call_reason",
    "resolution",
    "compliance_score",
    "ai_compliance_status",
    "ai_summary",
    "agent_name",
    "member_name",
    "plan_type",
    "full_transcript"
).collect()

print(f"Exporting {len(calls_data)} transcripts...")

# COMMAND ----------

# Export each call as a TXT file with metadata header
for row in calls_data:
    call_id = row["call_id"]

    # Build the text content with metadata header
    content = f"""CALL TRANSCRIPT
================================================================================
Call ID: {row["call_id"]}
Date: {row["call_start_time"]}
Source: {row["transcript_source"]}

Agent: {row["agent_name"]}
Member: {row["member_name"]}
Plan Type: {row["plan_type"]}

Call Reason: {row["call_reason"]}
Resolution: {row["resolution"]}
Compliance Score: {row["compliance_score"]}
AI Compliance Status: {row["ai_compliance_status"]}

AI Summary:
{row["ai_summary"]}

================================================================================
TRANSCRIPT
================================================================================

{row["full_transcript"]}

================================================================================
END OF TRANSCRIPT
================================================================================
"""

    # Write to file
    file_path = f"{OUTPUT_PATH}/{call_id}.txt"
    dbutils.fs.put(file_path, content, overwrite=True)

print(f"Exported {len(calls_data)} transcript files to {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# List files in the output directory
files = dbutils.fs.ls(OUTPUT_PATH)
print(f"Total files: {len(files)}")
print("\nFirst 10 files:")
for f in files[:10]:
    print(f"  {f.name} ({f.size} bytes)")

# COMMAND ----------

# Preview one transcript
if files:
    sample_content = dbutils.fs.head(files[0].path, 2000)
    print("Sample transcript content:")
    print("=" * 80)
    print(sample_content)
    print("..." if len(sample_content) >= 2000 else "")

# COMMAND ----------

print(f"\nTranscripts exported successfully!")
print(f"Volume path: {OUTPUT_PATH}")
print(f"Total files: {len(files)}")
print("\nYou can now create a Knowledge Assistant pointing to this volume.")
