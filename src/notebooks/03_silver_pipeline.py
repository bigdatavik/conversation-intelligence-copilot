# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Pipeline - AI-Enriched Member Support Call Data
# MAGIC
# MAGIC Applies AI Functions to enrich Bronze data with sentiment analysis, compliance classification, and summarization.
# MAGIC
# MAGIC **AI Functions Used:**
# MAGIC - `ai_analyze_sentiment()` - Sentiment on each utterance
# MAGIC - `ai_classify()` - Classify call reasons and compliance issues
# MAGIC - `ai_summarize()` - Generate call summaries
# MAGIC - `ai_extract()` - Extract compliance flags from transcripts
# MAGIC
# MAGIC **Output Tables:**
# MAGIC - `silver_calls` - Enriched call data with AI classifications
# MAGIC - `silver_utterances` - Utterances with sentiment analysis
# MAGIC - `silver_transcript_chunks` - Chunked transcripts for Vector Search

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
# MAGIC ## Build Full Transcripts for AI Processing

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read bronze tables
bronze_calls = spark.table("bronze_calls")
bronze_utterances = spark.table("bronze_utterances")

print(f"Bronze calls: {bronze_calls.count()}")
print(f"Bronze utterances: {bronze_utterances.count()}")

# COMMAND ----------

# Build full transcript text per call (ordered by sequence)
transcripts = bronze_utterances.orderBy("call_id", "sequence").groupBy("call_id").agg(
    concat_ws("\n",
        collect_list(
            concat(col("speaker_role"), lit(": "), col("text"))
        )
    ).alias("full_transcript")
)

# Join calls with transcripts
calls_with_transcripts = bronze_calls.join(transcripts, "call_id", "left")

print(f"Calls with transcripts: {calls_with_transcripts.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Silver Tables (Clean Slate)

# COMMAND ----------

# Drop existing silver tables to avoid schema conflicts
spark.sql("DROP TABLE IF EXISTS silver_utterances")
spark.sql("DROP TABLE IF EXISTS silver_calls")
spark.sql("DROP TABLE IF EXISTS silver_transcript_chunks")
print("Dropped existing silver tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Utterances - Sentiment Analysis

# COMMAND ----------

# Create temporary view for SQL-based AI functions
bronze_utterances.createOrReplaceTempView("bronze_utterances_view")

# Apply sentiment analysis using ai_analyze_sentiment
silver_utterances_df = spark.sql("""
SELECT
    call_id,
    agent_id,
    member_id,
    sequence,
    speaker,
    speaker_role,
    text,
    start_time,
    duration_seconds,
    ai_analyze_sentiment(text) AS sentiment,
    current_timestamp() AS enriched_at
FROM bronze_utterances_view
""")

silver_utterances_df.write.mode("overwrite").saveAsTable("silver_utterances")
print(f"Created silver_utterances: {spark.table('silver_utterances').count()} rows")

# COMMAND ----------

# Sentiment distribution
print("\n=== Sentiment Distribution ===")
spark.sql("""
SELECT sentiment, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
FROM silver_utterances
GROUP BY sentiment
ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Calls - AI Classifications for Quality Auditing

# COMMAND ----------

# MAGIC %md
# MAGIC Apply AI functions to:
# MAGIC 1. Classify call reasons (verify ground truth)
# MAGIC 2. Identify compliance issues
# MAGIC 3. Generate quality-focused summaries

# COMMAND ----------

# Create temporary view with transcripts
calls_with_transcripts.createOrReplaceTempView("calls_transcripts_view")

# Apply AI functions for compliance classification and summarization
silver_calls_df = spark.sql("""
SELECT
    call_id,
    transcript_source,
    call_start_time,
    call_end_time,
    duration_seconds,
    call_reason,
    resolution,
    csat_score,
    compliance_score,
    quality_notes,
    compliance_flags,
    agent_id,
    agent_name,
    member_id,
    member_name,
    plan_type,

    -- AI-classified call reason (verify/enhance ground truth)
    ai_classify(
        full_transcript,
        ARRAY('billing_inquiry', 'coverage_question', 'claims_status', 'prescription_issue', 'provider_lookup', 'enrollment_change', 'complaint', 'general_inquiry')
    ) AS ai_call_reason,

    -- AI-classified resolution status
    ai_classify(
        full_transcript,
        ARRAY('fully_resolved', 'partially_resolved', 'unresolved', 'escalated', 'callback_scheduled')
    ) AS ai_resolution,

    -- AI-identified compliance issues from transcript analysis
    ai_classify(
        full_transcript,
        ARRAY('compliant', 'missing_recording_notice', 'incomplete_verification', 'lacks_empathy', 'abrupt_closing', 'multiple_issues')
    ) AS ai_compliance_status,

    -- AI-generated quality-focused summary
    ai_summarize(full_transcript, 150) AS ai_summary,

    -- AI sentiment of overall call
    ai_analyze_sentiment(full_transcript) AS ai_overall_sentiment,

    full_transcript,
    current_timestamp() AS enriched_at
FROM calls_transcripts_view
WHERE full_transcript IS NOT NULL
""")

silver_calls_df.write.mode("overwrite").saveAsTable("silver_calls")
print(f"Created silver_calls: {spark.table('silver_calls').count()} rows")

# COMMAND ----------

# Compare ground truth vs AI classifications
print("\n=== Call Reason: Ground Truth vs AI ===")
spark.sql("""
SELECT
    call_reason AS ground_truth,
    ai_call_reason AS ai_predicted,
    COUNT(*) as count
FROM silver_calls
GROUP BY call_reason, ai_call_reason
ORDER BY count DESC
LIMIT 15
""").show(truncate=False)

# COMMAND ----------

print("\n=== Compliance Status Distribution ===")
spark.sql("""
SELECT
    ai_compliance_status,
    COUNT(*) as count,
    ROUND(AVG(compliance_score), 1) as avg_ground_truth_score,
    ROUND(AVG(csat_score), 2) as avg_csat
FROM silver_calls
GROUP BY ai_compliance_status
ORDER BY count DESC
""").show(truncate=False)

# COMMAND ----------

# Sample AI summaries
print("\n=== Sample AI Summaries ===")
spark.sql("""
SELECT call_id, call_reason, resolution, ai_summary
FROM silver_calls
ORDER BY call_id
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transcript Chunks - For Vector Search / Knowledge Assistant

# COMMAND ----------

# MAGIC %md
# MAGIC Create chunked transcripts for semantic search and RAG applications.

# COMMAND ----------

# Read silver calls for chunking
silver_calls = spark.table("silver_calls")

# Create chunks from transcript
import builtins

def chunk_transcript(transcript, chunk_size=500, overlap=100):
    """Split transcript into overlapping chunks."""
    if not transcript:
        return []

    chunks = []
    start = 0
    chunk_num = 0

    while start < len(transcript):
        end = start + chunk_size
        chunk_text = transcript[start:end]

        # Try to end at sentence boundary
        if end < len(transcript):
            last_period = chunk_text.rfind('.')
            last_newline = chunk_text.rfind('\n')
            break_point = builtins.max(last_period, last_newline)
            if break_point > chunk_size // 2:
                chunk_text = chunk_text[:break_point + 1]
                end = start + break_point + 1

        chunks.append({
            "chunk_num": chunk_num,
            "chunk_text": chunk_text.strip()
        })

        chunk_num += 1
        start = end - overlap

        if start >= len(transcript):
            break

    return chunks

# Register UDF
chunk_schema = ArrayType(StructType([
    StructField("chunk_num", IntegerType(), False),
    StructField("chunk_text", StringType(), False)
]))
chunk_udf = udf(chunk_transcript, chunk_schema)

# Apply chunking
chunks_df = silver_calls.select(
    "call_id",
    "transcript_source",
    "call_start_time",
    "call_reason",
    "resolution",
    "compliance_score",
    "ai_compliance_status",
    "ai_summary",
    "agent_id",
    "agent_name",
    "member_id",
    "plan_type",
    explode(chunk_udf(col("full_transcript"))).alias("chunk")
).select(
    concat(col("call_id"), lit("_"), col("chunk.chunk_num")).alias("chunk_id"),
    "call_id",
    col("chunk.chunk_num").alias("chunk_num"),
    col("chunk.chunk_text").alias("chunk_text"),
    "transcript_source",
    "call_start_time",
    "call_reason",
    "resolution",
    "compliance_score",
    "ai_compliance_status",
    "ai_summary",
    "agent_id",
    "agent_name",
    "member_id",
    "plan_type",
    current_timestamp().alias("created_at")
)

chunks_df.write.mode("overwrite").saveAsTable("silver_transcript_chunks")
print(f"Created silver_transcript_chunks: {spark.table('silver_transcript_chunks').count()} rows")

# COMMAND ----------

# Sample chunks
print("\n=== Sample Transcript Chunks ===")
spark.sql("""
SELECT chunk_id, call_id, chunk_num,
       LEFT(chunk_text, 150) as chunk_preview,
       call_reason, compliance_score
FROM silver_transcript_chunks
WHERE call_id IN (SELECT call_id FROM silver_transcript_chunks GROUP BY call_id LIMIT 2)
ORDER BY call_id, chunk_num
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Summary of Silver tables
tables = ["silver_calls", "silver_utterances", "silver_transcript_chunks"]

print("=== Silver Layer Summary ===")
for table in tables:
    count = spark.table(table).count()
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# AI classification accuracy (compared to ground truth)
print("\n=== AI Classification Accuracy ===")
spark.sql("""
SELECT
    'Call Reason' as classification,
    COUNT(*) as total,
    SUM(CASE WHEN call_reason = ai_call_reason THEN 1 ELSE 0 END) as matches,
    ROUND(SUM(CASE WHEN call_reason = ai_call_reason THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as accuracy_pct
FROM silver_calls
""").show()

# COMMAND ----------

# Compliance score correlation with AI status
print("\n=== Compliance Score by AI Status ===")
spark.sql("""
SELECT
    ai_compliance_status,
    COUNT(*) as calls,
    ROUND(MIN(compliance_score), 0) as min_score,
    ROUND(AVG(compliance_score), 1) as avg_score,
    ROUND(MAX(compliance_score), 0) as max_score
FROM silver_calls
GROUP BY ai_compliance_status
ORDER BY avg_score DESC
""").show()

# COMMAND ----------

# Sentiment by speaker role
print("\n=== Sentiment by Speaker Role ===")
spark.sql("""
SELECT speaker_role, sentiment, COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY speaker_role), 1) as pct_within_role
FROM silver_utterances
GROUP BY speaker_role, sentiment
ORDER BY speaker_role, count DESC
""").show()

# COMMAND ----------

print("\nSilver pipeline complete!")
