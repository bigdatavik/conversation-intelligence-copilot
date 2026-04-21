# Databricks notebook source
# MAGIC %md
# MAGIC # ⚠️ DO NOT RUN THIS NOTEBOOK ⚠️
# MAGIC
# MAGIC **This notebook contains UI instructions only - there is no executable code.**
# MAGIC
# MAGIC Follow the steps below manually in the Databricks UI to create the Knowledge Assistant and Supervisor Agent.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Knowledge Assistant & Supervisor Agent Setup
# MAGIC
# MAGIC This notebook provides instructions for creating the Knowledge Assistant and Supervisor Agent via the Databricks UI.
# MAGIC
# MAGIC **Prerequisites (completed by previous notebooks):**
# MAGIC - `06_export_transcripts.py` - Exported TXT files to Volume
# MAGIC - `07_genie_space.py` - Created Genie Space for analytics
# MAGIC
# MAGIC **What this notebook covers:**
# MAGIC 1. Create Knowledge Assistant (UI steps)
# MAGIC 2. Create Supervisor Agent (UI steps)
# MAGIC 3. Test queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites Check

# COMMAND ----------

CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"

# Check transcript files exist
transcript_volume = f"/Volumes/{CATALOG}/{SCHEMA}/transcripts"
files = dbutils.fs.ls(transcript_volume)
print(f"Transcript files ready: {len(files)} TXT files in {transcript_volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 1: Create Knowledge Assistant
# MAGIC
# MAGIC ## 1.1 Navigate to AI Hub
# MAGIC 1. In the left sidebar, click **AI Hub**
# MAGIC 2. Click **Create** → **Knowledge Assistant**
# MAGIC
# MAGIC ## 1.2 Basic Settings
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Name** | `Call Transcript Assistant` |
# MAGIC | **Description** | `Search member support call transcripts to find examples of compliance issues, agent behaviors, and call handling patterns.` |
# MAGIC
# MAGIC ## 1.3 Data Source
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Volume Path** | `/Volumes/humana_payer/conversation-intelligence-copilot/transcripts` |
# MAGIC | **Source Description** | See below |
# MAGIC
# MAGIC **Source Description:**
# MAGIC ```
# MAGIC Member support call transcripts between call center agents and health plan members.
# MAGIC Contains conversations about billing inquiries, coverage questions, claims status,
# MAGIC prescription issues, and enrollment changes. Each file includes metadata on
# MAGIC compliance scores, AI-detected compliance status, call reason, resolution, and agent information.
# MAGIC ```
# MAGIC
# MAGIC ## 1.4 Instructions
# MAGIC
# MAGIC Copy and paste these instructions:
# MAGIC
# MAGIC ```
# MAGIC You are a call quality assistant for a health plan call center.
# MAGIC
# MAGIC You have access to transcripts of member support calls between agents and members.
# MAGIC
# MAGIC Use these transcripts to:
# MAGIC - Find examples of calls with specific compliance issues
# MAGIC - Show how agents handle different call types
# MAGIC - Identify patterns in agent behavior
# MAGIC - Surface calls that need review
# MAGIC
# MAGIC When answering questions:
# MAGIC - Always cite the Call ID when referencing specific conversations
# MAGIC - Include relevant compliance scores and AI compliance status
# MAGIC - Summarize key points from transcripts concisely
# MAGIC - Note the call reason and resolution status when relevant
# MAGIC
# MAGIC Common compliance flags to look for:
# MAGIC - Missing recording notice
# MAGIC - Incomplete member verification
# MAGIC - Lack of empathy
# MAGIC - Abrupt closing
# MAGIC ```
# MAGIC
# MAGIC ## 1.5 Save and Deploy
# MAGIC - Click **Create**
# MAGIC - Wait for endpoint to provision (2-5 minutes)
# MAGIC - Note the **tile_id** for the Supervisor Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 2: Create Supervisor Agent
# MAGIC
# MAGIC ## 2.1 Navigate to AI Hub
# MAGIC 1. Click **AI Hub** in the left sidebar
# MAGIC 2. Click **Create** → **Supervisor Agent**
# MAGIC
# MAGIC ## 2.2 Basic Settings
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Name** | `Call Quality Intelligence Copilot` |
# MAGIC | **Description** | `Unified AI assistant for call quality analytics and transcript insights` |
# MAGIC
# MAGIC ## 2.3 Add Child Agents
# MAGIC
# MAGIC ### Agent 1: Analytics (Genie Space)
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Type** | Genie Space |
# MAGIC | **Name** | `analytics` |
# MAGIC | **Genie Space** | Select `Call Quality Intelligence Genie` |
# MAGIC | **Description** | `Handles metrics, KPIs, trends, and aggregate queries using SQL. Use for questions about compliance scores, resolution rates, agent performance rankings, and call volume trends.` |
# MAGIC
# MAGIC ### Agent 2: Knowledge (KA)
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Type** | Knowledge Assistant |
# MAGIC | **Name** | `transcripts` |
# MAGIC | **Knowledge Assistant** | Select `Call Transcript Assistant` |
# MAGIC | **Description** | `Searches call transcripts for examples, patterns, and qualitative insights. Use for questions about specific call content, agent behaviors, compliance issue examples, and call handling techniques.` |
# MAGIC
# MAGIC ## 2.4 Routing Instructions
# MAGIC
# MAGIC Copy and paste:
# MAGIC
# MAGIC ```
# MAGIC Route queries based on intent:
# MAGIC
# MAGIC -> analytics (Genie Space):
# MAGIC   - Metrics: "How many calls...", "What's the compliance rate..."
# MAGIC   - Rankings: "Top agents...", "Best performing...", "Lowest scoring..."
# MAGIC   - Trends: "Show trend...", "Compare this month...", "Over time..."
# MAGIC   - Aggregations: "Total calls...", "Average duration...", "Resolution rate..."
# MAGIC   - Filters: "Calls needing review...", "By call reason...", "By agent..."
# MAGIC
# MAGIC -> transcripts (Knowledge Assistant):
# MAGIC   - Examples: "Show me a call where...", "Find examples of..."
# MAGIC   - Techniques: "How do agents handle...", "What do agents say when..."
# MAGIC   - Patterns: "Common objections...", "Compliance issues look like..."
# MAGIC   - Specific calls: "What happened in call...", "Show transcript for..."
# MAGIC
# MAGIC For complex queries that need both analytics and examples:
# MAGIC 1. First query analytics for metrics/rankings
# MAGIC 2. Then query transcripts for specific examples
# MAGIC 3. Synthesize the response with both data and examples
# MAGIC ```
# MAGIC
# MAGIC ## 2.5 Save and Deploy
# MAGIC - Click **Create**
# MAGIC - Wait for endpoint to provision (2-5 minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Step 3: Test Queries
# MAGIC
# MAGIC ## Test Genie Space (Direct)
# MAGIC Open the Genie Space and try:
# MAGIC - "Which agents have the highest compliance scores?"
# MAGIC - "What is the resolution rate by call reason?"
# MAGIC - "Show me calls needing review"
# MAGIC
# MAGIC ## Test Knowledge Assistant
# MAGIC In the KA chat, try:
# MAGIC - "Find examples of calls with missing recording notice"
# MAGIC - "Show me a call where the agent handled a billing complaint well"
# MAGIC - "What does incomplete verification look like?"
# MAGIC
# MAGIC ## Test Supervisor Agent
# MAGIC In the Supervisor chat, try:
# MAGIC - "What's our overall compliance rate and show me an example of a low-scoring call"
# MAGIC - "Which agents need coaching? Give me specific examples of their issues"
# MAGIC - "What are the most common compliance flags? Show me examples of each"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Resource Summary
# MAGIC
# MAGIC | Resource | Type | Location |
# MAGIC |----------|------|----------|
# MAGIC | Genie Space | Analytics | `Call Quality Intelligence Genie` |
# MAGIC | Transcript Volume | Documents | `/Volumes/humana_payer/conversation-intelligence-copilot/transcripts/` |
# MAGIC | Knowledge Assistant | RAG | `Call Transcript Assistant` (created in Step 1) |
# MAGIC | Supervisor Agent | Multi-Agent | `Call Quality Intelligence Copilot` (created in Step 2) |
# MAGIC
# MAGIC ## Gold Tables (for Genie Space)
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `gold_call_summary` | Denormalized call details (one row per call) |
# MAGIC | `gold_agent_performance` | Agent-level quality KPIs |
# MAGIC | `gold_call_reason_metrics` | Metrics by call reason category |
# MAGIC | `gold_compliance_analysis` | Compliance flag patterns |

# COMMAND ----------

print("Setup instructions complete!")
print("\nNext steps:")
print("1. Create Knowledge Assistant via AI Hub (Step 1)")
print("2. Create Supervisor Agent via AI Hub (Step 2)")
print("3. Test with sample queries (Step 3)")
