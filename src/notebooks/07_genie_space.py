# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Setup - Call Quality Intelligence
# MAGIC
# MAGIC Creates the Genie Space programmatically for natural language analytics on call quality data.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Gold tables created by `04_gold_pipeline.py`
# MAGIC - SQL Warehouse available
# MAGIC
# MAGIC **Output:**
# MAGIC - Genie Space: `Call Quality Intelligence Genie`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import uuid
import json
import requests

# Configuration
CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"
GENIE_SPACE_NAME = "Call Quality Intelligence Genie"
GENIE_SPACE_DESCRIPTION = "Natural language analytics for call quality metrics. Ask questions about compliance scores, agent performance, call reasons, and resolution rates."

# Get workspace URL and token
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
if not workspace_host.startswith("https://"):
    workspace_host = f"https://{workspace_host}"

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

print(f"Workspace: {workspace_host}")
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def _id():
    """Generate a lowercase 32-hex UUID without hyphens (required by Genie API)."""
    return uuid.uuid4().hex

def get_warehouse_id():
    """Get the first available SQL warehouse ID."""
    response = requests.get(
        f"{workspace_host}/api/2.0/sql/warehouses",
        headers=headers
    )
    warehouses = response.json().get("warehouses", [])
    for wh in warehouses:
        if wh.get("state") == "RUNNING":
            return wh["id"]
    # Return first warehouse if none running
    if warehouses:
        return warehouses[0]["id"]
    raise Exception("No SQL warehouse found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Tables for Genie Space

# COMMAND ----------

# Gold tables to expose in Genie Space
tables = [
    {
        "identifier": f"{CATALOG}.`{SCHEMA}`.gold_call_summary",
        "description": "Denormalized call details with one row per call. Contains call_id, call_date, agent_name, member_name, call_reason, resolution, compliance_score, ai_compliance_status, csat_score, duration_minutes, and derived flags like is_resolved, needs_review."
    },
    {
        "identifier": f"{CATALOG}.`{SCHEMA}`.gold_agent_performance",
        "description": "Agent-level quality KPIs including total_calls, resolved_calls, avg_compliance_score, avg_csat, resolution_rate, compliance_rate, and calls_needing_review for each agent."
    },
    {
        "identifier": f"{CATALOG}.`{SCHEMA}`.gold_call_reason_metrics",
        "description": "Metrics aggregated by call reason category. Shows total_calls, resolution_rate, escalation_rate, avg_compliance_score, avg_csat, and avg_duration for each call reason like billing, claims, coverage, enrollment, prescription."
    },
    {
        "identifier": f"{CATALOG}.`{SCHEMA}`.gold_compliance_analysis",
        "description": "Compliance flag analysis showing occurrence_count, affected_calls, agents_with_flag, avg_compliance_when_flagged, and avg_csat_when_flagged for each compliance issue type."
    }
]

# Sort tables by identifier (required by API)
tables_sorted = sorted(tables, key=lambda x: x["identifier"])

print("Tables configured:")
for t in tables_sorted:
    print(f"  - {t['identifier']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Sample Questions

# COMMAND ----------

sample_questions = [
    {"id": _id(), "question": ["Which agents have the highest compliance scores?"]},
    {"id": _id(), "question": ["Show me calls that need compliance review"]},
    {"id": _id(), "question": ["What is the resolution rate by call reason?"]},
    {"id": _id(), "question": ["Which agents need coaching based on their metrics?"]},
    {"id": _id(), "question": ["What are the most common compliance issues?"]},
    {"id": _id(), "question": ["Average CSAT score by agent team"]},
    {"id": _id(), "question": ["How many calls were escalated this month?"]},
    {"id": _id(), "question": ["Show me agent performance rankings"]},
]

# Sort by id (required by API)
sample_questions = sorted(sample_questions, key=lambda x: x["id"])

print(f"Sample questions: {len(sample_questions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define SQL Snippets for Better Query Generation

# COMMAND ----------

# Filters - common WHERE clause patterns
sql_filters = [
    {"id": _id(), "sql": ["needs_review = 1"], "display_name": "Calls Needing Review",
     "instruction": ["Use when user asks for calls that need review, low compliance, or problem calls"],
     "synonyms": ["problem calls", "low compliance", "issues", "needs attention"]},
    {"id": _id(), "sql": ["compliance_score >= 90"], "display_name": "Compliant Calls",
     "instruction": ["Use when filtering for high compliance or good calls"],
     "synonyms": ["good compliance", "passing", "high score"]},
    {"id": _id(), "sql": ["compliance_score < 70"], "display_name": "Low Compliance Calls",
     "instruction": ["Use when filtering for low compliance scores"],
     "synonyms": ["failing", "poor compliance", "bad score"]},
    {"id": _id(), "sql": ["is_resolved = 1"], "display_name": "Resolved Calls",
     "instruction": ["Use when filtering for successfully resolved calls"],
     "synonyms": ["completed", "successful", "fixed"]},
]
sql_filters = sorted(sql_filters, key=lambda x: x["id"])

# Expressions - common SELECT expressions/dimensions
sql_expressions = [
    {"id": _id(), "sql": ["agent_name"], "display_name": "Agent Name",
     "instruction": ["Use for grouping by agent"],
     "synonyms": ["agent", "rep", "representative"]},
    {"id": _id(), "sql": ["call_reason"], "display_name": "Call Reason",
     "instruction": ["Use for grouping by call reason category"],
     "synonyms": ["reason", "category", "type of call"]},
    {"id": _id(), "sql": ["agent_team"], "display_name": "Agent Team",
     "instruction": ["Use for grouping by team"],
     "synonyms": ["team", "department", "group"]},
    {"id": _id(), "sql": ["call_date"], "display_name": "Call Date",
     "instruction": ["Use for time-based analysis"],
     "synonyms": ["date", "day", "when"]},
]
sql_expressions = sorted(sql_expressions, key=lambda x: x["id"])

# Measures - common aggregate calculations
sql_measures = [
    {"id": _id(), "sql": ["AVG(compliance_score)"], "display_name": "Average Compliance Score",
     "instruction": ["Use for calculating average compliance"],
     "synonyms": ["avg compliance", "mean compliance", "compliance average"]},
    {"id": _id(), "sql": ["AVG(csat_score)"], "display_name": "Average CSAT",
     "instruction": ["Use for calculating average customer satisfaction"],
     "synonyms": ["avg csat", "satisfaction", "customer rating"]},
    {"id": _id(), "sql": ["SUM(is_resolved) * 100.0 / COUNT(*)"], "display_name": "Resolution Rate",
     "instruction": ["Use for calculating percentage of resolved calls"],
     "synonyms": ["resolution %", "resolved percentage", "success rate"]},
    {"id": _id(), "sql": ["COUNT(*)"], "display_name": "Total Calls",
     "instruction": ["Use for counting calls"],
     "synonyms": ["call count", "number of calls", "volume"]},
    {"id": _id(), "sql": ["SUM(needs_review)"], "display_name": "Calls Needing Review",
     "instruction": ["Use for counting calls that need compliance review"],
     "synonyms": ["review count", "problem count"]},
]
sql_measures = sorted(sql_measures, key=lambda x: x["id"])

print(f"SQL snippets: {len(sql_filters)} filters, {len(sql_expressions)} expressions, {len(sql_measures)} measures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Example Question-SQL Pairs

# COMMAND ----------

example_question_sqls = [
    {
        "id": _id(),
        "question": ["Which agents have the highest compliance scores?"],
        "sql": [f"SELECT agent_name, avg_compliance_score, total_calls, compliance_rate FROM `{CATALOG}`.`{SCHEMA}`.gold_agent_performance ORDER BY avg_compliance_score DESC LIMIT 10"]
    },
    {
        "id": _id(),
        "question": ["Show me calls that need compliance review"],
        "sql": [f"SELECT call_id, call_date, agent_name, call_reason, compliance_score, ai_compliance_status FROM `{CATALOG}`.`{SCHEMA}`.gold_call_summary WHERE needs_review = 1 ORDER BY compliance_score ASC LIMIT 20"]
    },
    {
        "id": _id(),
        "question": ["What is the resolution rate by call reason?"],
        "sql": [f"SELECT call_reason, total_calls, resolution_rate, avg_compliance_score, avg_csat FROM `{CATALOG}`.`{SCHEMA}`.gold_call_reason_metrics ORDER BY resolution_rate DESC"]
    },
    {
        "id": _id(),
        "question": ["What are the most common compliance issues?"],
        "sql": [f"SELECT compliance_flag, occurrence_count, affected_calls, avg_compliance_when_flagged, avg_csat_when_flagged FROM `{CATALOG}`.`{SCHEMA}`.gold_compliance_analysis ORDER BY occurrence_count DESC"]
    },
    {
        "id": _id(),
        "question": ["Which agents need coaching?"],
        "sql": [f"SELECT agent_name, agent_team, total_calls, avg_compliance_score, avg_csat, calls_needing_review FROM `{CATALOG}`.`{SCHEMA}`.gold_agent_performance WHERE avg_compliance_score < 80 OR calls_needing_review > 5 ORDER BY avg_compliance_score ASC"]
    },
]
example_question_sqls = sorted(example_question_sqls, key=lambda x: x["id"])

print(f"Example question-SQL pairs: {len(example_question_sqls)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Text Instructions

# COMMAND ----------

text_instructions = [
    {
        "id": _id(),
        "instruction": [
            "You are a call quality analytics assistant for a healthcare payer contact center.",
            "Answer questions about agent performance, compliance scores, call reasons, and resolution rates.",
            "Use the gold tables which contain pre-aggregated metrics.",
            "gold_call_summary has one row per call with all details.",
            "gold_agent_performance has agent-level KPIs.",
            "gold_call_reason_metrics has metrics by call reason category.",
            "gold_compliance_analysis has compliance flag patterns."
        ]
    },
    {
        "id": _id(),
        "instruction": [
            "When asked about agents needing coaching, look for agents with avg_compliance_score < 80 or calls_needing_review > 5.",
            "When asked about calls needing review, use needs_review = 1 filter.",
            "Compliance scores: 90+ is excellent, 70-89 is acceptable, below 70 needs review."
        ]
    }
]
text_instructions = sorted(text_instructions, key=lambda x: x["id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Serialized Space (Version 2)

# COMMAND ----------

serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": sample_questions
    },
    "data_sources": {
        "tables": tables_sorted,
        "metric_views": []
    },
    "instructions": {
        "text_instructions": text_instructions,
        "example_question_sqls": example_question_sqls,
        "join_specs": [],  # No joins needed - using denormalized gold tables
        "sql_snippets": {
            "filters": sql_filters,
            "expressions": sql_expressions,
            "measures": sql_measures
        }
    },
    "benchmarks": {
        "questions": []
    }
}

print("Serialized space built successfully")
print(f"  Tables: {len(tables_sorted)}")
print(f"  Sample questions: {len(sample_questions)}")
print(f"  Example SQL pairs: {len(example_question_sqls)}")
print(f"  Text instructions: {len(text_instructions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space

# COMMAND ----------

# Get warehouse ID
warehouse_id = get_warehouse_id()
print(f"Using warehouse: {warehouse_id}")

# Build the API payload
payload = {
    "display_name": GENIE_SPACE_NAME,
    "description": GENIE_SPACE_DESCRIPTION,
    "warehouse_id": warehouse_id,
    "serialized_space": json.dumps(serialized_space)
}

# Check if Genie Space already exists
list_response = requests.get(
    f"{workspace_host}/api/2.0/genie/spaces",
    headers=headers
)

existing_space = None
if list_response.status_code == 200:
    spaces = list_response.json().get("spaces", [])
    for space in spaces:
        if space.get("display_name") == GENIE_SPACE_NAME:
            existing_space = space
            break

if existing_space:
    # Update existing space
    space_id = existing_space["space_id"]
    print(f"Updating existing Genie Space: {space_id}")

    response = requests.patch(
        f"{workspace_host}/api/2.0/genie/spaces/{space_id}",
        headers=headers,
        json=payload
    )
else:
    # Create new space
    print("Creating new Genie Space...")

    response = requests.post(
        f"{workspace_host}/api/2.0/genie/spaces",
        headers=headers,
        json=payload
    )

if response.status_code in [200, 201]:
    result = response.json()
    space_id = result.get("space_id", existing_space["space_id"] if existing_space else "unknown")
    print(f"\nGenie Space created/updated successfully!")
    print(f"  Space ID: {space_id}")
    print(f"  Name: {GENIE_SPACE_NAME}")
    print(f"  URL: {workspace_host}/genie/spaces/{space_id}")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
    raise Exception(f"Failed to create/update Genie Space: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Get the space details
get_response = requests.get(
    f"{workspace_host}/api/2.0/genie/spaces/{space_id}",
    headers=headers
)

if get_response.status_code == 200:
    space_details = get_response.json()
    print("Genie Space Details:")
    print(f"  Name: {space_details.get('display_name')}")
    print(f"  Space ID: {space_details.get('space_id')}")
    print(f"  Description: {space_details.get('description')}")
    print(f"\nURL: {workspace_host}/genie/spaces/{space_id}")
else:
    print(f"Could not retrieve space details: {get_response.status_code}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Questions
# MAGIC
# MAGIC Open the Genie Space in the UI and try these questions:
# MAGIC
# MAGIC 1. "Which agents have the highest compliance scores?"
# MAGIC 2. "Show me calls that need compliance review"
# MAGIC 3. "What is the resolution rate by call reason?"
# MAGIC 4. "What are the most common compliance issues?"
# MAGIC 5. "Which agents need coaching?"

# COMMAND ----------

print("Genie Space setup complete!")
print(f"\nOpen the Genie Space: {workspace_host}/genie/spaces/{space_id}")
print("\nNext step: Run 08_knowledge_assistant.py to create the Knowledge Assistant and Supervisor Agent")
