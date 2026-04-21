# Conversation Intelligence Copilot - Technical Learning Guide

## How to Use This Guide

This guide teaches you the project end-to-end. Work through it sequentially—each section builds on the previous.

**Time to complete:** 2-3 hours for full understanding

**Practice tip:** Each section starts with a speaking blurb you can say out loud when presenting or demoing. Practice these until they feel natural.

---

# Part 1: Understanding the Business Problem

> **Say this:** "Let me start by explaining the problem we're solving. Healthcare payers have two separate call channels that both generate transcripts, and traditionally they've had to send these to expensive third-party AI vendors for analysis. We're going to replace that with native Databricks capabilities."

## What We're Solving

Healthcare payers have two call channels that generate transcripts:

### Channel 1: Inbound Member Support

> **Say this:** "First, there's inbound member support. These are calls from existing health plan members who have questions about billing, claims, or coverage. What matters here is: Did we resolve their issue? Was the agent compliant with regulations? Was the member satisfied?"

- **Who calls:** Existing health plan members
- **Why they call:** Billing questions, claims status, coverage inquiries, prescription issues
- **What matters:** Did we resolve the issue? Was the agent compliant? Was the member satisfied?

### Channel 2: Outbound Sales Prospecting

> **Say this:** "The second channel is outbound sales. These are sales agents—often independent brokers—calling Medicare-eligible prospects. Here we care about different things: Did they enroll? What objections came up? Was the agent CMS-compliant?"

- **Who calls:** Sales agents (often independent brokers)
- **Who they call:** Medicare-eligible prospects
- **What matters:** Did they enroll? What objections came up? Was the agent CMS-compliant?

## The Traditional Approach (What We're Replacing)

> **Say this:** "Here's the traditional approach, and this is what most organizations are doing today. Transcripts go from the call center to a third-party AI vendor, you wait for batch scores to come back, then you import those scores into your data warehouse, and finally build dashboards. The problems are obvious—it's expensive, it's slow, and your insights are completely disconnected from your operational data."

```
Call Center → Transcripts → Send to 3rd Party AI → Wait for Scores → Import to Data Warehouse → Build Dashboards
```

**Problems:**
1. **Cost:** Pay per transcript to external AI vendor
2. **Latency:** Batch processing means delays
3. **Fragmentation:** Scores disconnected from operational data
4. **Limited Search:** Can't easily search transcript content

## Our Approach

> **Say this:** "Our approach is fundamentally different. We keep everything in Databricks. Transcripts come in, AI Functions process them in the same pipeline, analytics are immediate, and supervisors can search transcripts semantically. No external APIs. No batch delays. No per-transcript fees."

```
Call Center → Transcripts → Databricks (AI Functions) → Analytics + Search → AI Agents
```

**Benefits:**
1. **Cost:** Standard compute only, no per-transcript fees
2. **Speed:** Near real-time processing
3. **Unified:** Everything in one platform
4. **Searchable:** Full semantic search over transcripts

---

# Part 2: The Data Model

> **Say this:** "Now let me walk you through the data model. We're using a medallion architecture—Bronze, Silver, Gold—which is a standard Databricks pattern. Each layer serves a specific purpose."

## Understanding the Tables

### Bronze Layer (Raw Data)

> **Say this:** "Bronze is our raw data layer. This is where we parse the JSON files from the call center systems and create structured tables. We keep it simple here—minimal transformation, just get it into Delta format."

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `bronze_calls` | One row per call | call_id, agent_id, member_id, call_start_time, duration_seconds, csat_score, compliance_score |
| `bronze_utterances` | One row per speaker turn | call_id, sequence, speaker_role, text, start_time |
| `bronze_agents` | Agent master data | agent_id, agent_name, team, tenure_months |
| `bronze_members` | Member master data | member_id, member_name, plan_type, age |

### Silver Layer (AI-Enriched)

> **Say this:** "Silver is where the magic happens. This is where we apply AI Functions to enrich every single call. We classify call reasons, detect compliance issues, generate summaries, and analyze sentiment—all using SQL-native AI capabilities."

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `silver_calls` | Calls + AI enrichments | All bronze columns + ai_call_reason, ai_compliance_status, ai_summary, ai_overall_sentiment, full_transcript |
| `silver_utterances` | Utterances + sentiment | All bronze columns + sentiment |
| `silver_transcript_chunks` | Chunked for search | chunk_id, call_id, chunk_num, chunk_text, metadata columns |

### Gold Layer (Analytics)

> **Say this:** "Gold is analytics-ready. We denormalize and aggregate here so Genie Space queries are fast and simple. One table for call-level facts, one for agent KPIs, one for call reason breakdowns, and one for compliance patterns."

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `gold_call_summary` | Denormalized call facts | All call details + agent info + member info + derived flags (is_resolved, needs_review, etc.) |
| `gold_agent_performance` | Agent-level KPIs | agent_id, total_calls, avg_compliance_score, avg_csat, resolution_rate |
| `gold_call_reason_metrics` | Metrics by call reason | call_reason, total_calls, resolution_rate, avg_compliance, avg_csat |
| `gold_compliance_analysis` | Compliance flag patterns | compliance_flag, occurrence_count, affected_calls, avg_compliance_when_flagged |

## Data Flow Diagram

> **Say this:** "Here's the complete data flow. JSON files land in a Volume, get parsed into Bronze Delta tables, AI Functions enrich them in Silver, Gold creates the aggregates, and then our three AI agents sit on top—Genie for analytics, Knowledge Assistant for transcript search, and the Supervisor to route between them."

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RAW DATA (Volume)                            │
│         /Volumes/humana_payer/conversation-intelligence-copilot/    │
│                     raw_data/transcripts/*.json                     │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                                │
│  ┌─────────────┐  ┌──────────────────┐  ┌─────────┐  ┌─────────┐   │
│  │bronze_calls │  │bronze_utterances │  │bronze_  │  │bronze_  │   │
│  │             │  │                  │  │agents   │  │members  │   │
│  └─────────────┘  └──────────────────┘  └─────────┘  └─────────┘   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                          AI Functions Applied
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                                │
│  ┌─────────────┐  ┌──────────────────┐  ┌─────────────────────┐    │
│  │silver_calls │  │silver_utterances │  │silver_transcript_   │    │
│  │+ AI columns │  │+ sentiment       │  │chunks (for search)  │    │
│  └─────────────┘  └──────────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                              Aggregations
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                                 │
│  ┌──────────────┐  ┌────────────────┐  ┌────────────────────────┐  │
│  │gold_call_    │  │gold_agent_     │  │gold_call_reason_       │  │
│  │summary       │  │performance     │  │metrics                 │  │
│  └──────────────┘  └────────────────┘  └────────────────────────┘  │
│                                                                     │
│  ┌────────────────────┐                                            │
│  │gold_compliance_    │                                            │
│  │analysis            │                                            │
│  └────────────────────┘                                            │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AI AGENTS                                   │
│  ┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐   │
│  │Genie Space  │    │Knowledge         │    │Supervisor Agent  │   │
│  │(SQL on Gold)│    │Assistant (Search)│    │(Routes between)  │   │
│  └─────────────┘    └──────────────────┘    └──────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

# Part 3: The Notebooks (Deep Dive)

> **Say this:** "Now let's dive into the actual code. There are six notebooks in this pipeline. I'll walk you through each one so you understand exactly what's happening at every step."

## Notebook 1: Generate Transcripts

> **Say this:** "The first notebook generates synthetic transcript data. In a real deployment, you'd skip this—your transcripts would come from Balto, Genesys, or whatever telephony system you use. But for demo purposes, we need realistic test data."

**File:** `01_generate_transcripts.py`

**Purpose:** Creates synthetic call transcript data (for demo purposes)

### What It Does

1. **Creates reference data:**
   - 10 agents with names, teams, tenure
   - 50 members with names, plan types, ages

2. **Generates 100 call transcripts:**
   - Random pairing of agent + member
   - Call metadata (duration, CSAT, compliance score)
   - Quality audit info (compliance flags)
   - Full conversation as utterances array

3. **Saves as JSON files:**
   - Each call = one JSON file
   - Saved to `/Volumes/.../raw_data/transcripts/`

### Key Code Pattern

> **Say this:** "Each transcript is a JSON document with nested structure—metadata, quality audit, agent info, member info, and an array of utterances. This matches what real telephony systems export."

```python
# Generate a single transcript
transcript = {
    "call_id": f"CALL-{i:06d}",
    "transcript_source": random.choice(["Balto", "Genesys"]),
    "metadata": {
        "call_start_time": fake_timestamp(),
        "duration_seconds": random.randint(120, 900),
        "csat_score": weighted_random(1, 5),
    },
    "quality_audit": {
        "compliance_score": random.randint(50, 100),
        "compliance_flags": random_flags(),
    },
    "agent": {"agent_id": agent["id"], "name": agent["name"]},
    "member": {"member_id": member["id"], "name": member["name"]},
    "utterances": generate_conversation(call_reason)
}
```

### Why JSON Format?

- Matches real telephony system exports
- Nested structure (metadata, agent, member, utterances)
- Easy to parse with Spark's JSON reader

---

## Notebook 2: Bronze Pipeline

> **Say this:** "The Bronze pipeline parses those JSON files into structured Delta tables. We're extracting nested fields, exploding arrays, and creating our foundation tables. No AI here yet—just clean data parsing."

**File:** `02_bronze_pipeline.py`

**Purpose:** Parse raw JSON into structured Delta tables

### What It Does

1. **Reads JSON files from Volume:**
   ```python
   raw_df = spark.read.option("multiline", "true").json(RAW_PATH)
   ```

2. **Extracts nested fields into flat tables:**
   - `bronze_calls` - Call-level data
   - `bronze_utterances` - Exploded utterance array
   - `bronze_agents` - Distinct agents
   - `bronze_members` - Distinct members

3. **Saves as Delta tables:**
   ```python
   bronze_calls.write.mode("overwrite").saveAsTable("bronze_calls")
   ```

### Key Code Pattern: Parsing Nested JSON

> **Say this:** "We use Spark's column selection to reach into nested JSON structures. Notice how we use dot notation—metadata.call_start_time, quality_audit.compliance_score—and cast everything to explicit types."

```python
# Extract nested fields with explicit casting
bronze_calls = raw_df.select(
    F.col("call_id"),
    F.col("transcript_source"),
    F.col("metadata.call_start_time").cast("timestamp").alias("call_start_time"),
    F.col("metadata.duration_seconds").cast("int").alias("duration_seconds"),
    F.col("quality_audit.compliance_score").cast("int").alias("compliance_score"),
    F.col("quality_audit.compliance_flags").alias("compliance_flags"),  # Array type
    F.col("agent.agent_id").alias("agent_id"),
    F.col("agent.name").alias("agent_name"),
    # ...
)
```

### Key Code Pattern: Exploding Arrays

> **Say this:** "For utterances, we use the explode function. This takes an array column and creates one row per element. So if a call has 20 utterances, we get 20 rows in the utterances table."

```python
# Turn utterances array into rows
bronze_utterances = raw_df.select(
    F.col("call_id"),
    F.col("agent.agent_id").alias("agent_id"),
    F.col("member.member_id").alias("member_id"),
    F.explode("utterances").alias("utterance")  # Each element becomes a row
).select(
    "call_id",
    "agent_id",
    "member_id",
    F.col("utterance.sequence").alias("sequence"),
    F.col("utterance.speaker").alias("speaker"),
    F.col("utterance.speaker_role").alias("speaker_role"),
    F.col("utterance.text").alias("text"),
)
```

### Common Issue: Schema Merge Conflicts

If you re-run with different data types, Delta may fail to merge schemas. Solution:

```python
# Drop table first to avoid schema conflicts
spark.sql("DROP TABLE IF EXISTS bronze_calls")
```

---

## Notebook 3: Silver Pipeline (AI Functions)

> **Say this:** "Now we get to the most important notebook—the Silver pipeline. This is where we apply AI Functions to every single call. No Python ML models to manage. No embeddings infrastructure. Just SQL functions that call foundation models directly."

**File:** `03_silver_pipeline.py`

**Purpose:** Enrich Bronze data with AI Functions

### This is the Most Important Notebook

This is where the AI magic happens. Study this carefully.

### Step 1: Build Full Transcripts

> **Say this:** "Before we can analyze a call, we need the complete conversation as one text block. We concatenate all utterances in order, with speaker labels. The result looks like a chat transcript—Agent says this, Member says that."

Before AI analysis, we need the complete conversation as one text block:

```python
# Concatenate utterances in order
transcripts = bronze_utterances.orderBy("call_id", "sequence").groupBy("call_id").agg(
    concat_ws("\n",
        collect_list(
            concat(col("speaker_role"), lit(": "), col("text"))
        )
    ).alias("full_transcript")
)

# Result looks like:
# "Agent: Thank you for calling...
#  Member: Hi, I have a question about my bill...
#  Agent: I'd be happy to help with that..."
```

### Step 2: Apply AI Functions

> **Say this:** "Here's where it gets powerful. In a single SQL statement, we're classifying call reason, detecting compliance issues, generating a summary, and analyzing sentiment. All four AI Functions running on every transcript."

Now we call AI Functions in SQL:

```sql
SELECT
    call_id,
    call_reason,
    resolution,
    compliance_score,
    full_transcript,

    -- AI Function 1: Classify call reason
    ai_classify(
        full_transcript,
        ARRAY('billing_inquiry', 'coverage_question', 'claims_status',
              'prescription_issue', 'provider_lookup', 'enrollment_change',
              'complaint', 'general_inquiry')
    ) AS ai_call_reason,

    -- AI Function 2: Classify compliance status
    ai_classify(
        full_transcript,
        ARRAY('compliant', 'missing_recording_notice', 'incomplete_verification',
              'lacks_empathy', 'abrupt_closing', 'multiple_issues')
    ) AS ai_compliance_status,

    -- AI Function 3: Generate summary
    ai_summarize(full_transcript, 150) AS ai_summary,

    -- AI Function 4: Overall sentiment
    ai_analyze_sentiment(full_transcript) AS ai_overall_sentiment

FROM calls_with_transcripts
```

### Understanding Each AI Function

#### ai_analyze_sentiment(text)

> **Say this:** "ai_analyze_sentiment is the simplest one. Give it any text, and it returns positive, negative, neutral, or mixed. We run this on every utterance so we can track emotional arcs through a call—when did the member get frustrated? When did they calm down?"

**Input:** Any text string
**Output:** One of: `positive`, `negative`, `neutral`, `mixed`

**How it works:** Calls a foundation model that's been tuned for sentiment classification. No training required.

**Use cases:**
- Track member emotion through a call
- Identify frustrated callers
- Measure agent empathy

```sql
-- Sentiment on each utterance
SELECT
    call_id,
    sequence,
    text,
    ai_analyze_sentiment(text) AS sentiment
FROM bronze_utterances
```

#### ai_classify(text, categories_array)

> **Say this:** "ai_classify is incredibly powerful. You give it text and an array of categories, and it picks the best match. The key insight is that YOU define the categories. Change the array, and you're classifying different things—call reasons, compliance issues, objection types, whatever you need."

**Input:** Text + Array of possible categories
**Output:** The category that best matches

**How it works:** The model reads the text and picks the most appropriate label from your array.

**Key insight:** YOU define the categories. Change the array to classify anything.

```sql
-- Call reason classification
ai_classify(transcript, ARRAY('billing', 'claims', 'coverage', 'complaint'))

-- Compliance classification
ai_classify(transcript, ARRAY('compliant', 'non_compliant', 'needs_review'))

-- Sales outcome classification
ai_classify(transcript, ARRAY('enrolled', 'callback', 'not_interested'))

-- Objection type classification
ai_classify(transcript, ARRAY('price', 'timing', 'competition', 'none'))
```

#### ai_summarize(text, max_words)

> **Say this:** "ai_summarize generates a concise summary. You specify the max word count. This is huge for supervisors—instead of reading full transcripts, they can review summaries and 10x their coverage."

**Input:** Text + Maximum word count
**Output:** Concise summary

**How it works:** Generates a summary capturing the key points.

```sql
-- 2-3 sentence summary
ai_summarize(full_transcript, 150) AS ai_summary

-- One-liner
ai_summarize(full_transcript, 50) AS ai_headline
```

#### ai_extract(text, labels_array)

> **Say this:** "ai_extract pulls specific entities or facts from text. Give it an array of things to extract—like member name, issue type, resolution—and it returns those values from the transcript."

**Input:** Text + Array of things to extract
**Output:** Extracted values for each label

**Use case:** Pull specific entities or facts from text.

```sql
-- Extract specific information
ai_extract(transcript, ARRAY('member_name', 'issue_type', 'resolution'))
```

### Step 3: Sentiment on Utterances

> **Say this:** "We also run sentiment on every individual utterance, not just the overall call. This lets us track the emotional arc—when exactly did the conversation turn negative? Did the agent recover it?"

We also analyze sentiment on each individual utterance:

```sql
SELECT
    call_id,
    sequence,
    speaker_role,
    text,
    ai_analyze_sentiment(text) AS sentiment
FROM bronze_utterances
```

This lets us track emotional arcs—when did the member get frustrated? When did they calm down?

### Step 4: Create Transcript Chunks

> **Say this:** "For the Knowledge Assistant, we need to chunk transcripts into smaller pieces. Full transcripts can be thousands of words, but embedding models work best with smaller chunks. We create overlapping chunks so context isn't lost at boundaries."

For Knowledge Assistant search, we need smaller chunks:

```python
def chunk_transcript(transcript, chunk_size=500, overlap=100):
    """Split transcript into overlapping chunks."""
    chunks = []
    start = 0
    chunk_num = 0

    while start < len(transcript):
        end = start + chunk_size
        chunk_text = transcript[start:end]

        # Try to end at sentence boundary
        if end < len(transcript):
            last_period = chunk_text.rfind('.')
            if last_period > chunk_size // 2:
                chunk_text = chunk_text[:last_period + 1]
                end = start + last_period + 1

        chunks.append({
            "chunk_num": chunk_num,
            "chunk_text": chunk_text.strip()
        })

        chunk_num += 1
        start = end - overlap  # Overlap for context continuity

    return chunks
```

**Why chunking?**
- Full transcripts may be too long for embedding models
- Smaller chunks = more precise search results
- Overlap ensures context isn't lost at boundaries

---

## Notebook 4: Gold Pipeline

> **Say this:** "The Gold pipeline creates analytics-ready aggregates. We denormalize everything so Genie Space queries are fast and simple. No joins needed at query time."

**File:** `04_gold_pipeline.py`

**Purpose:** Create analytics-ready aggregates

### Table 1: gold_call_summary

> **Say this:** "gold_call_summary is our main fact table—one row per call with everything joined in. Agent info, member info, all the AI classifications, plus derived flags like is_resolved and needs_review. This is what powers most Genie queries."

Denormalized view joining calls with agent and member info:

```sql
CREATE TABLE gold_call_summary AS
SELECT
    c.call_id,
    c.transcript_source,
    DATE(c.call_start_time) AS call_date,
    c.call_reason,
    c.resolution,
    c.compliance_score,
    c.ai_compliance_status,
    c.ai_summary,

    -- Agent details (joined)
    c.agent_id,
    c.agent_name,
    a.team AS agent_team,

    -- Member details (joined)
    c.member_id,
    m.plan_type,

    -- Derived flags for analytics
    CASE WHEN c.resolution = 'resolved' THEN 1 ELSE 0 END AS is_resolved,
    CASE WHEN c.compliance_score >= 90 THEN 1 ELSE 0 END AS is_compliant,
    CASE WHEN c.compliance_score < 70 THEN 1 ELSE 0 END AS needs_review

FROM silver_calls c
LEFT JOIN bronze_agents a ON c.agent_id = a.agent_id
LEFT JOIN bronze_members m ON c.member_id = m.member_id
```

**Why denormalize?**
- Genie Space queries are simpler
- No joins needed at query time
- Better performance for dashboards

### Table 2: gold_agent_performance

> **Say this:** "gold_agent_performance gives us agent-level KPIs—total calls, average compliance, average CSAT, resolution rate. Perfect for agent rankings and performance reviews."

Agent-level KPIs:

```sql
CREATE TABLE gold_agent_performance AS
SELECT
    agent_id,
    agent_name,
    agent_team,

    COUNT(*) AS total_calls,
    SUM(is_resolved) AS resolved_calls,

    ROUND(AVG(compliance_score), 1) AS avg_compliance_score,
    ROUND(AVG(csat_score), 2) AS avg_csat,

    ROUND(SUM(is_resolved) * 100.0 / COUNT(*), 1) AS resolution_rate,
    ROUND(SUM(is_compliant) * 100.0 / COUNT(*), 1) AS compliance_rate

FROM gold_call_summary
GROUP BY agent_id, agent_name, agent_team
```

### Table 3: gold_compliance_analysis

> **Say this:** "gold_compliance_analysis shows us compliance flag patterns. We explode the flags array—one row per flag per call—then aggregate to see which issues are most common, which agents have them, and what the impact is on compliance scores."

Compliance flag patterns (requires exploding the flags array):

```sql
-- First, explode compliance_flags array
SELECT
    call_id,
    agent_id,
    explode_outer(compliance_flags) AS flag
FROM silver_calls
WHERE compliance_flags IS NOT NULL

-- Then aggregate
SELECT
    flag AS compliance_flag,
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT call_id) AS affected_calls,
    COUNT(DISTINCT agent_id) AS agents_with_flag,
    ROUND(AVG(compliance_score), 1) AS avg_compliance_when_flagged
FROM compliance_flags_view
GROUP BY flag
```

---

## Notebook 5: Knowledge Assistant Setup

> **Say this:** "Notebook 5 contains instructions for creating the Knowledge Assistant through the UI. We don't create KA programmatically because it involves selecting Volumes, configuring instructions, and waiting for endpoint provisioning—all better done interactively."

**File:** `05_knowledge_assistant.py`

**Purpose:** Instructions for creating KA via UI (not code)

### Why UI Not Code?

Knowledge Assistant creation requires:
- Selecting a Volume path
- Configuring instructions
- Waiting for endpoint provisioning

These are better done interactively in the UI.

### The Notebook Contains:

1. Prerequisites check (verify transcript files exist)
2. Step-by-step UI instructions with screenshots
3. Configuration values to copy/paste
4. Test queries to verify setup

---

## Notebook 6: Export Transcripts

> **Say this:** "The final notebook exports transcripts to TXT files for the Knowledge Assistant. KA needs files in a Volume—it can't read Delta tables directly. We include a metadata header in each file so when KA retrieves content, it has context about the call."

**File:** `06_export_transcripts.py`

**Purpose:** Convert Delta table data to TXT files for KA

### Why Export to TXT?

Knowledge Assistant needs files in a Volume—it can't read Delta tables directly.

### What It Does

```python
# Read transcripts from Silver table
silver_calls = spark.table("silver_calls")

# For each call, create a TXT file with metadata header
for row in calls_data:
    content = f"""CALL TRANSCRIPT
================================================================================
Call ID: {row["call_id"]}
Agent: {row["agent_name"]}
Compliance Score: {row["compliance_score"]}
AI Compliance Status: {row["ai_compliance_status"]}
Call Reason: {row["call_reason"]}
================================================================================
TRANSCRIPT
================================================================================

{row["full_transcript"]}
"""
    # Write to Volume
    dbutils.fs.put(f"{OUTPUT_PATH}/{call_id}.txt", content, overwrite=True)
```

### Why Include Metadata Header?

When KA searches and retrieves a chunk, the header provides context:
- "This call had compliance score 65"
- "The agent was Sarah Chen"
- "Call reason was billing inquiry"

This helps the AI generate better answers with context.

---

# Part 4: The AI Agents

> **Say this:** "Now let's talk about the three AI agents that supervisors actually interact with. These provide natural language access to all the data we've processed."

## Genie Space

> **Say this:** "Genie Space is our analytics interface. Supervisors ask questions in plain English—'Which agents have the lowest compliance?'—and Genie translates that to SQL, runs it against our Gold tables, and returns results with visualizations. No SQL knowledge required."

### What It Is
Natural language to SQL interface over your Gold tables.

### How It Works
1. User asks question in English
2. Genie translates to SQL
3. SQL runs against warehouse
4. Results returned with visualization

### Key Configuration

**Tables connected:**
- gold_call_summary
- gold_agent_performance
- gold_call_reason_metrics
- gold_compliance_analysis

**Instructions (teach Genie your domain):**
```
This is a quality auditing dataset for member support calls.
- gold_call_summary has one row per call
- gold_agent_performance has agent-level KPIs
- Compliance scores below 70 need review
- Resolution means the issue was resolved in this call
```

**Sample questions:**
- "Which agents have the highest compliance scores?"
- "Show me calls needing review"
- "Resolution rate by call reason"

### Join Specs

> **Say this:** "We also configure join specs so Genie knows how tables relate. This lets it automatically join data when a question spans multiple tables."

Tell Genie how tables relate:

```
gold_call_summary.agent_id = gold_agent_performance.agent_id (many-to-one)
gold_call_summary.call_reason = gold_call_reason_metrics.call_reason (many-to-one)
```

---

## Knowledge Assistant

> **Say this:** "Knowledge Assistant is our semantic search interface. It's RAG—Retrieval-Augmented Generation—over the actual transcript files. When you ask 'Find calls where agents missed the recording disclosure,' it searches through transcript embeddings, retrieves relevant chunks, and generates an answer citing specific calls."

### What It Is
RAG (Retrieval-Augmented Generation) over your transcript files.

### How It Works
1. User asks question
2. KA searches transcript embeddings for relevant chunks
3. Retrieved chunks sent to LLM as context
4. LLM generates answer citing the transcripts

### Key Configuration

**Volume path:** `/Volumes/humana_payer/conversation-intelligence-copilot/transcripts`

**Instructions:**
```
You are a call quality assistant.
- Always cite the Call ID when referencing transcripts
- Include compliance scores when relevant
- Summarize key points concisely
```

### What KA Creates Automatically

> **Say this:** "The beautiful thing about Knowledge Assistant is you don't manage any vector infrastructure. It automatically creates embeddings, builds the vector index, and handles the retrieval pipeline. You just point it at your files."

- Embeddings for all documents
- Vector index for semantic search
- Retrieval pipeline

You don't manage any of this—KA handles it.

---

## Supervisor Agent

> **Say this:** "The Supervisor Agent is the magic layer that ties everything together. It's a multi-agent orchestrator that analyzes your question and routes it to the right tool. Ask for metrics—it goes to Genie. Ask for examples—it goes to KA. Ask for both—it queries both and synthesizes the answer."

### What It Is
Multi-agent orchestrator that routes questions to the right tool.

### How It Works
1. User asks question
2. Supervisor analyzes intent
3. Routes to Genie (metrics) or KA (examples) or both
4. Synthesizes combined response

### Key Configuration

**Child agents:**
1. `analytics` - Genie Space for SQL queries
2. `transcripts` - Knowledge Assistant for search

**Routing instructions:**
```
Route based on intent:

→ analytics (Genie):
  - Metrics questions
  - Rankings
  - Aggregations
  - Trends

→ transcripts (Knowledge Assistant):
  - Example requests
  - Pattern discovery
  - Specific call content

For complex questions, use both and synthesize.
```

---

# Part 5: Customizing for Your Use Case

> **Say this:** "One of the best things about this architecture is how easy it is to customize. Want to adapt this for sales instead of support? Just change the classification arrays. Let me show you."

## Changing Classification Categories

### For Sales Channel

> **Say this:** "For sales calls, we change the ai_classify arrays. Instead of call reasons like billing and claims, we use outcomes like enrolled and callback. Instead of support compliance flags, we use CMS compliance rules."

Edit the Silver pipeline SQL:

```sql
-- Instead of support call reasons:
ai_classify(transcript,
    ARRAY('billing_inquiry', 'coverage_question', 'claims_status')
) AS ai_call_reason

-- Use sales outcomes:
ai_classify(transcript,
    ARRAY('enrolled', 'callback_scheduled', 'not_interested',
          'needs_more_info', 'wrong_number')
) AS ai_call_outcome
```

### For Different Compliance Rules

```sql
-- Support compliance:
ai_classify(transcript,
    ARRAY('compliant', 'missing_recording_notice', 'incomplete_verification')
)

-- Sales/CMS compliance:
ai_classify(transcript,
    ARRAY('compliant', 'missing_scope_of_appointment', 'missing_cms_disclaimer',
          'unauthorized_benefit_claims', 'high_pressure_closing')
)
```

## Adding New Gold Tables

> **Say this:** "You can easily add new Gold tables for different analytics. Here's an example—objection analysis for sales calls."

### Example: Objection Analysis (Sales)

```sql
CREATE TABLE gold_objection_analysis AS
SELECT
    ai_objection_type AS objection,
    COUNT(*) AS occurrence_count,
    SUM(CASE WHEN ai_call_outcome = 'enrolled' THEN 1 ELSE 0 END) AS overcame_count,
    ROUND(SUM(CASE WHEN ai_call_outcome = 'enrolled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS overcome_rate
FROM silver_calls
WHERE ai_objection_type != 'no_objection'
GROUP BY ai_objection_type
```

## Updating Genie Space

> **Say this:** "After adding new tables, update your Genie Space—add the table as a data source, configure join specs if needed, and add relevant sample questions."

After adding new Gold tables:
1. Edit Genie Space configuration
2. Add new table to data sources
3. Add join specs if needed
4. Add relevant sample questions

---

# Part 6: Troubleshooting

> **Say this:** "Let me cover some common issues you might run into and how to fix them."

## Common Issues

### "AI Function returned null"

> **Say this:** "If an AI Function returns null, check that the transcript text isn't empty, and make sure you're running on a serverless SQL warehouse. AI Functions require serverless."

- Check that transcript text is not empty
- Verify warehouse is serverless (AI Functions require serverless)

### "Schema merge failed"

> **Say this:** "Schema merge failures usually happen when you re-run pipelines with different data types. The fix is simple—add DROP TABLE IF EXISTS before creating tables, and always use explicit casts on columns."

- Add `DROP TABLE IF EXISTS` before creating tables
- Use explicit `.cast()` on all columns

### "Genie doesn't understand my question"

> **Say this:** "If Genie struggles with questions, improve its context. Add more sample questions, better text instructions with your domain terminology, and SQL snippets for common patterns."

- Add more sample questions to Genie Space
- Improve text instructions with domain terminology
- Add SQL snippets for common patterns

### "KA returns irrelevant results"

> **Say this:** "If KA returns off-topic results, check that your transcript files have the metadata headers—those help the retrieval. Also verify the Volume path is correct and add more specific instructions."

- Check transcript files have metadata headers
- Verify Volume path is correct
- Add more specific instructions to KA

### "Supervisor routes to wrong agent"

> **Say this:** "If the Supervisor sends questions to the wrong agent, refine the routing instructions. Add clearer keywords for each agent and include example questions with explicit routing guidance."

- Refine routing instructions with clearer keywords
- Add example questions with explicit routing

---

# Part 7: Key Concepts Summary

> **Say this:** "Let me summarize the key concepts. This is what you need to remember."

## Medallion Architecture

> **Say this:** "Medallion architecture: Bronze is raw ingestion with minimal transformation. Silver is cleaned, enriched, AI-augmented. Gold is aggregated and analytics-ready."

- **Bronze:** Raw ingestion, minimal transformation
- **Silver:** Cleaned, enriched, AI-augmented
- **Gold:** Aggregated, analytics-ready

## AI Functions

> **Say this:** "AI Functions are SQL-native—no ML infrastructure to manage. They run on serverless SQL warehouses. Four main ones: classify, summarize, sentiment, and extract."

- SQL-native, no ML infrastructure
- Run on serverless SQL warehouse
- `ai_classify()`, `ai_summarize()`, `ai_analyze_sentiment()`, `ai_extract()`

## Why This Architecture?

> **Say this:** "Why this architecture? Five reasons: It's unified—everything in one platform. It's governed—Unity Catalog handles security. It's scalable—serverless compute. It's flexible—easy to customize classifications. And it's searchable—both SQL analytics AND semantic search."

1. **Unified:** Everything in one platform
2. **Governed:** Unity Catalog for security
3. **Scalable:** Serverless compute
4. **Flexible:** Easy to customize classifications
5. **Searchable:** Both SQL analytics AND semantic search

## The Three Agents

> **Say this:** "The three agents: Genie for structured data and SQL analytics on Gold tables. Knowledge Assistant for unstructured data and semantic search over transcripts. Supervisor to route between them intelligently."

1. **Genie:** Structured data (SQL on Gold tables)
2. **KA:** Unstructured data (Search over transcripts)
3. **Supervisor:** Routes between them intelligently

---

# Part 8: Practice Exercises

> **Say this:** "Finally, here are some exercises to solidify your understanding. Work through these hands-on."

## Exercise 1: Trace a Single Call
1. Find call `CALL-000001` in bronze_calls
2. Find its utterances in bronze_utterances
3. Find it in silver_calls with AI enrichments
4. Find it in gold_call_summary

## Exercise 2: Add a New Classification
1. Add "tone" classification: `ARRAY('professional', 'casual', 'frustrated', 'empathetic')`
2. Modify Silver pipeline
3. Re-run and verify results

## Exercise 3: Create a New Gold Table
1. Design `gold_daily_metrics` with call volume and avg compliance by day
2. Write the SQL
3. Add to Genie Space

## Exercise 4: Test the Agents
1. Ask Genie: "What's the resolution rate for billing inquiries?"
2. Ask KA: "Find calls where agents showed empathy"
3. Ask Supervisor: "Which call reasons have worst CSAT? Show examples."

---

# Quick Reference Card

> **Say this:** "Here's your quick reference for key paths, tables, and syntax."

## Key Paths
- Raw data: `/Volumes/humana_payer/conversation-intelligence-copilot/raw_data/transcripts/`
- Transcripts for KA: `/Volumes/humana_payer/conversation-intelligence-copilot/transcripts/`
- Tables: `humana_payer.conversation-intelligence-copilot.*`

## Key Tables
- `bronze_calls`, `bronze_utterances`
- `silver_calls`, `silver_utterances`, `silver_transcript_chunks`
- `gold_call_summary`, `gold_agent_performance`, `gold_call_reason_metrics`, `gold_compliance_analysis`

## AI Functions
```sql
ai_analyze_sentiment(text) → positive/negative/neutral/mixed
ai_classify(text, ARRAY[categories]) → best matching category
ai_summarize(text, max_words) → summary string
ai_extract(text, ARRAY[labels]) → extracted values
```

## Agents
- Genie Space: `Call Quality Intelligence Genie`
- Knowledge Assistant: `Call Transcript Assistant`
- Supervisor: `Call Quality Intelligence Copilot`
