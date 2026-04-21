# Conversation Intelligence Copilot

AI-powered call quality analytics on Databricks. Replace expensive third-party transcript scoring tools with native Databricks AI Functions.

## Overview

This solution processes call center transcripts through a medallion architecture:

```
Raw Transcripts (JSON) → Bronze (Delta) → Silver (AI Functions) → Gold (Aggregates) → AI Agents
```

### AI Functions Used

| Function | Purpose |
|----------|---------|
| `ai_analyze_sentiment()` | Track emotion in every utterance |
| `ai_classify()` | Categorize call reasons & compliance status |
| `ai_summarize()` | Generate 2-sentence summaries |
| `ai_extract()` | Pull specific compliance flags |

### AI Agents

| Agent | Type | Purpose |
|-------|------|---------|
| **Genie Space** | SQL Analytics | "Which agents have lowest compliance?" |
| **Knowledge Assistant** | Transcript Search | "Find calls with missing disclosure" |
| **Supervisor Agent** | Multi-Agent | Routes to the right tool automatically |

## Prerequisites

- Databricks workspace with Unity Catalog
- Serverless SQL Warehouse
- Foundation Model access (for AI Functions)

## Quick Start

### 1. Deploy with Databricks Asset Bundles

```bash
# Authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Validate
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev

# Run full pipeline
databricks bundle run conversation_intelligence_setup -t dev
```

### 2. Run Notebooks Individually

```bash
databricks bundle run generate_transcripts -t dev
databricks bundle run bronze_pipeline -t dev
databricks bundle run silver_pipeline -t dev
databricks bundle run gold_pipeline -t dev
databricks bundle run export_transcripts -t dev
databricks bundle run genie_space -t dev
```

### 3. Create Knowledge Assistant & Supervisor Agent

After running the pipeline, follow the UI instructions in `08_knowledge_assistant.py` to create:
1. Knowledge Assistant (searches transcripts)
2. Supervisor Agent (routes between Genie and KA)

## Project Structure

```
conversation-intelligence-copilot/
├── databricks.yml              # DABs configuration
├── README.md
├── src/
│   └── notebooks/
│       ├── 01_generate_transcripts.py   # Generate synthetic call data
│       ├── 02_bronze_pipeline.py        # Ingest raw JSON to Delta
│       ├── 03_silver_pipeline.py        # Apply AI Functions
│       ├── 04_gold_pipeline.py          # Create analytics aggregates
│       ├── 06_export_transcripts.py     # Export TXT for KA
│       ├── 07_genie_space.py            # Create Genie Space (API)
│       └── 08_knowledge_assistant.py    # KA + Supervisor UI steps
└── docs/
    ├── demo-guide.md            # Demo script with speaker notes
    └── learning-guide.md        # Deep-dive technical guide
```

## Tables Created

### Bronze Layer
- `bronze_calls` - Raw call metadata
- `bronze_utterances` - Individual speaker turns
- `bronze_agents` - Agent reference data
- `bronze_members` - Member reference data

### Silver Layer
- `silver_calls` - Enriched calls with AI classifications
- `silver_utterances` - Utterances with sentiment
- `silver_transcript_chunks` - Chunked text for vector search

### Gold Layer
- `gold_call_summary` - Denormalized call details (one row per call)
- `gold_agent_performance` - Agent-level quality KPIs
- `gold_call_reason_metrics` - Metrics by call reason
- `gold_compliance_analysis` - Compliance flag patterns

## Demo Questions

### Genie Space (Analytics)
- "Which agents have the highest compliance scores?"
- "Show me calls that need compliance review"
- "What is the resolution rate by call reason?"

### Knowledge Assistant (Transcript Search)
- "Find calls where agents missed the recording disclosure"
- "Show me how top agents handle billing complaints"
- "What compliance issues appear most often?"

### Supervisor Agent (Combined)
- "Which agents need coaching? Show specific examples."
- "What's driving low CSAT? Show patterns."

## Configuration

Default configuration (edit in notebooks or databricks.yml):
- **Catalog:** `humana_payer`
- **Schema:** `conversation-intelligence-copilot`

## Resources

- [Demo Guide](docs/demo-guide.md) - Presentation flow with speaker notes
- [Learning Guide](docs/learning-guide.md) - Technical deep-dive

## License

Internal use only.
