# Conversation Intelligence Copilot - Demo Guide

## Overview

**Demo Duration:** 15-20 minutes (can extend to 30 with deep dives)

**Audience:** Healthcare payer teams, contact center leaders, data platform teams

**Key Message:** Replace expensive third-party transcript scoring tools with native Databricks AI Functions—same platform where your data lives.

---

## Pre-Demo Checklist

- [ ] Databricks workspace open: https://fevm-serverless-mwelio-humana.cloud.databricks.com
- [ ] SQL Warehouse running (Serverless Starter Warehouse)
- [ ] Genie Space open: `Call Quality Intelligence Genie`
- [ ] Knowledge Assistant open: `Call Transcript Assistant`
- [ ] Supervisor Agent open: `Call Quality Intelligence Copilot`
- [ ] Google Slides open for presentation

---

# Presentation Flow (Matches Google Slides)

## Slide 1: Title Slide

**Title:** Conversation Intelligence Copilot
**Subtitle:** AI-Powered Call Quality Analytics on Databricks

**Say This:**
> "Today I'm going to show you how we built a complete conversation intelligence platform on Databricks—replacing external transcript scoring tools with native AI capabilities. This solution works for BOTH inbound member support calls AND outbound sales prospecting. Same architecture, same AI functions, different classifications."

---

## Slide 2: The Transcript Challenge

**Bullets:**
- Thousands of calls daily across support & sales
- Transcripts are unstructured and hard to search
- Third-party AI tools are expensive and fragmented
- Insights disconnected from your data platform

**Say This:**
> "Let's talk about the problem. Healthcare payers handle thousands of calls every day. On the support side, members call about billing, claims, coverage. On the sales side, agents call Medicare-eligible prospects. Both channels generate transcripts. Traditionally, those transcripts get sent to third-party AI services for scoring. You wait for batch results, pay per transcript, then manually import scores back. It's slow, expensive, and disconnected."

---

## Slide 3: Native AI on the Lakehouse

**Bullets:**
- Transcripts land directly in Delta Lake
- AI Functions enrich in the same pipeline
- No external APIs, no ML infrastructure
- Real-time insights as calls complete

**Say This:**
> "Here's what we built. Transcripts land in Unity Catalog as Delta tables. We enrich them using Databricks AI Functions—SQL-native functions that call foundation models directly. No Python required. No endpoints to manage. The enriched data flows into Gold tables for analytics. And we expose everything through three AI agents. All on one platform. All governed by Unity Catalog."

---

## Slide 4: End-to-End Pipeline

**Visual:** Raw Transcripts (JSON) → Bronze (Delta) → Silver (AI Functions) → Gold (Aggregates) → AI Agents

**Say This:**
> "Let me walk you through the architecture. Raw JSON transcripts come from Balto, Genesys, or any telephony system. Bronze layer ingests them into Delta tables. The magic happens in Silver—we apply AI Functions to every transcript. We analyze sentiment, classify call reasons, detect compliance issues, and generate summaries. All in SQL. Gold aggregates for analytics. And three AI agents sit on top for natural language access."

---

## Slide 5: Four Functions That Change Everything

| Function | What It Does |
|----------|--------------|
| `ai_analyze_sentiment()` | Tracks emotion in every utterance |
| `ai_classify()` | Categorizes call reasons & compliance |
| `ai_summarize()` | Generates 2-sentence summaries |
| `ai_extract()` | Pulls specific compliance flags |

**Say This:**
> "The AI Functions are the secret sauce. ai_analyze_sentiment returns positive, negative, neutral, or mixed. We run it on every utterance to track emotional arcs. ai_classify picks the best match from your category array—call reasons, compliance status, whatever you need. ai_summarize generates concise summaries so supervisors can review 10x more calls. And here's the key: this is just SQL. No ML models to train. It runs on your serverless SQL warehouse."

---

## Slide 6: AI Functions in Action

**SQL Examples:**

```sql
-- Sentiment on every utterance
SELECT ai_analyze_sentiment(text) AS sentiment
FROM bronze_utterances

-- Classify call reasons from transcript
SELECT ai_classify(
  full_transcript,
  ARRAY('billing', 'claims', 'coverage', 'enrollment', 'prescription')
) AS ai_call_reason

-- Detect compliance issues
SELECT ai_classify(
  full_transcript,
  ARRAY('compliant', 'missing_disclosure', 'incomplete_verification', 'needs_review')
) AS ai_compliance_status

-- Generate 2-sentence summary
SELECT ai_summarize(full_transcript, 100) AS ai_summary
```

**Key Points:**
- No Python required—pure SQL
- Runs on serverless SQL warehouse
- Results stored in Silver layer tables
- Supports batch processing of thousands of calls

**Say This:**
> "Let me show you the actual SQL. This is running in our Silver pipeline. ai_analyze_sentiment runs on every utterance—that's how we track emotional arcs through a call. ai_classify takes the full transcript and picks the best match from your category array. We use it twice: once for call reasons, once for compliance status. ai_summarize generates a concise summary so supervisors can quickly review calls. The key insight: this is all SQL. No Python notebooks. No ML infrastructure. It runs directly on your serverless SQL warehouse and writes results to Delta tables."

---

## Slide 7: Questions That Showcase AI Functions

### Genie Space Questions (AI Function Evidence)

| Question | AI Function | Column Queried |
|----------|-------------|----------------|
| "Show me the AI-generated summaries for calls needing review" | `ai_summarize()` | `ai_summary` |
| "What's the breakdown of AI compliance status?" | `ai_classify()` | `ai_compliance_status` |
| "Compare AI call reasons vs manual call reasons" | `ai_classify()` | `ai_call_reason` vs `call_reason` |
| "Which agents have the most AI-detected compliance issues?" | `ai_classify()` | `ai_compliance_status` |

### Supervisor Agent Questions (Combined Evidence)

| Question | What It Shows |
|----------|---------------|
| "Show me calls where AI detected missing disclosure and give me examples" | Genie returns `ai_compliance_status`, KA shows transcript |
| "What patterns did the AI find in low-scoring calls? Show examples." | AI classifications + transcript evidence |
| "Compare the AI summary to the actual transcript for a problem call" | Shows `ai_summary` vs raw transcript |

### Best Demo Questions (columns with `ai_` prefix prove AI did the work)

- **"Show me the AI summary and compliance status for the 5 lowest scoring calls"**
- **"How often does AI compliance status disagree with manual compliance score?"**
- **"What call reasons did the AI detect most frequently?"**

**Say This:**
> "These questions are specifically designed to showcase that AI Functions are doing the work. Notice that the answers reference columns with the 'ai_' prefix—ai_summary, ai_compliance_status, ai_call_reason. These are the outputs of our AI Functions. When you demo, point out these column names to prove the AI enrichment is happening. The comparison questions—like AI vs manual compliance—are particularly powerful because they show AI validation of human work."

---

## Slide 8: Same Platform, Different Classifications

| Member Support | Sales Prospecting |
|----------------|-------------------|
| Call reasons: billing, claims, coverage | Outcomes: enrolled, callback, not interested |
| Compliance: verification, empathy | Compliance: CMS disclaimer, scope of appointment |
| Success: resolution rate, CSAT | Success: conversion rate, objection handling |

**Say This:**
> "The beautiful thing is it works for BOTH channels. For member support, we classify call reasons like billing or claims. For sales, we classify outcomes—enrolled, callback, not interested. Same AI Functions. Same pipeline. You just change the classification arrays. One platform serves both your support center AND your sales network."

---

## Slide 9: Your Quality Copilot

**Three AI Agents:**

1. **Genie Space** (SQL Analytics) - "Which agents have lowest compliance?"
2. **Knowledge Assistant** (Transcript Search) - "Find calls with missing disclosure"
3. **Supervisor Agent** (Unified) - Routes to the right tool automatically

**Say This:**
> "Now let's talk about how supervisors USE this. Genie Space is for analytics—ask in plain English, it writes SQL, returns results. Knowledge Assistant is for transcript search—finds relevant excerpts with citations. The Supervisor Agent is the magic layer—it routes questions automatically. Ask 'Which agents need coaching and show me examples?' It hits Genie for rankings, KA for evidence, and synthesizes the response."

---

## Slide 10: Let's See It In Action

**[LIVE DEMO]**

**Say This:**
> "Alright, let's see this in action. I'm going to show you three things: First, the Genie Space answering analytics questions. Second, the Knowledge Assistant searching transcripts. Third, the Supervisor Agent combining both. Let's start with Genie..."

---

## Slide 11: Demo Questions

### Genie Space (Analytics)
| Question | What It Shows |
|----------|---------------|
| "Which agents have the highest compliance scores?" | Agent rankings |
| "Show me calls that need compliance review" | Low-score call list |
| "What is the resolution rate by call reason?" | Call reason performance |

### Knowledge Assistant (Transcript Search)
| Question | What It Shows |
|----------|---------------|
| "Find calls where agents missed the recording disclosure" | Compliance issue examples |
| "Show me how top agents handle billing complaints" | Best practice examples |
| "What compliance issues appear most often?" | Pattern discovery |

### Supervisor Agent (Combined)
| Question | What It Shows |
|----------|---------------|
| "Which agents need coaching? Show specific examples." | Metrics + transcript evidence |
| "What's driving low CSAT? Show patterns." | Root cause analysis |
| "How do top agents handle price objections?" | Sales best practices |

---

## Slide 12: What You've Seen

**Bullets:**
- Raw transcripts → AI-enriched insights in one pipeline
- Four AI Functions replace external scoring tools
- Natural language access for supervisors (no SQL required)
- Works for member support AND sales prospecting
- All on Databricks—governed, secure, unified

**Say This:**
> "Let me summarize. We took raw transcripts and built a complete intelligence platform. AI Functions classify, score, and summarize every call. Gold tables power analytics. AI Agents let supervisors ask questions in plain English. This replaces expensive third-party tools. No external APIs. No batch processing. And it works for both support and sales."

---

## Slide 13: Build This In Your Environment

**Steps:**
1. Land transcripts in Unity Catalog Volume (JSON)
2. Run Bronze pipeline (parse to Delta tables)
3. Run Silver pipeline (apply AI Functions)
4. Run Gold pipeline (create aggregates)
5. Create Genie Space + Knowledge Assistant + Supervisor

**Say This:**
> "How do you build this yourself? The entire pipeline is in our notebooks. Bronze ingests JSON. Silver applies AI Functions. Gold creates aggregates. Then you create the agents. We can walk through the code in detail, or I can share the repo. Any questions?"

---

# Live Demo Script

## Demo 1: Genie Space (3-5 minutes)

Open: `Call Quality Intelligence Genie`

**Question 1:** "Which agents have the highest compliance scores?"

> "I'm asking a simple question in plain English. Genie translates this to SQL—you can see the query it generated. Here are our top agents. Notice I didn't write any SQL."

**Question 2:** "Show me calls that need compliance review"

> "Now let's find problem areas. Genie knows compliance scores below 70 need review. These are the calls supervisors should prioritize. But what actually HAPPENED in these calls? That's where Knowledge Assistant comes in."

**Question 3:** "What is the resolution rate by call reason?"

> "This shows us which call types are hardest to resolve. Complaints have the lowest resolution rate. Why? We'd need to look at actual transcripts."

---

## Demo 2: Knowledge Assistant (3-5 minutes)

Open: `Call Transcript Assistant`

**Question 1:** "Find calls where agents missed the recording disclosure"

> "Now I'm in Knowledge Assistant. This searches actual transcript content. Here's a call where the agent jumped straight into verification without mentioning the recording. This is the actual conversation—not a metric, the real words."

**Question 2:** "Show me how top agents handle billing complaints"

> "Let's flip it positive. Now we're looking for examples of GOOD behavior. Here's Sarah Chen—acknowledges frustration, explains clearly, offers a credit. This is training material from your best agents."

**Question 3:** "What compliance issues appear most often?"

> "Knowledge Assistant finds patterns across transcripts. Abrupt closing appears frequently. Incomplete verification. It's showing me actual examples of each."

---

## Demo 3: Supervisor Agent (3-5 minutes)

Open: `Call Quality Intelligence Copilot`

**Question 1:** "Which agents need coaching? Show me specific examples."

> "This is the Supervisor Agent. Watch what happens. First it queries Genie for agents with low scores. Then it queries KA for transcript examples. One question. Two data sources. Unified answer."

**Question 2:** "What's driving low CSAT? Show patterns."

> "Let's do root cause analysis. The Supervisor gets CSAT metrics from Genie, then searches transcripts for patterns. This analysis used to take hours. Now it's one question."

---

# Quick Reference: Demo Questions

## Genie Space (Analytics)
- "Which agents have the highest compliance scores?"
- "Show me calls needing review"
- "What is the resolution rate by call reason?"
- "Average CSAT by agent team"
- "Call volume trend over the last 30 days"

## Knowledge Assistant (Transcript Search)
- "Find calls with missing recording disclosure"
- "Show me how top agents handle billing complaints"
- "Examples of incomplete member verification"
- "What do frustrated members say about coverage?"
- "Find calls where the agent showed empathy"

## Supervisor Agent (Combined)
- "Which agents need coaching? Show examples."
- "What's driving low CSAT? Show patterns."
- "How do top agents handle price objections?"
- "What are the most common compliance flags? Give examples."
- "Compare agent performance across teams with transcript evidence."

---

# Q&A Talking Points

**Q: How much does this cost compared to third-party tools?**
> "AI Functions run on your serverless SQL warehouse—standard compute costs. No per-transcript fees. We've seen 70%+ cost reduction compared to external AI services."

**Q: What transcript sources does this support?**
> "Anything that produces text. Balto, Genesys, Five9, custom telephony. As long as you can export JSON with the transcript text, the pipeline ingests it."

**Q: How accurate are the AI classifications?**
> "For call reason classification, we typically see 85-90% agreement with human labelers. You can compare AI classifications against ground truth in your environment."

**Q: Can we customize the compliance rules?**
> "Absolutely. The classification categories are just arrays in SQL. Change the array, re-run the pipeline."

**Q: How real-time is this?**
> "Near-real-time. Transcripts land, pipeline processes, insights available within minutes. For true streaming, use Delta Live Tables."

---

# Troubleshooting

| Issue | Solution |
|-------|----------|
| Genie not responding | Check warehouse is running |
| KA returns no results | Verify transcripts exported to Volume |
| Supervisor times out | Simplify the question, try each agent individually |

---

# Post-Demo Follow-Up

1. Share the LinkedIn article for architecture overview
2. Offer access to the notebook repo
3. Schedule deep-dive on specific components
4. Discuss their transcript sources and compliance requirements
5. Propose POC scope if interested
