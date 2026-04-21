# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Member Support Call Transcripts
# MAGIC
# MAGIC Generates synthetic member support call transcripts for demonstrating
# MAGIC Databricks AI Functions for transcript parsing, enrichment, and compliance scoring.
# MAGIC
# MAGIC **Use Case**: Replace external transcript scoring tools with native Databricks AI Functions
# MAGIC
# MAGIC **Call Type**: Inbound member support calls (members calling for help)
# MAGIC
# MAGIC **Output:** 100 JSON transcript files in `/Volumes/humana_payer/conversation-intelligence-copilot/raw_data/transcripts/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Configuration
CATALOG = "humana_payer"
SCHEMA = "conversation-intelligence-copilot"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/transcripts"

N_CALLS = 100  # Generate 100 calls for demo
SEED = 42

random.seed(SEED)

print(f"Generating {N_CALLS} member support call transcripts")
print(f"Output path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data

# COMMAND ----------

# Support agents
AGENTS = [
    {"agent_id": "AGT-001", "name": "Sarah Johnson", "team": "Tier 1 Support", "tenure_months": 24},
    {"agent_id": "AGT-002", "name": "Michael Chen", "team": "Tier 1 Support", "tenure_months": 18},
    {"agent_id": "AGT-003", "name": "Emily Rodriguez", "team": "Tier 1 Support", "tenure_months": 36},
    {"agent_id": "AGT-004", "name": "David Kim", "team": "Tier 2 Support", "tenure_months": 48},
    {"agent_id": "AGT-005", "name": "Jessica Martinez", "team": "Tier 2 Support", "tenure_months": 30},
    {"agent_id": "AGT-006", "name": "Robert Taylor", "team": "Tier 1 Support", "tenure_months": 12},
    {"agent_id": "AGT-007", "name": "Amanda Wilson", "team": "Tier 2 Support", "tenure_months": 42},
    {"agent_id": "AGT-008", "name": "Christopher Lee", "team": "Tier 1 Support", "tenure_months": 8},
    {"agent_id": "AGT-009", "name": "Nicole Brown", "team": "Tier 1 Support", "tenure_months": 15},
    {"agent_id": "AGT-010", "name": "James Anderson", "team": "Tier 2 Support", "tenure_months": 54},
]

# Call reasons/categories
CALL_REASONS = [
    "billing_inquiry",
    "coverage_question",
    "claims_status",
    "prescription_issue",
    "provider_lookup",
    "enrollment_change",
    "complaint",
    "general_inquiry",
]

# Plan types
PLAN_TYPES = ["Medicare Advantage", "Medicare Supplement", "Part D PDP", "Dual Eligible SNP"]

# Transcript sources (simulating different call center systems)
TRANSCRIPT_SOURCES = ["Balto", "Genesys"]

# Compliance checkpoints that agents should follow
COMPLIANCE_CHECKPOINTS = [
    "identity_verification",  # Verify member identity
    "hipaa_acknowledgment",   # HIPAA-compliant communication
    "call_recording_notice",  # Inform about call recording
    "resolution_confirmation", # Confirm issue resolved
    "callback_offer",         # Offer callback if needed
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transcript Templates

# COMMAND ----------

def generate_member():
    """Generate a random member."""
    first_names = ["John", "Mary", "Robert", "Patricia", "William", "Jennifer", "James", "Linda",
                   "Richard", "Barbara", "Thomas", "Susan", "Charles", "Margaret", "Joseph", "Dorothy"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]

    return {
        "member_id": f"MBR-{random.randint(100000, 999999)}",
        "name": f"{random.choice(first_names)} {random.choice(last_names)}",
        "age": random.randint(65, 89),
        "plan_type": random.choice(PLAN_TYPES),
        "member_since": f"{random.randint(2018, 2024)}-{random.randint(1,12):02d}-01",
    }


def generate_billing_inquiry_transcript(agent, member):
    """Generate a billing inquiry call transcript."""
    utterances = []
    seq = 1

    # Opening - with compliance (call recording notice)
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you for calling Member Services, this call may be recorded for quality assurance. My name is {agent['name']}. How may I help you today?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Hi, I received a bill that I don't understand. It says I owe $150 but I thought my plan covered this."
    })
    seq += 1

    # Identity verification (compliance checkpoint)
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I'd be happy to help you with that billing question. For security purposes, can you please verify your date of birth and the last four digits of your Social Security number?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"Sure, my date of birth is March 15, 1955, and the last four are 4532."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you for verifying that, {member['name'].split()[0]}. I can see your account now. Let me look into this charge for you."
    })
    seq += 1

    # Investigation
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I see the charge is for a specialist visit on October 15th. It looks like this provider is out-of-network, which is why there's a higher cost share."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "But my doctor referred me to this specialist. I didn't know they were out of network."
    })
    seq += 1

    # Resolution
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I understand that's frustrating. Let me check if we can apply any exceptions since you were referred. Please hold for just a moment."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Good news! Since you were referred by your PCP, I can submit a request to have this reprocessed as in-network. This should reduce your cost to about $30."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Oh, that's great! Thank you so much for looking into that."
    })
    seq += 1

    # Resolution confirmation (compliance checkpoint)
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! Just to confirm, I've submitted the reprocessing request. You should see an updated bill within 7-10 business days. Is there anything else I can help you with today?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, that's all. Thank you for your help!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! Thank you for being a valued member. Have a great day!"
    })

    return utterances, {
        "call_reason": "billing_inquiry",
        "resolution": "resolved",
        "compliance_score": 95,  # Good compliance - all checkpoints met
        "quality_notes": "Agent properly verified identity, explained issue clearly, found resolution",
        "compliance_flags": []
    }


def generate_coverage_question_transcript(agent, member):
    """Generate a coverage question call transcript."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Member Services, this is {agent['name']}. This call is being recorded. How can I assist you?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I need to know if my plan covers knee replacement surgery."
    })
    seq += 1

    # Identity verification
    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I can definitely help you with coverage information. May I have your member ID and date of birth to pull up your plan details?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"My member ID is {member['member_id']} and my birthday is June 22, 1952."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you, {member['name'].split()[0]}. I have your {member['plan_type']} plan information here. Knee replacement surgery is covered under your plan. However, you'll need prior authorization from your doctor."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "What does that mean? What do I need to do?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Your orthopedic surgeon will need to submit a prior authorization request to us. They'll provide medical documentation showing the surgery is necessary. Once approved, you can schedule the procedure."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "How long does the authorization take?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Standard authorization takes 5-7 business days. If your doctor marks it as urgent, we can expedite it to 72 hours. Your cost share for inpatient surgery is $350 per admission."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Okay, I understand. Thank you for explaining that."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! Is there anything else you'd like to know about your coverage?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, that covers it. Thanks!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Thank you for calling. Take care!"
    })

    return utterances, {
        "call_reason": "coverage_question",
        "resolution": "resolved",
        "compliance_score": 90,
        "quality_notes": "Agent verified identity, provided clear coverage information and cost estimates",
        "compliance_flags": []
    }


def generate_complaint_transcript(agent, member):
    """Generate a complaint call transcript - potentially lower quality/compliance."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Hello, Member Services."
    })  # Missing call recording notice - compliance issue
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I've been trying to get my prescription filled for three days and the pharmacy keeps saying there's a problem with my coverage!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "What's your name?"  # Abrupt, missing empathy
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"It's {member['name']}. This is ridiculous. I need my blood pressure medication!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Let me look at your account. What's your member ID?"
    })  # No empathy acknowledgment
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"It's {member['member_id']}. I've been a member for years and never had this problem before."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I see there was a system update and your pharmacy benefits weren't synced properly. I can fix that."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Well, can you fix it now? I really need this medication."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Yes, I've updated your file. You should be able to fill it now. Just go back to the pharmacy."
    })  # No confirmation, no callback offer
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Fine. I hope it works this time."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "It should. Bye."  # Abrupt ending
    })

    return utterances, {
        "call_reason": "complaint",
        "resolution": "resolved",
        "compliance_score": 55,  # Low compliance - missing call recording notice, no proper verification
        "quality_notes": "Missing call recording notice, incomplete identity verification, lacked empathy, abrupt closing",
        "compliance_flags": ["missing_recording_notice", "incomplete_verification", "no_empathy", "abrupt_closing"]
    }


def generate_claims_status_transcript(agent, member):
    """Generate a claims status inquiry transcript."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you for calling Member Services. This call may be monitored for quality purposes. I'm {agent['name']}. How may I help you?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I submitted a claim about a month ago and I haven't heard anything. Can you check on it?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Absolutely, I'd be happy to check your claim status. For verification, could you please provide your full name and date of birth?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"Yes, I'm {member['name']}, born August 8, 1949."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Thank you. I can see your recent claim for the MRI from October 3rd. It's currently in review and should be processed within the next 5 business days."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Why is it taking so long?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I understand the wait is frustrating. This particular claim required additional medical review because it was a diagnostic imaging service. Our clinical team is reviewing the documentation to ensure proper coverage."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Will I be notified when it's done?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Yes, you'll receive an Explanation of Benefits in the mail, and you can also check the status online through our member portal. Would you like me to walk you through setting up online access?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, I'll wait for the mail. Thanks for checking."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! To confirm, your claim should be processed within 5 business days and you'll receive notification by mail. Is there anything else I can assist you with?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, that's all. Thank you."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Thank you for calling. Have a wonderful day!"
    })

    return utterances, {
        "call_reason": "claims_status",
        "resolution": "resolved",
        "compliance_score": 92,
        "quality_notes": "Proper verification, clear explanation, offered additional help, confirmed next steps",
        "compliance_flags": []
    }


def generate_prescription_issue_transcript(agent, member):
    """Generate a prescription issue transcript with potential escalation."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Member Services, {agent['name']} speaking. This call is recorded for quality and training. How can I help you today?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "My doctor prescribed a new medication but the pharmacy says it's not covered. It's for my diabetes."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I'm sorry to hear you're having trouble with your prescription. Let me help you with that. Can I have your member ID and verify your date of birth?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"My ID is {member['member_id']}, and I was born January 3, 1956."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you for verifying that, {member['name'].split()[0]}. What's the name of the medication your doctor prescribed?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "It's Ozempic. My doctor said it would help control my blood sugar better."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I see. Ozempic is on our formulary but it requires prior authorization and step therapy. This means your doctor needs to document that other medications haven't worked for you."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I've already tried Metformin and it upset my stomach. This is frustrating!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I completely understand your frustration. The good news is that since you've already tried Metformin, your doctor can submit that information with the prior authorization request. Let me give you the fax number for your doctor's office."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Can't you just approve it now? I really need this medication."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I wish I could approve it directly, but the authorization needs to come through our pharmacy department with your doctor's documentation. However, I can flag your case as urgent, which will expedite the review to 24-48 hours once we receive the request."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Okay, I guess that's the best you can do. What's the fax number?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "The prior authorization fax number is 1-800-555-0199. I'll also send you a secure message through the member portal with these details. Would you like me to call you back once the authorization is processed?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Yes, please call me back. My number is 555-123-4567."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Perfect, I've noted your callback number. To summarize: your doctor will fax the prior authorization with your Metformin history, we'll expedite the review, and I'll call you back within 48 hours with an update. Is there anything else I can help with?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, thank you for your help."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! I'll be in touch soon. Take care!"
    })

    return utterances, {
        "call_reason": "prescription_issue",
        "resolution": "pending_followup",
        "compliance_score": 98,
        "quality_notes": "Excellent empathy, clear explanation of process, offered callback, summarized next steps",
        "compliance_flags": []
    }


def generate_general_inquiry_transcript(agent, member):
    """Generate a general inquiry transcript."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Good afternoon, Member Services. This is {agent['name']} and this call may be recorded. What can I do for you today?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Hi, I just want to make sure my premium payment went through. I mailed a check last week."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I'd be happy to check on that for you. May I have your name and member ID please?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"Sure, I'm {member['name']}, member ID {member['member_id']}."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "And for verification, could you confirm your date of birth?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "December 12, 1951."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Thank you. Let me pull up your payment history... Yes, I can confirm we received your check payment of $145.50 on November 10th. Your account is current through December."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Oh good, thank you. That's a relief."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! Is there anything else I can help you with while I have you on the line?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Actually, can I set up automatic payments so I don't have to worry about this?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Absolutely! I can help you set that up right now. We can draft from a checking account or charge a credit or debit card. Which would you prefer?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Let's do checking account."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Perfect. I'll need your bank routing number and account number. These will be stored securely and only used for your monthly premium."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Okay, the routing number is 123456789 and the account is 987654321."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Got it. Your automatic payment is now set up and will begin with your January premium. You'll receive a confirmation letter in the mail. Is there anything else?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, that's everything. Thank you so much!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "My pleasure! Thank you for being a member. Have a great day!"
    })

    return utterances, {
        "call_reason": "general_inquiry",
        "resolution": "resolved",
        "compliance_score": 94,
        "quality_notes": "Proper verification, proactively offered autopay setup, clear communication",
        "compliance_flags": []
    }


def generate_provider_lookup_transcript(agent, member):
    """Generate a provider lookup call transcript."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you for calling Member Services. This call is recorded for quality assurance. My name is {agent['name']}. How can I assist you today?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I'm looking for a new primary care doctor. My current one is retiring."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I can help you find a new primary care physician. May I have your member ID and date of birth to look up your plan's network?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"Sure, it's {member['member_id']}, and my birthday is April 5, 1948."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Thank you, {member['name'].split()[0]}. I see you're in our {member['plan_type']} plan. What's your zip code so I can search for providers near you?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I'm in 45202."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I found several primary care physicians within 5 miles of your location. Do you have any preferences, such as a male or female doctor, or any specific medical group?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I'd prefer a female doctor if possible."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I have three female PCPs accepting new patients. Dr. Lisa Park at Community Health Center, Dr. Maria Santos at Riverside Medical, and Dr. Jennifer Walsh at Primary Care Associates. Would you like contact information for any of these?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Yes, can I get the info for Dr. Park?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Dr. Lisa Park is at 123 Main Street, Suite 200. Her office number is 555-234-5678. She's 1.2 miles from your zip code. Would you like me to send this information to you via email or mail?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Email would be great."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I'll send that to the email on file. Once you select your new PCP, you can update your selection through our member portal or call us back. Is there anything else I can help with?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No, that's very helpful. Thank you!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "You're welcome! Thank you for calling. Have a great day!"
    })

    return utterances, {
        "call_reason": "provider_lookup",
        "resolution": "resolved",
        "compliance_score": 96,
        "quality_notes": "Verified identity, gathered preferences, provided multiple options, offered follow-up",
        "compliance_flags": []
    }


def generate_escalation_transcript(agent, member):
    """Generate a call that requires escalation - compliance issues."""
    utterances = []
    seq = 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": f"Member Services, this is {agent['name']}."  # Missing recording notice
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I want to speak to a supervisor! I've called three times about a claim and nobody can help me!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I understand you're frustrated. Can I try to help you first before getting a supervisor?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "No! I've explained this too many times. Just get me a supervisor!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "I'll connect you with a supervisor. Can I at least get your member ID so they have your information?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": f"It's {member['member_id']}."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": agent["name"], "speaker_role": "agent",
        "text": "Thank you. Please hold while I transfer you to a supervisor. This may take a few minutes."
    })
    seq += 1

    # Supervisor joins
    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": f"Hello, this is Maria Lopez, a supervisor with Member Services. I understand you've been having an ongoing issue. I apologize for the frustration. Can you please tell me what's been happening?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "I had surgery in August and the hospital is billing me $3,000. Your company says it was processed but I never got an explanation of benefits!"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": f"I'm so sorry you've been dealing with this, {member['name'].split()[0]}. Let me pull up your account and review the claim in detail. For security, can you confirm your date of birth?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "September 15, 1950."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": "Thank you. I can see the claim was processed on September 3rd. It looks like the EOB was sent to an old address. I'll resend it to your current address and email you a copy today. The claim was paid and your responsibility is only $450, not $3,000."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Oh! That's a big difference. Why couldn't the other agents tell me this?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": "I apologize that this wasn't resolved sooner. I'll make sure to follow up with the team. I'm also going to call the hospital billing department directly and provide them with the correct payment information. You shouldn't have to do anything else."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Thank you. I really appreciate you taking care of this."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": "Absolutely. I'll call you back within 24 hours to confirm everything is resolved. Is this number the best way to reach you?"
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": member["name"], "speaker_role": "member",
        "text": "Yes, that's fine."
    })
    seq += 1

    utterances.append({
        "sequence": seq, "speaker": "Supervisor Maria Lopez", "speaker_role": "supervisor",
        "text": "Thank you for your patience, {member['name'].split()[0]}. I'll speak with you tomorrow. Have a good day."
    })

    return utterances, {
        "call_reason": "complaint",
        "resolution": "escalated_resolved",
        "compliance_score": 75,  # Initial agent issues, supervisor recovered
        "quality_notes": "Initial agent missing recording notice, proper escalation, supervisor demonstrated excellent recovery",
        "compliance_flags": ["missing_recording_notice", "escalation_required"]
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Transcripts

# COMMAND ----------

# Transcript generator functions by call reason
TRANSCRIPT_GENERATORS = {
    "billing_inquiry": generate_billing_inquiry_transcript,
    "coverage_question": generate_coverage_question_transcript,
    "complaint": generate_complaint_transcript,
    "claims_status": generate_claims_status_transcript,
    "prescription_issue": generate_prescription_issue_transcript,
    "general_inquiry": generate_general_inquiry_transcript,
    "provider_lookup": generate_provider_lookup_transcript,
    "escalation": generate_escalation_transcript,
}

# Weight distribution for call reasons
CALL_REASON_WEIGHTS = {
    "billing_inquiry": 0.18,
    "coverage_question": 0.18,
    "claims_status": 0.15,
    "prescription_issue": 0.15,
    "complaint": 0.08,
    "general_inquiry": 0.15,
    "provider_lookup": 0.06,
    "escalation": 0.05,
}

def generate_call_transcript(call_id, call_date):
    """Generate a complete call transcript."""
    agent = random.choice(AGENTS)
    member = generate_member()

    # Select call reason based on weights
    reasons = list(CALL_REASON_WEIGHTS.keys())
    weights = list(CALL_REASON_WEIGHTS.values())
    call_reason = random.choices(reasons, weights=weights, k=1)[0]

    # Get appropriate generator
    generator = TRANSCRIPT_GENERATORS.get(call_reason, generate_general_inquiry_transcript)
    utterances, metadata = generator(agent, member)

    # Add timestamps to utterances
    current_time = call_date
    for utt in utterances:
        utt["start_time"] = current_time.isoformat()
        duration = random.uniform(2, 8)
        utt["duration_seconds"] = round(duration, 1)
        current_time += timedelta(seconds=duration + random.uniform(0.5, 2))

    # Calculate call duration
    duration_seconds = (current_time - call_date).total_seconds()

    # Determine CSAT based on compliance score and resolution
    if metadata["compliance_score"] < 60:
        base_csat = random.choice([1, 2, 2, 3])
    elif metadata["compliance_score"] < 80:
        base_csat = random.choice([2, 3, 3, 4])
    elif metadata["compliance_score"] < 90:
        base_csat = random.choice([3, 4, 4, 5])
    else:
        base_csat = random.choice([4, 4, 5, 5, 5])

    csat = max(1, min(5, base_csat))

    # Build transcript JSON
    transcript = {
        "call_id": call_id,
        "transcript_source": random.choice(TRANSCRIPT_SOURCES),
        "metadata": {
            "call_start_time": call_date.isoformat(),
            "call_end_time": current_time.isoformat(),
            "duration_seconds": int(duration_seconds),
            "call_reason": metadata["call_reason"],
            "resolution": metadata["resolution"],
            "csat_score": csat,
        },
        "quality_audit": {
            "compliance_score": metadata["compliance_score"],
            "quality_notes": metadata["quality_notes"],
            "compliance_flags": metadata.get("compliance_flags", []),
        },
        "agent": agent,
        "member": {
            "member_id": member["member_id"],
            "name": member["name"],
            "age": member["age"],
            "plan_type": member["plan_type"],
            "member_since": member["member_since"],
        },
        "utterances": utterances,
    }

    return transcript

# COMMAND ----------

# Generate all transcripts
print(f"Generating {N_CALLS} call transcripts...")

# Date range - last 3 months
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

transcripts = []
for i in range(N_CALLS):
    call_id = f"CALL-{i+1:06d}"

    # Random call date within range
    days_offset = random.randint(0, 90)
    call_date = start_date + timedelta(days=days_offset, hours=random.randint(8, 17), minutes=random.randint(0, 59))

    transcript = generate_call_transcript(call_id, call_date)
    transcripts.append(transcript)

    if (i + 1) % 20 == 0:
        print(f"  Generated {i + 1}/{N_CALLS} transcripts")

print(f"Generated {len(transcripts)} transcripts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save to Volume

# COMMAND ----------

# Ensure volume directory exists
dbutils.fs.mkdirs(VOLUME_PATH)

# Save each transcript as JSON
print(f"Saving transcripts to {VOLUME_PATH}...")
for transcript in transcripts:
    file_path = f"{VOLUME_PATH}/{transcript['call_id']}.json"
    dbutils.fs.put(file_path, json.dumps(transcript, indent=2), overwrite=True)

print(f"Saved {len(transcripts)} transcripts to Volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# Verify files
files = dbutils.fs.ls(VOLUME_PATH)
print(f"Files in Volume: {len(files)}")

# Show sample transcript
sample = transcripts[0]
print(f"\nSample Transcript: {sample['call_id']}")
print(f"  Source: {sample['transcript_source']}")
print(f"  Reason: {sample['metadata']['call_reason']}")
print(f"  Duration: {sample['metadata']['duration_seconds']}s")
print(f"  Resolution: {sample['metadata']['resolution']}")
print(f"  CSAT: {sample['metadata']['csat_score']}")
print(f"  Compliance Score: {sample['quality_audit']['compliance_score']}")
print(f"  Compliance Flags: {sample['quality_audit']['compliance_flags']}")
print(f"  Agent: {sample['agent']['name']}")
print(f"  Member: {sample['member']['name']}")
print(f"  Utterances: {len(sample['utterances'])}")

# Distribution stats
print("\n=== Call Reason Distribution ===")
reason_counts = {}
for t in transcripts:
    reason = t['metadata']['call_reason']
    reason_counts[reason] = reason_counts.get(reason, 0) + 1
for reason, count in sorted(reason_counts.items()):
    print(f"  {reason}: {count}")

print("\n=== Compliance Score Distribution ===")
scores = [t['quality_audit']['compliance_score'] for t in transcripts]
print(f"  Min: {min(scores)}, Max: {max(scores)}, Avg: {sum(scores)/len(scores):.1f}")

print("\n=== Transcript Source Distribution ===")
source_counts = {}
for t in transcripts:
    source = t['transcript_source']
    source_counts[source] = source_counts.get(source, 0) + 1
for source, count in sorted(source_counts.items()):
    print(f"  {source}: {count}")

print("\n=== Calls with Compliance Flags ===")
flagged = sum(1 for t in transcripts if t['quality_audit']['compliance_flags'])
print(f"  Flagged calls: {flagged}/{len(transcripts)}")
