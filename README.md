---
title: DataOps Environment Server
emoji: 🔧
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 8000
base_path: /web
tags:
  - openenv
---

# DataOps Environment

A real-world OpenEnv environment for training AI agents on **data operations tasks**: data quality management, ETL pipeline debugging, and security incident response.

## Motivation

Every company with data needs data engineers to manage pipelines, enforce quality rules, and respond to data incidents. This environment simulates a realistic DataOps workspace where an AI agent must explore databases, diagnose issues, fix problems, and report findings — the same tasks human data engineers perform daily.

## Action Space

```python
class DataOpsAction(Action):
    tool_name: str      # Name of the tool to call
    arguments: dict     # Tool-specific arguments
```

### Available Tools (16)

| Tool | Description | Arguments |
|------|-------------|-----------|
| `list_tables` | List all tables in the database | -- |
| `describe_table` | Get schema, row count, sample rows | `table_name` |
| `list_views` | List all SQL views | -- |
| `execute_sql` | Run a SELECT query | `query` |
| `explain_query` | Get query execution plan | `query` |
| `create_view` | Create a SQL view | `name`, `query`, `description` |
| `update_data` | Update rows in a table | `table`, `set_clause`, `where_clause` |
| `delete_data` | Delete rows from a table | `table`, `where_clause` |
| `add_quality_rule` | Add a data quality rule | `table`, `column`, `rule_type`, `rule_value` |
| `list_quality_rules` | List active quality rules | -- |
| `run_quality_check` | Run quality checks on a table | `table` |
| `create_pipeline` | Create/update an ETL pipeline | `name`, `source`, `dest`, `transform_sql` |
| `run_pipeline` | Execute a pipeline | `name` |
| `get_pipeline_status` | Check pipeline status and errors | `name` |
| `check_access` | Check who has access to a table | `table` |
| `submit_report` | Submit task completion report | `task_id`, `findings` |

## Observation Space

```python
class DataOpsObservation(Observation):
    result: Any           # Tool execution result
    error: Optional[str]  # Error message if tool failed
    tool_name: str        # Which tool was called
    available_tools: Optional[List[str]]  # Available tools (on reset)
    task_prompt: Optional[str]            # Task description (on reset)
    done: bool            # Whether episode has ended
    reward: float         # Step reward
```

## Tasks

### Task 1: Set Up a Revenue Dashboard (Easy)
- **Goal**: Create a `daily_revenue` view after cleaning data quality issues
- **Issues to find**: 15 orders with invalid amounts, 8 orders with null customer IDs
- **Skills tested**: Schema exploration, data quality rules, data cleaning, view creation
- **Max steps**: 15

### Task 2: Fix a Broken Pipeline (Medium)
- **Goal**: Diagnose and fix a failing ETL pipeline
- **Issues to find**: Wrong column reference, case mismatch in filter, overly strict quality rule
- **Skills tested**: Pipeline debugging, SQL analysis, multi-issue diagnosis
- **Max steps**: 20

### Task 3: Data Incident Response (Hard)
- **Goal**: Investigate unauthorized data tampering, restore corrupted records, secure the system
- **Issues to find**: 47 corrupted customer emails, unauthorized access, malicious view
- **Skills tested**: Forensic investigation, data restoration, access control, incident reporting
- **Max steps**: 25

## Reward Structure

| Event | Reward |
|-------|--------|
| Successful tool execution | +0.29 |
| Tool returned error | -0.20 |
| Unknown tool called | -0.20 |
| Internal error | -0.30 |
| Submit report (final grade) | 0.0 - 1.0 based on task completion |

## Grading

All grading is **100% programmatic** with no LLM judging. Each task has 5-7 SQL-based state checks.

Score = (checks passed) / (total checks), clamped to [0.0, 1.0].

## Setup

```bash
pip install openenv-core
cd dataops_env
uvicorn server.app:app --host 0.0.0.0 --port 8000
```

## Docker

```bash
docker build -t dataops-env -f server/Dockerfile .
docker run -p 8000:8000 dataops-env
```

## Baseline Scores

| Task | Difficulty | Expected Score Range |
|------|-----------|---------------------|
| Task 1: Revenue Dashboard | Easy | 0.6 - 1.0 |
| Task 2: Fix Pipeline | Medium | 0.3 - 0.8 |
| Task 3: Incident Response | Hard | 0.1 - 0.5 |
