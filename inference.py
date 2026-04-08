"""
Inference Script for the DataOps Environment
=============================================
Runs a baseline LLM agent against all 3 tasks in the DataOps environment.

MANDATORY ENV VARS:
    API_BASE_URL   - The API endpoint for the LLM
    MODEL_NAME     - The model identifier
    HF_TOKEN       - Your Hugging Face / API key
    LOCAL_IMAGE_NAME - Docker image name (optional, if using from_docker_image)

STDOUT FORMAT: [START], [STEP], [END] as specified by hackathon rules.
"""

import asyncio
import json
import os
import textwrap
from typing import Any, Dict, List, Optional

from openai import OpenAI

try:
    from dataops_env import DataopsEnv, DataOpsAction
except ImportError:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from client import DataopsEnv
    from models import DataOpsAction

LOCAL_IMAGE_NAME = os.getenv("LOCAL_IMAGE_NAME")

API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
HF_TOKEN = os.getenv("HF_TOKEN")
BENCHMARK = "dataops_env"
TEMPERATURE = 0.3
MAX_TOKENS = 1024


# --- Logging helpers (exact format required by hackathon) ---

def log_start(task: str, env: str, model: str) -> None:
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    error_val = error if error else "null"
    done_val = str(done).lower()
    print(
        f"[STEP] step={step} action={action} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(
        f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}",
        flush=True,
    )


# --- System prompt for the agent ---

SYSTEM_PROMPT = textwrap.dedent("""
You are a Data Operations Engineer AI agent. You interact with a DataOps environment
by calling tools to manage data infrastructure.

You must respond with EXACTLY one JSON object per turn, in this format:
{"tool_name": "<tool_name>", "arguments": {<args>}}

Available tools and their arguments:
- list_tables() — List all tables
- describe_table(table_name) — Get schema and sample rows
- list_views() — List all views
- execute_sql(query) — Run a SELECT query
- explain_query(query) — Get query execution plan
- create_view(name, query, description) — Create a SQL view
- update_data(table, set_clause, where_clause) — Update rows
- delete_data(table, where_clause) — Delete rows
- add_quality_rule(table, column, rule_type, rule_value) — Add data quality rule
  rule_type must be: not_null, unique, range, regex, or custom_sql
- list_quality_rules() — List active quality rules
- run_quality_check(table) — Run quality checks
- create_pipeline(name, source, dest, transform_sql) — Create/update ETL pipeline
- run_pipeline(name) — Execute a pipeline
- get_pipeline_status(name) — Check pipeline status
- check_access(table) — Check who has access
- drop_view(name) — Drop/delete a SQL view
- revoke_access(user_name, table) — Revoke a user's access to a table
- submit_report(task_id, findings) — Submit final report (ends the episode)

IMPORTANT:
- Always explore the schema first before making changes.
- Use execute_sql to investigate data before modifying it.
- When done, ALWAYS call submit_report with your findings.
- Respond with ONLY the JSON object, no other text.
""").strip()


def build_user_prompt(task_prompt: str, step: int, last_result: Any, last_error: Optional[str], history: List[str]) -> str:
    history_block = "\n".join(history[-6:]) if history else "None"

    parts = []
    if step == 1:
        parts.append(f"TASK:\n{task_prompt}")
        parts.append("\nBegin by exploring the database schema. Respond with a JSON tool call.")
    else:
        if last_error:
            parts.append(f"Step {step} — Previous tool returned ERROR: {last_error}")
        else:
            result_str = json.dumps(last_result, default=str)
            if len(result_str) > 2000:
                result_str = result_str[:2000] + "... (truncated)"
            parts.append(f"Step {step} — Previous result:\n{result_str}")

        parts.append(f"\nRecent history:\n{history_block}")
        parts.append("\nDecide your next action. Respond with a JSON tool call.")

    return "\n".join(parts)


def parse_agent_response(text: str) -> Dict[str, Any]:
    """Parse the LLM response into a tool call."""
    text = text.strip()

    # Try to extract JSON from the response
    # Handle cases where model wraps in ```json ... ```
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0].strip()
    elif "```" in text:
        text = text.split("```")[1].split("```")[0].strip()

    # Find the first { and last }
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1:
        text = text[start : end + 1]

    try:
        parsed = json.loads(text)
        tool_name = parsed.get("tool_name", "")
        arguments = parsed.get("arguments", {})
        return {"tool_name": tool_name, "arguments": arguments}
    except json.JSONDecodeError:
        return {"tool_name": "list_tables", "arguments": {}}


def get_model_response(client: OpenAI, messages: List[Dict]) -> str:
    try:
        completion = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            stream=False,
        )
        return (completion.choices[0].message.content or "").strip()
    except Exception as exc:
        print(f"[DEBUG] Model request failed: {exc}", flush=True)
        return '{"tool_name": "list_tables", "arguments": {}}'


async def run_task(client: OpenAI, env, task_id: int, task_name: str, max_steps: int) -> float:
    """Run a single task and return the score."""
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False

    log_start(task=task_name, env=BENCHMARK, model=MODEL_NAME)

    try:
        result = await env.reset(task_id=task_id)
        task_prompt = result.observation.task_prompt or ""
        last_result = None
        last_error = None
        history: List[str] = []

        messages = [{"role": "system", "content": SYSTEM_PROMPT}]

        for step in range(1, max_steps + 1):
            if result.done:
                break

            user_prompt = build_user_prompt(task_prompt, step, last_result, last_error, history)
            messages.append({"role": "user", "content": user_prompt})

            response_text = get_model_response(client, messages)
            messages.append({"role": "assistant", "content": response_text})

            parsed = parse_agent_response(response_text)
            tool_name = parsed["tool_name"]
            arguments = parsed["arguments"]

            action = DataOpsAction(tool_name=tool_name, arguments=arguments)
            result = await env.step(action)

            obs = result.observation
            reward = result.reward or 0.0
            done = result.done
            error = obs.error

            rewards.append(reward)
            steps_taken = step
            last_result = obs.result
            last_error = obs.error

            action_str = f"{tool_name}({json.dumps(arguments, default=str)[:100]})"
            log_step(step=step, action=action_str, reward=reward, done=done, error=error)

            history.append(f"Step {step}: {tool_name} -> reward={reward:.2f}, error={error}")

            if done:
                # Get score from grade result
                if obs.result and isinstance(obs.result, dict):
                    grade = obs.result.get("grade", {})
                    score = grade.get("score", reward)
                else:
                    score = reward
                break

        score = min(max(score, 0.0), 1.0)
        success = score > 0.1

    except Exception as e:
        print(f"[DEBUG] Task {task_name} error: {e}", flush=True)
        score = 0.0

    finally:
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)

    return score


async def main() -> None:
    client = OpenAI(base_url=API_BASE_URL, api_key=HF_TOKEN)

    # Connect to environment
    if LOCAL_IMAGE_NAME:
        env = await DataopsEnv.from_docker_image(LOCAL_IMAGE_NAME)
    else:
        base_url = os.getenv("ENV_URL", "http://localhost:8000")
        env = DataopsEnv(base_url=base_url)

    tasks = [
        (1, "setup_revenue_dashboard", 15),
        (2, "fix_broken_pipeline", 20),
        (3, "data_incident_response", 25),
    ]

    scores = []
    try:
        for task_id, task_name, max_steps in tasks:
            score = await run_task(client, env, task_id, task_name, max_steps)
            scores.append(score)
            print(f"[DEBUG] Task {task_name}: score={score:.3f}", flush=True)
    finally:
        try:
            await env.close()
        except Exception as e:
            print(f"[DEBUG] env.close() error: {e}", flush=True)

    avg_score = sum(scores) / len(scores) if scores else 0.0
    print(f"\n[DEBUG] Final scores: {[f'{s:.3f}' for s in scores]}, average: {avg_score:.3f}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
