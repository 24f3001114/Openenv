"""
Tool implementations for the DataOps environment.
Each tool is a function that takes the database manager and arguments,
and returns a result dict or raises an error.
"""

import re
from typing import Any, Dict, List

from .database import DatabaseManager


def list_tables(db: DatabaseManager, **kwargs) -> Dict[str, Any]:
    tables = db.get_tables()
    return {"tables": tables, "count": len(tables)}


def describe_table(db: DatabaseManager, table_name: str, **kwargs) -> Dict[str, Any]:
    if not db.table_exists(table_name):
        raise ValueError(f"Table '{table_name}' does not exist")

    columns = db.get_table_info(table_name)
    row_count = db.get_row_count(table_name)
    sample = db.get_sample_rows(table_name, limit=5)

    return {
        "table_name": table_name,
        "columns": [
            {"name": c["name"], "type": c["type"], "notnull": bool(c["notnull"]), "pk": bool(c["pk"])}
            for c in columns
        ],
        "row_count": row_count,
        "sample_rows": sample,
    }


def list_views(db: DatabaseManager, **kwargs) -> Dict[str, Any]:
    views = db.get_views()
    return {"views": [{"name": v["name"], "sql": v["sql"]} for v in views], "count": len(views)}


def execute_sql(db: DatabaseManager, query: str, **kwargs) -> Dict[str, Any]:
    q = query.strip().upper()
    # Only allow SELECT queries for safety
    if not q.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed. Use other tools for modifications.")

    try:
        results = db.fetchall(query)
        # Limit to 100 rows
        truncated = len(results) > 100
        results = results[:100]
        return {
            "rows": results,
            "row_count": len(results),
            "truncated": truncated,
        }
    except Exception as e:
        raise ValueError(f"SQL error: {str(e)}")


def explain_query(db: DatabaseManager, query: str, **kwargs) -> Dict[str, Any]:
    try:
        results = db.fetchall(f"EXPLAIN QUERY PLAN {query}")
        return {"plan": results}
    except Exception as e:
        raise ValueError(f"SQL error: {str(e)}")


def create_view(db: DatabaseManager, name: str, query: str, description: str = "", **kwargs) -> Dict[str, Any]:
    if db.view_exists(name):
        raise ValueError(f"View '{name}' already exists")

    try:
        db.execute(f"CREATE VIEW [{name}] AS {query}")
        db.commit()
        return {"status": "success", "view_name": name, "message": f"View '{name}' created successfully"}
    except Exception as e:
        raise ValueError(f"Failed to create view: {str(e)}")


def update_data(db: DatabaseManager, table: str, set_clause: str, where_clause: str, **kwargs) -> Dict[str, Any]:
    if not db.table_exists(table):
        raise ValueError(f"Table '{table}' does not exist")

    # Prevent dangerous operations
    if not where_clause.strip():
        raise ValueError("WHERE clause is required for updates")

    try:
        sql = f"UPDATE [{table}] SET {set_clause} WHERE {where_clause}"
        cursor = db.execute(sql)
        db.commit()
        return {"status": "success", "rows_affected": cursor.rowcount}
    except Exception as e:
        raise ValueError(f"Update failed: {str(e)}")


def delete_data(db: DatabaseManager, table: str, where_clause: str, **kwargs) -> Dict[str, Any]:
    if not db.table_exists(table):
        raise ValueError(f"Table '{table}' does not exist")

    if not where_clause.strip():
        raise ValueError("WHERE clause is required for deletes")

    try:
        sql = f"DELETE FROM [{table}] WHERE {where_clause}"
        cursor = db.execute(sql)
        db.commit()
        return {"status": "success", "rows_deleted": cursor.rowcount}
    except Exception as e:
        raise ValueError(f"Delete failed: {str(e)}")


def add_quality_rule(
    db: DatabaseManager,
    table: str,
    column: str,
    rule_type: str,
    rule_value: str = "",
    **kwargs,
) -> Dict[str, Any]:
    valid_types = ["not_null", "unique", "range", "regex", "custom_sql"]
    if rule_type not in valid_types:
        raise ValueError(f"Invalid rule_type. Must be one of: {valid_types}")

    if not db.table_exists(table):
        raise ValueError(f"Table '{table}' does not exist")

    db.execute(
        "INSERT INTO quality_rules (table_name, column_name, rule_type, rule_value) VALUES (?,?,?,?)",
        (table, column, rule_type, rule_value),
    )
    db.commit()
    rule_id = db.fetchone("SELECT last_insert_rowid() as id")["id"]
    return {"status": "success", "rule_id": rule_id, "message": f"Quality rule added: {rule_type} on {table}.{column}"}


def update_quality_rule(
    db: DatabaseManager,
    rule_id: int,
    rule_value: str = None,
    rule_type: str = None,
    is_active: int = None,
    **kwargs,
) -> Dict[str, Any]:
    existing = db.fetchone("SELECT * FROM quality_rules WHERE rule_id = ?", (rule_id,))
    if not existing:
        raise ValueError(f"Quality rule with id {rule_id} not found")

    updates = []
    params = []
    if rule_value is not None:
        updates.append("rule_value = ?")
        params.append(rule_value)
    if rule_type is not None:
        valid_types = ["not_null", "unique", "range", "regex", "custom_sql"]
        if rule_type not in valid_types:
            raise ValueError(f"Invalid rule_type. Must be one of: {valid_types}")
        updates.append("rule_type = ?")
        params.append(rule_type)
    if is_active is not None:
        updates.append("is_active = ?")
        params.append(is_active)

    if not updates:
        raise ValueError("No fields to update. Provide rule_value, rule_type, or is_active.")

    params.append(rule_id)
    db.execute(f"UPDATE quality_rules SET {', '.join(updates)} WHERE rule_id = ?", tuple(params))
    db.commit()
    return {"status": "success", "message": f"Quality rule {rule_id} updated"}


def delete_quality_rule(db: DatabaseManager, rule_id: int, **kwargs) -> Dict[str, Any]:
    existing = db.fetchone("SELECT * FROM quality_rules WHERE rule_id = ?", (rule_id,))
    if not existing:
        raise ValueError(f"Quality rule with id {rule_id} not found")

    db.execute("DELETE FROM quality_rules WHERE rule_id = ?", (rule_id,))
    db.commit()
    return {"status": "success", "message": f"Quality rule {rule_id} deleted"}


def list_quality_rules(db: DatabaseManager, **kwargs) -> Dict[str, Any]:
    rules = db.fetchall("SELECT * FROM quality_rules WHERE is_active = 1 ORDER BY rule_id")
    return {"rules": rules, "count": len(rules)}


def run_quality_check(db: DatabaseManager, table: str, **kwargs) -> Dict[str, Any]:
    if not db.table_exists(table):
        raise ValueError(f"Table '{table}' does not exist")

    rules = db.fetchall(
        "SELECT * FROM quality_rules WHERE table_name = ? AND is_active = 1",
        (table,),
    )

    violations = []
    for rule in rules:
        col = rule["column_name"]
        rtype = rule["rule_type"]
        rval = rule["rule_value"]

        try:
            if rtype == "not_null":
                rows = db.fetchall(
                    f"SELECT rowid, * FROM [{table}] WHERE [{col}] IS NULL"
                )
                if rows:
                    violations.append({
                        "rule_id": rule["rule_id"],
                        "rule": f"{col} NOT NULL",
                        "violation_count": len(rows),
                        "sample_ids": [r.get("rowid", r.get(list(r.keys())[0])) for r in rows[:5]],
                    })

            elif rtype == "range":
                # Parse range expression like "> 0" or ">= 100"
                rows = db.fetchall(
                    f"SELECT rowid, * FROM [{table}] WHERE NOT ([{col}] {rval})"
                )
                if rows:
                    violations.append({
                        "rule_id": rule["rule_id"],
                        "rule": f"{col} {rval}",
                        "violation_count": len(rows),
                        "sample_values": [r[col] for r in rows[:5]],
                    })

            elif rtype == "unique":
                rows = db.fetchall(
                    f"SELECT [{col}], COUNT(*) as cnt FROM [{table}] GROUP BY [{col}] HAVING cnt > 1"
                )
                if rows:
                    violations.append({
                        "rule_id": rule["rule_id"],
                        "rule": f"{col} UNIQUE",
                        "violation_count": sum(r["cnt"] - 1 for r in rows),
                        "duplicate_values": [r[col] for r in rows[:5]],
                    })

            elif rtype == "custom_sql":
                rows = db.fetchall(rval)
                if rows:
                    violations.append({
                        "rule_id": rule["rule_id"],
                        "rule": f"custom: {rval[:80]}",
                        "violation_count": len(rows),
                    })

        except Exception as e:
            violations.append({
                "rule_id": rule["rule_id"],
                "rule": f"{rtype} on {col}",
                "error": str(e),
            })

    return {
        "table": table,
        "rules_checked": len(rules),
        "violations": violations,
        "total_violations": sum(v.get("violation_count", 0) for v in violations),
        "clean": len(violations) == 0,
    }


def create_pipeline(
    db: DatabaseManager,
    name: str,
    source: str,
    dest: str,
    transform_sql: str,
    **kwargs,
) -> Dict[str, Any]:
    existing = db.fetchone("SELECT * FROM pipelines WHERE name = ?", (name,))
    if existing:
        # Update existing pipeline
        db.execute(
            "UPDATE pipelines SET source_table=?, dest_table=?, transform_sql=?, status='pending', last_error=NULL WHERE name=?",
            (source, dest, transform_sql, name),
        )
        db.commit()
        return {"status": "success", "message": f"Pipeline '{name}' updated"}

    db.execute(
        "INSERT INTO pipelines (name, source_table, dest_table, transform_sql) VALUES (?,?,?,?)",
        (name, source, dest, transform_sql),
    )
    db.commit()
    return {"status": "success", "message": f"Pipeline '{name}' created"}


def run_pipeline(db: DatabaseManager, name: str, **kwargs) -> Dict[str, Any]:
    pipeline = db.fetchone("SELECT * FROM pipelines WHERE name = ?", (name,))
    if not pipeline:
        raise ValueError(f"Pipeline '{name}' not found")

    transform_sql = pipeline["transform_sql"]

    try:
        # First, check quality rules on dest table
        dest = pipeline["dest_table"]
        rules = db.fetchall(
            "SELECT * FROM quality_rules WHERE table_name = ? AND is_active = 1",
            (dest,),
        )

        # Execute the transform
        db.executescript(transform_sql)
        db.commit()

        # Check quality rules after execution
        if rules:
            check = run_quality_check(db, dest)
            if check["total_violations"] > 0:
                db.execute(
                    "UPDATE pipelines SET status='failed', last_run=datetime('now'), last_error=? WHERE name=?",
                    (f"Quality check failed: {check['total_violations']} violations", name),
                )
                db.commit()
                return {
                    "status": "failed",
                    "error": f"Pipeline ran but quality check failed with {check['total_violations']} violations",
                    "violations": check["violations"],
                }

        # Update pipeline status
        db.execute(
            "UPDATE pipelines SET status='success', last_run=datetime('now'), last_error=NULL WHERE name=?",
            (name,),
        )
        db.commit()

        row_count = db.get_row_count(dest)
        return {
            "status": "success",
            "message": f"Pipeline '{name}' ran successfully",
            "rows_in_destination": row_count,
        }

    except Exception as e:
        db.execute(
            "UPDATE pipelines SET status='failed', last_run=datetime('now'), last_error=? WHERE name=?",
            (str(e), name),
        )
        db.commit()
        return {"status": "failed", "error": str(e)}


def get_pipeline_status(db: DatabaseManager, name: str, **kwargs) -> Dict[str, Any]:
    pipeline = db.fetchone("SELECT * FROM pipelines WHERE name = ?", (name,))
    if not pipeline:
        raise ValueError(f"Pipeline '{name}' not found")

    return {
        "name": pipeline["name"],
        "source_table": pipeline["source_table"],
        "dest_table": pipeline["dest_table"],
        "transform_sql": pipeline["transform_sql"],
        "status": pipeline["status"],
        "last_run": pipeline["last_run"],
        "last_error": pipeline["last_error"],
        "schedule": pipeline["schedule"],
    }


def check_access(db: DatabaseManager, table: str, **kwargs) -> Dict[str, Any]:
    entries = db.fetchall(
        "SELECT * FROM access_control WHERE table_name = ? ORDER BY user_name",
        (table,),
    )
    return {"table": table, "access_entries": entries, "count": len(entries)}


def drop_view(db: DatabaseManager, name: str, **kwargs) -> Dict[str, Any]:
    if not db.view_exists(name):
        raise ValueError(f"View '{name}' does not exist")
    try:
        db.execute(f"DROP VIEW [{name}]")
        db.commit()
        return {"status": "success", "message": f"View '{name}' dropped"}
    except Exception as e:
        raise ValueError(f"Failed to drop view: {str(e)}")


def revoke_access(db: DatabaseManager, user_name: str, table: str, **kwargs) -> Dict[str, Any]:
    existing = db.fetchall(
        "SELECT * FROM access_control WHERE user_name = ? AND table_name = ?",
        (user_name, table),
    )
    if not existing:
        raise ValueError(f"No access entry found for user '{user_name}' on table '{table}'")
    db.execute(
        "DELETE FROM access_control WHERE user_name = ? AND table_name = ?",
        (user_name, table),
    )
    db.commit()
    return {"status": "success", "message": f"Access revoked for '{user_name}' on '{table}'"}


def submit_report(db: DatabaseManager, task_id: int, findings: str, **kwargs) -> Dict[str, Any]:
    # Log the report in audit_log
    db.execute(
        "INSERT INTO audit_log (action, table_name, user_name, details, timestamp) VALUES (?,?,?,?,datetime('now'))",
        ("REPORT_SUBMITTED", "dataops_env", "agent", findings),
    )
    db.commit()
    return {"status": "submitted", "task_id": task_id, "message": "Report submitted for grading"}


# Tool registry
TOOLS = {
    "list_tables": list_tables,
    "describe_table": describe_table,
    "list_views": list_views,
    "execute_sql": execute_sql,
    "explain_query": explain_query,
    "create_view": create_view,
    "update_data": update_data,
    "delete_data": delete_data,
    "add_quality_rule": add_quality_rule,
    "update_quality_rule": update_quality_rule,
    "delete_quality_rule": delete_quality_rule,
    "list_quality_rules": list_quality_rules,
    "run_quality_check": run_quality_check,
    "create_pipeline": create_pipeline,
    "run_pipeline": run_pipeline,
    "get_pipeline_status": get_pipeline_status,
    "check_access": check_access,
    "drop_view": drop_view,
    "revoke_access": revoke_access,
    "submit_report": submit_report,
}

TOOL_DESCRIPTIONS = {
    "list_tables": "List all tables in the database",
    "describe_table": "Get schema, row count, and sample rows for a table. Args: table_name",
    "list_views": "List all views and their SQL definitions",
    "execute_sql": "Run a SELECT query and get results. Args: query",
    "explain_query": "Get the execution plan for a SQL query. Args: query",
    "create_view": "Create a new SQL view. Args: name, query, description",
    "update_data": "Update rows in a table. Args: table, set_clause, where_clause",
    "delete_data": "Delete rows from a table. Args: table, where_clause",
    "add_quality_rule": "Add a data quality rule. Args: table, column, rule_type (not_null|unique|range|regex|custom_sql), rule_value",
    "update_quality_rule": "Update an existing quality rule. Args: rule_id, rule_value (optional), rule_type (optional), is_active (optional, 0 or 1)",
    "delete_quality_rule": "Delete a quality rule. Args: rule_id",
    "list_quality_rules": "List all active quality rules",
    "run_quality_check": "Run quality checks on a table. Args: table",
    "create_pipeline": "Create or update an ETL pipeline. Args: name, source, dest, transform_sql",
    "run_pipeline": "Execute a pipeline. Args: name",
    "get_pipeline_status": "Get pipeline status and details. Args: name",
    "check_access": "Check who has access to a table. Args: table",
    "drop_view": "Drop/delete a SQL view. Args: name",
    "revoke_access": "Revoke a user's access to a table. Args: user_name, table",
    "submit_report": "Submit task completion report. Args: task_id, findings",
}
