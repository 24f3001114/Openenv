"""
Task definitions for the DataOps environment.
Each task has a prompt, max steps, and grading checks.
"""

TASKS = {
    1: {
        "name": "setup_revenue_dashboard",
        "title": "Set Up a Revenue Dashboard",
        "difficulty": "easy",
        "max_steps": 15,
        "prompt": (
            "The sales team needs a daily revenue dashboard. Your tasks:\n"
            "1. Explore the database schema to understand the data.\n"
            "2. Add data quality rules: order amounts must be positive (> 0), "
            "and customer_id must not be null.\n"
            "3. Run a quality check on the orders table and fix any violations.\n"
            "4. Create a view called 'daily_revenue' that shows total revenue "
            "per day per region by joining orders with products.\n"
            "5. Submit a report summarizing what you did.\n\n"
            "Available tools: list_tables, describe_table, list_views, execute_sql, "
            "explain_query, create_view, update_data, delete_data, add_quality_rule, "
            "list_quality_rules, run_quality_check, create_pipeline, run_pipeline, "
            "get_pipeline_status, check_access, drop_view, revoke_access, "
            "update_quality_rule, delete_quality_rule, submit_report"
        ),
    },
    2: {
        "name": "fix_broken_pipeline",
        "title": "Fix a Broken Pipeline",
        "difficulty": "medium",
        "max_steps": 20,
        "prompt": (
            "The nightly ETL pipeline 'customer_metrics' has been failing for 3 days. "
            "The customer success team can't see updated metrics. Your tasks:\n"
            "1. Check the pipeline status to see the error.\n"
            "2. Investigate the root cause — examine the pipeline's transform SQL, "
            "the source table schema, and any quality rules.\n"
            "3. Fix ALL issues you find (there may be multiple problems).\n"
            "4. Re-run the pipeline and verify it succeeds.\n"
            "5. Submit a report detailing what was broken and how you fixed it.\n\n"
            "Available tools: list_tables, describe_table, list_views, execute_sql, "
            "explain_query, create_view, update_data, delete_data, add_quality_rule, "
            "list_quality_rules, run_quality_check, create_pipeline, run_pipeline, "
            "get_pipeline_status, check_access, drop_view, revoke_access, "
            "update_quality_rule, delete_quality_rule, submit_report"
        ),
    },
    3: {
        "name": "data_incident_response",
        "title": "Data Incident — Investigate and Remediate Corruption",
        "difficulty": "hard",
        "max_steps": 25,
        "prompt": (
            "SECURITY ALERT: Unauthorized user 'external_analyst' was found with "
            "write access to the customers table. Customer data may have been tampered with.\n\n"
            "Your tasks:\n"
            "1. Check access controls to confirm the unauthorized access.\n"
            "2. Review the audit log to understand what happened.\n"
            "3. Compare the customers table with the customers_backup table "
            "to identify corrupted records.\n"
            "4. Restore corrupted customer records from the backup.\n"
            "5. Remove any unauthorized views or objects created by the attacker.\n"
            "6. Revoke the unauthorized access.\n"
            "7. Submit a detailed incident report with: number of affected records, "
            "what was changed, and the attack vector.\n\n"
            "Available tools: list_tables, describe_table, list_views, execute_sql, "
            "explain_query, create_view, update_data, delete_data, add_quality_rule, "
            "list_quality_rules, run_quality_check, create_pipeline, run_pipeline, "
            "get_pipeline_status, check_access, drop_view, revoke_access, "
            "update_quality_rule, delete_quality_rule, submit_report"
        ),
    },
}
