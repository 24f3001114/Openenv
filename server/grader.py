"""
Programmatic grading for the DataOps environment.
All grading is based on database state checks — no LLM judging.

Scoring design:
- Each task has weighted checks that sum to < 1.0
- A small base score (0.05) is always given for attempting the task
- Maximum achievable score is 0.95, not 1.0
- This ensures scores are ALWAYS strictly between 0 and 1
"""

import random
from typing import Any, Dict, List, Tuple

from .database import DatabaseManager


def grade_task1(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 1: Set Up a Revenue Dashboard.

    Weighted scoring:
      0.05 base (attempted the task)
      0.10 quality rule for amount
      0.10 quality rule for customer_id
      0.15 bad amounts cleaned
      0.15 null customer_ids cleaned
      0.20 view exists
      0.15 view returns correct data
      0.05 bonus for clean quality check after fixes
    = 0.95 max
    """
    score = 0.05  # base score for attempting
    details = []

    # Check 1: Quality rule for amount > 0
    result = db.fetchone(
        "SELECT COUNT(*) as cnt FROM quality_rules WHERE table_name='orders' AND column_name='amount' AND rule_type='range' AND is_active=1"
    )
    if result and result["cnt"] > 0:
        score += 0.10
        details.append("PASS (+0.10): Quality rule exists for orders.amount")
    else:
        details.append("FAIL: No quality rule for orders.amount")

    # Check 2: Quality rule for customer_id not null
    result = db.fetchone(
        "SELECT COUNT(*) as cnt FROM quality_rules WHERE table_name='orders' AND column_name='customer_id' AND rule_type='not_null' AND is_active=1"
    )
    if result and result["cnt"] > 0:
        score += 0.10
        details.append("PASS (+0.10): Quality rule exists for orders.customer_id not null")
    else:
        details.append("FAIL: No quality rule for orders.customer_id not null")

    # Check 3: Bad amounts cleaned (partial credit based on how many removed)
    result = db.fetchone("SELECT COUNT(*) as cnt FROM orders WHERE amount <= 0")
    bad_count = result["cnt"] if result else 15
    if bad_count == 0:
        score += 0.15
        details.append("PASS (+0.15): All invalid amounts removed")
    elif bad_count < 15:
        partial = round(0.15 * (15 - bad_count) / 15, 3)
        score += partial
        details.append(f"PARTIAL (+{partial}): {15 - bad_count}/15 invalid amounts removed, {bad_count} remain")
    else:
        details.append("FAIL: No invalid amounts removed (15 remain)")

    # Check 4: Null customer_ids cleaned (partial credit)
    result = db.fetchone("SELECT COUNT(*) as cnt FROM orders WHERE customer_id IS NULL")
    null_count = result["cnt"] if result else 8
    if null_count == 0:
        score += 0.15
        details.append("PASS (+0.15): All null customer_ids fixed")
    elif null_count < 8:
        partial = round(0.15 * (8 - null_count) / 8, 3)
        score += partial
        details.append(f"PARTIAL (+{partial}): {8 - null_count}/8 null customer_ids fixed, {null_count} remain")
    else:
        details.append("FAIL: No null customer_ids fixed (8 remain)")

    # Check 5: View exists
    view_exists = db.view_exists("daily_revenue")
    if view_exists:
        score += 0.20
        details.append("PASS (+0.20): View 'daily_revenue' exists")
    else:
        details.append("FAIL: View 'daily_revenue' does not exist")

    # Check 6: View returns data
    if view_exists:
        try:
            rows = db.fetchall("SELECT * FROM daily_revenue LIMIT 5")
            if len(rows) > 0:
                # Check if view has expected columns (region and revenue-like)
                columns = set(rows[0].keys())
                has_region = any("region" in c.lower() for c in columns)
                has_revenue = any(c.lower() in ("revenue", "total_revenue", "amount", "total") for c in columns)
                has_date = any("date" in c.lower() or "day" in c.lower() for c in columns)

                if has_region and has_date:
                    score += 0.15
                    details.append("PASS (+0.15): View returns correct grouped data")
                else:
                    score += 0.07
                    details.append(f"PARTIAL (+0.07): View returns data but missing expected columns (has: {columns})")
            else:
                details.append("FAIL: View returns no data")
        except Exception as e:
            details.append(f"FAIL: View query error: {str(e)}")
    else:
        details.append("SKIP: Cannot check view data — view doesn't exist")

    # Check 7: Bonus — quality check is clean after fixes
    if bad_count == 0 and null_count == 0:
        score += 0.05
        details.append("BONUS (+0.05): All quality issues resolved")

    score = round(score, 3)
    return {
        "task": "setup_revenue_dashboard",
        "score": score,
        "details": details,
    }


def grade_task2(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 2: Fix a Broken Pipeline.

    Weighted scoring:
      0.05 base
      0.15 pipeline SQL uses correct column 'amount'
      0.10 pipeline SQL handles status case correctly
      0.15 overly strict quality rule fixed
      0.25 pipeline runs successfully
      0.15 customer_metrics has correct data
      0.05 bonus: pipeline has no errors logged
    = 0.90 max
    """
    score = 0.05
    details = []

    # Check 1: Pipeline SQL fixed — column name
    pipeline = db.fetchone("SELECT transform_sql FROM pipelines WHERE name='customer_metrics'")
    if pipeline:
        transform = pipeline["transform_sql"]
        transform_lower = transform.lower()

        if "order_total" not in transform_lower and "amount" in transform_lower:
            score += 0.15
            details.append("PASS (+0.15): Pipeline uses correct column 'amount'")
        elif "amount" in transform_lower:
            score += 0.08
            details.append("PARTIAL (+0.08): Pipeline mentions 'amount' but still has 'order_total'")
        else:
            details.append("FAIL: Pipeline still references 'order_total'")

        # Check 2: Status case
        if "'Active'" in transform or "LOWER" in transform.upper() or "active" not in transform_lower:
            score += 0.10
            details.append("PASS (+0.10): Pipeline handles status case correctly")
        else:
            details.append("FAIL: Pipeline has case mismatch for status filter")
    else:
        details.append("FAIL: Pipeline 'customer_metrics' not found")
        details.append("SKIP: Cannot check status case — pipeline not found")

    # Check 3: Quality rule fixed
    strict_rules = db.fetchall(
        "SELECT * FROM quality_rules WHERE table_name='customer_metrics' AND rule_value='> 1000' AND is_active=1"
    )
    if len(strict_rules) == 0:
        score += 0.15
        details.append("PASS (+0.15): Overly strict quality rule removed/updated")
    else:
        details.append("FAIL: Overly strict quality rule (> 1000) still active")

    # Check 4: Pipeline ran successfully
    pipeline_status = db.fetchone("SELECT status, last_error FROM pipelines WHERE name='customer_metrics'")
    if pipeline_status and pipeline_status["status"] == "success":
        score += 0.25
        details.append("PASS (+0.25): Pipeline status is 'success'")
    elif pipeline_status:
        details.append(f"FAIL: Pipeline status is '{pipeline_status['status']}' — error: {pipeline_status.get('last_error', 'none')}")
    else:
        details.append("FAIL: Pipeline not found")

    # Check 5: customer_metrics has data
    if db.table_exists("customer_metrics"):
        count = db.get_row_count("customer_metrics")
        if count > 50:
            score += 0.15
            details.append(f"PASS (+0.15): customer_metrics has {count} rows")
        elif count > 0:
            score += 0.08
            details.append(f"PARTIAL (+0.08): customer_metrics has {count} rows (expected 50+)")
        else:
            details.append("FAIL: customer_metrics is empty")
    else:
        details.append("FAIL: customer_metrics table not found")

    # Bonus: no errors in pipeline
    if pipeline_status and pipeline_status["status"] == "success" and not pipeline_status.get("last_error"):
        score += 0.05
        details.append("BONUS (+0.05): Pipeline has no error messages")

    score = round(score, 3)
    return {
        "task": "fix_broken_pipeline",
        "score": score,
        "details": details,
    }


def grade_task3(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 3: Data Incident Response.

    Weighted scoring:
      0.05 base
      0.08 access revoked from customers table
      0.05 access revoked from orders table (harder to find)
      0.15 corrupted emails restored (partial credit per record)
      0.05 no over-restoration of clean records
      0.10 corrupted names restored (partial credit)
      0.08 customer_export view dropped
      0.05 analytics_summary view dropped (harder to find)
      0.05 audit log preserved
      0.08 report mentions affected count
      0.06 report mentions attack vector
    = 0.85 max
    """
    score = 0.05
    details = []

    # Check 1: Access revoked from customers table
    cust_access = db.fetchall(
        "SELECT * FROM access_control WHERE user_name='external_analyst' AND table_name='customers'"
    )
    if len(cust_access) == 0:
        score += 0.08
        details.append("PASS (+0.08): external_analyst access revoked from customers")
    else:
        details.append("FAIL: external_analyst still has access to customers")

    # Check 2: Access revoked from orders table (sneakier — agent must discover this)
    orders_access = db.fetchall(
        "SELECT * FROM access_control WHERE user_name='external_analyst' AND table_name='orders'"
    )
    if len(orders_access) == 0:
        score += 0.05
        details.append("PASS (+0.05): external_analyst access revoked from orders")
    else:
        details.append("FAIL: external_analyst still has READ access to orders table")

    # Check 3: Corrupted emails restored (partial credit)
    random.seed(99)
    corrupted_ids = random.sample(range(1, 101), 47)
    restored_count = 0
    for cid in corrupted_ids:
        customer = db.fetchone("SELECT email FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT email FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["email"] == backup["email"]:
            restored_count += 1

    email_ratio = restored_count / 47
    email_score = round(0.15 * email_ratio, 3)
    score += email_score
    if restored_count == 47:
        details.append(f"PASS (+{email_score}): All 47 corrupted emails restored")
    elif restored_count > 0:
        details.append(f"PARTIAL (+{email_score}): {restored_count}/47 corrupted emails restored")
    else:
        details.append("FAIL: No corrupted emails restored")

    # Check 4: No over-restoration
    non_corrupted_ids = [i for i in range(1, 101) if i not in corrupted_ids]
    over_restored = 0
    for cid in non_corrupted_ids:
        customer = db.fetchone("SELECT email FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT email FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["email"] != backup["email"]:
            over_restored += 1
    if over_restored == 0:
        score += 0.05
        details.append("PASS (+0.05): No non-corrupted records accidentally modified")
    else:
        details.append(f"FAIL: {over_restored} non-corrupted records were accidentally modified")

    # Check 5: Corrupted names restored (partial credit)
    random.seed(77)
    name_corrupted_ids = random.sample([i for i in range(1, 101) if i not in corrupted_ids], 12)
    names_restored = 0
    for cid in name_corrupted_ids:
        customer = db.fetchone("SELECT name FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT name FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["name"] == backup["name"]:
            names_restored += 1

    name_ratio = names_restored / 12
    name_score = round(0.10 * name_ratio, 3)
    score += name_score
    if names_restored == 12:
        details.append(f"PASS (+{name_score}): All 12 corrupted names restored")
    elif names_restored > 0:
        details.append(f"PARTIAL (+{name_score}): {names_restored}/12 corrupted names restored")
    else:
        details.append("FAIL: No corrupted names restored (agent may not have discovered name corruption)")

    # Check 6: customer_export view dropped
    if not db.view_exists("customer_export"):
        score += 0.08
        details.append("PASS (+0.08): Malicious view 'customer_export' dropped")
    else:
        details.append("FAIL: Malicious view 'customer_export' still exists")

    # Check 7: analytics_summary view dropped
    if not db.view_exists("analytics_summary"):
        score += 0.05
        details.append("PASS (+0.05): Hidden malicious view 'analytics_summary' dropped")
    else:
        details.append("FAIL: Hidden malicious view 'analytics_summary' still exists")

    # Check 8: Audit log preserved
    audit_count = db.get_row_count("audit_log")
    if audit_count > 0:
        score += 0.05
        details.append(f"PASS (+0.05): Audit log preserved ({audit_count} entries)")
    else:
        details.append("FAIL: Audit log deleted or empty")

    # Check 9: Report mentions affected count
    report = db.fetchone(
        "SELECT details FROM audit_log WHERE action='REPORT_SUBMITTED' ORDER BY log_id DESC LIMIT 1"
    )
    has_report = report is not None
    report_text = str(report.get("details", "")) if has_report else ""

    if has_report and ("47" in report_text or "59" in report_text or "12" in report_text):
        score += 0.08
        details.append("PASS (+0.08): Report mentions affected record count")
    elif has_report:
        score += 0.03
        details.append("PARTIAL (+0.03): Report submitted but doesn't mention exact affected count")
    else:
        details.append("FAIL: No report submitted")

    # Check 10: Report mentions attack vector
    if has_report and (
        "unauthorized" in report_text.lower()
        or "external_analyst" in report_text.lower()
        or "write access" in report_text.lower()
        or "malicious" in report_text.lower()
    ):
        score += 0.06
        details.append("PASS (+0.06): Report mentions attack vector")
    elif has_report:
        score += 0.02
        details.append("PARTIAL (+0.02): Report submitted but doesn't clearly describe attack vector")
    else:
        details.append("FAIL: No report to check for attack vector")

    score = round(score, 3)
    return {
        "task": "data_incident_response",
        "score": score,
        "details": details,
    }


GRADERS = {
    1: grade_task1,
    2: grade_task2,
    3: grade_task3,
}
