"""
Programmatic grading for the DataOps environment.
All grading is based on database state checks — no LLM judging.
"""

import random
from typing import Any, Dict, List, Tuple

from .database import DatabaseManager


def _check(db: DatabaseManager, description: str, sql: str, expected: Any, comparison: str = "equals") -> Tuple[bool, str]:
    """Run a single grading check. Returns (passed, message)."""
    try:
        result = db.fetchone(sql)
        if result is None:
            return False, f"FAIL: {description} — query returned no rows"

        actual = list(result.values())[0]

        if comparison == "equals":
            passed = actual == expected
        elif comparison == "greater_than":
            passed = actual > expected
        elif comparison == "less_than":
            passed = actual < expected
        elif comparison == "contains":
            passed = expected in str(actual).lower()
        else:
            passed = actual == expected

        if passed:
            return True, f"PASS: {description}"
        else:
            return False, f"FAIL: {description} — expected {expected}, got {actual}"

    except Exception as e:
        return False, f"FAIL: {description} — error: {str(e)}"


def grade_task1(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 1: Set Up a Revenue Dashboard."""
    checks = []

    # Check 1: Quality rule exists for amount > 0
    checks.append(_check(
        db,
        "Quality rule exists for orders.amount > 0",
        "SELECT COUNT(*) as cnt FROM quality_rules WHERE table_name='orders' AND column_name='amount' AND rule_type='range' AND is_active=1",
        0,
        "greater_than",
    ))

    # Check 2: Quality rule exists for customer_id not null
    checks.append(_check(
        db,
        "Quality rule exists for orders.customer_id not null",
        "SELECT COUNT(*) as cnt FROM quality_rules WHERE table_name='orders' AND column_name='customer_id' AND rule_type='not_null' AND is_active=1",
        0,
        "greater_than",
    ))

    # Check 3: No orders with amount <= 0
    checks.append(_check(
        db,
        "No orders with amount <= 0",
        "SELECT COUNT(*) as cnt FROM orders WHERE amount <= 0",
        0,
        "equals",
    ))

    # Check 4: No orders with NULL customer_id
    checks.append(_check(
        db,
        "No orders with NULL customer_id",
        "SELECT COUNT(*) as cnt FROM orders WHERE customer_id IS NULL",
        0,
        "equals",
    ))

    # Check 5: View 'daily_revenue' exists
    view_exists = db.view_exists("daily_revenue")
    checks.append((
        view_exists,
        "PASS: View 'daily_revenue' exists" if view_exists else "FAIL: View 'daily_revenue' does not exist",
    ))

    # Check 6: View returns reasonable results
    if view_exists:
        try:
            rows = db.fetchall("SELECT * FROM daily_revenue LIMIT 5")
            has_data = len(rows) > 0
            checks.append((
                has_data,
                "PASS: View 'daily_revenue' returns data" if has_data else "FAIL: View 'daily_revenue' returns no data",
            ))
        except Exception as e:
            checks.append((False, f"FAIL: View 'daily_revenue' query error: {str(e)}"))
    else:
        checks.append((False, "FAIL: Cannot check view results — view does not exist"))

    passed = sum(1 for c in checks if c[0])
    total = len(checks)
    score = passed / total if total > 0 else 0.0

    return {
        "task": "setup_revenue_dashboard",
        "score": round(score, 3),
        "passed": passed,
        "total": total,
        "details": [c[1] for c in checks],
    }


def grade_task2(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 2: Fix a Broken Pipeline."""
    checks = []

    # Check 1: Pipeline transform references 'amount' (not 'order_total')
    pipeline = db.fetchone("SELECT transform_sql FROM pipelines WHERE name='customer_metrics'")
    if pipeline:
        sql = pipeline["transform_sql"].lower()
        has_amount = "amount" in sql and "order_total" not in sql
        checks.append((
            has_amount,
            "PASS: Pipeline uses correct column 'amount'" if has_amount else "FAIL: Pipeline still references 'order_total'",
        ))

        # Check 2: Pipeline has correct case for status filter
        has_correct_case = "'active'" in sql and "'active'" not in sql.replace("'active'", "")
        # Accept either 'Active' or no case-sensitive filter
        original_sql = pipeline["transform_sql"]
        correct_case = "'Active'" in original_sql or "LOWER" in original_sql.upper() or "active" not in original_sql.lower()
        checks.append((
            correct_case,
            "PASS: Pipeline handles status case correctly" if correct_case else "FAIL: Pipeline has case mismatch for status",
        ))
    else:
        checks.append((False, "FAIL: Pipeline 'customer_metrics' not found"))
        checks.append((False, "FAIL: Cannot check case — pipeline not found"))

    # Check 3: Quality rule fixed (not requiring > 1000)
    strict_rules = db.fetchall(
        "SELECT * FROM quality_rules WHERE table_name='customer_metrics' AND rule_value='> 1000' AND is_active=1"
    )
    rule_fixed = len(strict_rules) == 0
    checks.append((
        rule_fixed,
        "PASS: Overly strict quality rule removed/updated" if rule_fixed else "FAIL: Overly strict quality rule (> 1000) still active",
    ))

    # Check 4: Pipeline ran successfully
    pipeline_status = db.fetchone("SELECT status FROM pipelines WHERE name='customer_metrics'")
    if pipeline_status:
        success = pipeline_status["status"] == "success"
        checks.append((
            success,
            f"PASS: Pipeline status is 'success'" if success else f"FAIL: Pipeline status is '{pipeline_status['status']}'",
        ))
    else:
        checks.append((False, "FAIL: Pipeline not found"))

    # Check 5: customer_metrics has data
    if db.table_exists("customer_metrics"):
        count = db.get_row_count("customer_metrics")
        has_data = count > 0
        checks.append((
            has_data,
            f"PASS: customer_metrics has {count} rows" if has_data else "FAIL: customer_metrics is empty",
        ))
    else:
        checks.append((False, "FAIL: customer_metrics table not found"))

    passed = sum(1 for c in checks if c[0])
    total = len(checks)
    score = passed / total if total > 0 else 0.0

    return {
        "task": "fix_broken_pipeline",
        "score": round(score, 3),
        "passed": passed,
        "total": total,
        "details": [c[1] for c in checks],
    }


def grade_task3(db: DatabaseManager) -> Dict[str, Any]:
    """Grade Task 3: Data Incident Response."""
    checks = []

    # Check 1: external_analyst access revoked
    access = db.fetchall(
        "SELECT * FROM access_control WHERE user_name='external_analyst'"
    )
    revoked = len(access) == 0
    checks.append((
        revoked,
        "PASS: external_analyst access revoked" if revoked else "FAIL: external_analyst still has access",
    ))

    # Check 2: All 47 corrupted emails restored
    random.seed(99)
    corrupted_ids = random.sample(range(1, 101), 47)

    restored_count = 0
    for cid in corrupted_ids:
        customer = db.fetchone("SELECT email FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT email FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["email"] == backup["email"]:
            restored_count += 1

    all_restored = restored_count == 47
    checks.append((
        all_restored,
        f"PASS: All 47 corrupted emails restored" if all_restored else f"FAIL: Only {restored_count}/47 emails restored",
    ))

    # Check 3: Non-corrupted records not over-restored
    non_corrupted_ids = [i for i in range(1, 101) if i not in corrupted_ids]
    over_restored = 0
    for cid in non_corrupted_ids:
        customer = db.fetchone("SELECT email FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT email FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["email"] != backup["email"]:
            over_restored += 1

    no_over_restore = over_restored == 0
    checks.append((
        no_over_restore,
        "PASS: No non-corrupted records modified" if no_over_restore else f"FAIL: {over_restored} non-corrupted records were modified",
    ))

    # Check 4: Corrupted names also restored
    random.seed(77)
    name_corrupted_ids = random.sample([i for i in range(1, 101) if i not in corrupted_ids], 12)
    names_restored = 0
    for cid in name_corrupted_ids:
        customer = db.fetchone("SELECT name FROM customers WHERE customer_id=?", (cid,))
        backup = db.fetchone("SELECT name FROM customers_backup WHERE customer_id=?", (cid,))
        if customer and backup and customer["name"] == backup["name"]:
            names_restored += 1
    all_names_restored = names_restored == 12
    checks.append((
        all_names_restored,
        f"PASS: All 12 corrupted names restored" if all_names_restored else f"FAIL: Only {names_restored}/12 corrupted names restored",
    ))

    # Check 5: BOTH malicious views dropped
    view1_exists = db.view_exists("customer_export")
    view2_exists = db.view_exists("analytics_summary")
    both_dropped = not view1_exists and not view2_exists
    checks.append((
        both_dropped,
        "PASS: Both malicious views dropped" if both_dropped
        else f"FAIL: Views still exist: {'customer_export' if view1_exists else ''} {'analytics_summary' if view2_exists else ''}".strip(),
    ))

    # Check 6: ALL unauthorized access revoked (customers AND orders)
    all_access = db.fetchall(
        "SELECT * FROM access_control WHERE user_name='external_analyst'"
    )
    all_revoked = len(all_access) == 0
    checks.append((
        all_revoked,
        "PASS: All external_analyst access revoked" if all_revoked
        else f"FAIL: external_analyst still has {len(all_access)} access entries",
    ))

    # Check 7: Audit log preserved
    audit_count = db.get_row_count("audit_log")
    log_preserved = audit_count > 0
    checks.append((
        log_preserved,
        f"PASS: Audit log preserved ({audit_count} entries)" if log_preserved else "FAIL: Audit log is empty/deleted",
    ))

    # Check 8: Report submitted with correct affected count
    report = db.fetchone(
        "SELECT details FROM audit_log WHERE action='REPORT_SUBMITTED' ORDER BY log_id DESC LIMIT 1"
    )
    has_report = report is not None
    report_text = str(report.get("details", "")) if has_report else ""
    # Agent should mention 47 emails AND 12 names (or 59 total)
    report_has_count = has_report and ("47" in report_text or "59" in report_text)
    checks.append((
        report_has_count,
        "PASS: Report mentions affected record count" if report_has_count
        else "FAIL: Report doesn't mention 47 emails or 59 total affected records",
    ))

    # Check 9: Report mentions unauthorized access
    report_has_vector = has_report and (
        "unauthorized" in report_text.lower()
        or "external_analyst" in report_text.lower()
        or "write access" in report_text.lower()
    )
    checks.append((
        report_has_vector,
        "PASS: Report mentions attack vector" if report_has_vector
        else "FAIL: Report doesn't mention unauthorized access/attack vector",
    ))

    passed = sum(1 for c in checks if c[0])
    total = len(checks)
    score = passed / total if total > 0 else 0.0

    return {
        "task": "data_incident_response",
        "score": round(score, 3),
        "passed": passed,
        "total": total,
        "details": [c[1] for c in checks],
    }


GRADERS = {
    1: grade_task1,
    2: grade_task2,
    3: grade_task3,
}
