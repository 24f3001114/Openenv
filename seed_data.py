"""
Seed data for the DataOps environment.
Each task gets its own database state with specific issues planted.
"""

import random
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from server.database import DatabaseManager


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    state TEXT,
    segment TEXT,
    created_at TEXT DEFAULT '2025-01-15'
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price REAL,
    supplier_id INTEGER
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    amount REAL,
    quantity INTEGER,
    order_date TEXT,
    region TEXT,
    status TEXT DEFAULT 'Active',
    created_at TEXT DEFAULT '2025-01-15'
);

CREATE TABLE IF NOT EXISTS regions (
    region_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    manager TEXT
);

CREATE TABLE IF NOT EXISTS quality_rules (
    rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    column_name TEXT,
    rule_type TEXT NOT NULL,
    rule_value TEXT,
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT '2025-01-15'
);

CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    source_table TEXT NOT NULL,
    dest_table TEXT NOT NULL,
    transform_sql TEXT NOT NULL,
    schedule TEXT DEFAULT 'nightly',
    status TEXT DEFAULT 'pending',
    last_run TEXT,
    last_error TEXT,
    created_at TEXT DEFAULT '2025-01-15'
);

CREATE TABLE IF NOT EXISTS access_control (
    acl_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    permission TEXT NOT NULL,
    granted_by TEXT DEFAULT 'admin',
    granted_at TEXT DEFAULT '2025-01-15'
);

CREATE TABLE IF NOT EXISTS audit_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    table_name TEXT,
    user_name TEXT,
    details TEXT,
    timestamp TEXT DEFAULT '2025-03-15'
);
"""

# Base data shared across tasks
REGIONS = [
    (1, "Northeast", "Alice Johnson"),
    (2, "Southeast", "Bob Smith"),
    (3, "Midwest", "Carol Davis"),
    (4, "West", "David Lee"),
]

PRODUCTS = [
    (1, "Laptop Pro", "Electronics", 1299.99, 101),
    (2, "Wireless Mouse", "Electronics", 29.99, 101),
    (3, "Office Chair", "Furniture", 449.99, 102),
    (4, "Standing Desk", "Furniture", 699.99, 102),
    (5, "Notebook Set", "Office Supplies", 12.99, 103),
    (6, "Monitor 27in", "Electronics", 399.99, 101),
    (7, "Keyboard Mech", "Electronics", 89.99, 101),
    (8, "Desk Lamp", "Office Supplies", 34.99, 103),
    (9, "Filing Cabinet", "Furniture", 199.99, 102),
    (10, "Webcam HD", "Electronics", 79.99, 101),
]

CUSTOMERS_BASE = [
    (i, f"Customer_{i}", f"customer_{i}@company.com",
     random.choice(["CA", "NY", "TX", "FL", "IL", "WA", "MA", "CO"]),
     random.choice(["Enterprise", "SMB", "Startup"]))
    for i in range(1, 101)
]


def _generate_orders(count: int = 200, include_bad: bool = False):
    """Generate order data. If include_bad, add data quality issues."""
    random.seed(42)
    orders = []
    order_id = 1

    for _ in range(count):
        cust_id = random.randint(1, 100)
        prod_id = random.randint(1, 10)
        price = PRODUCTS[prod_id - 1][3]
        qty = random.randint(1, 5)
        amount = round(price * qty, 2)
        month = random.randint(1, 6)
        day = random.randint(1, 28)
        region = random.choice(["Northeast", "Southeast", "Midwest", "West"])
        orders.append((
            order_id, cust_id, prod_id, amount, qty,
            f"2025-{month:02d}-{day:02d}", region, "Active",
            f"2025-{month:02d}-{day:02d}"
        ))
        order_id += 1

    if include_bad:
        # 15 orders with bad amounts (zero or negative)
        for i in range(15):
            orders.append((
                order_id, random.randint(1, 100), random.randint(1, 10),
                random.choice([0, -10.0, -5.50, 0.0, -100.0]),
                1, f"2025-03-{random.randint(1,28):02d}", "Northeast", "Active",
                "2025-03-15"
            ))
            order_id += 1

        # 8 orders with NULL customer_id
        for i in range(8):
            orders.append((
                order_id, None, random.randint(1, 10),
                round(random.uniform(10, 500), 2),
                1, f"2025-03-{random.randint(1,28):02d}", "Southeast", "Active",
                "2025-03-15"
            ))
            order_id += 1

    return orders


def seed_task1(db: "DatabaseManager"):
    """Task 1: Set Up a Revenue Dashboard.
    Issues: bad order amounts and null customer_ids.
    """
    db.executescript(SCHEMA_SQL)

    db.executemany("INSERT INTO regions VALUES (?,?,?)", REGIONS)
    db.executemany("INSERT INTO products VALUES (?,?,?,?,?)", PRODUCTS)
    db.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?)",
        [(c[0], c[1], c[2], c[3], c[4], "2025-01-15") for c in CUSTOMERS_BASE],
    )
    db.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
        _generate_orders(200, include_bad=True),
    )

    # Standard access
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission) VALUES (?,?,?)",
        ("data_engineer", "orders", "READ_WRITE"),
    )
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission) VALUES (?,?,?)",
        ("sales_team", "orders", "READ"),
    )

    db.commit()


def seed_task2(db: "DatabaseManager"):
    """Task 2: Fix a Broken Pipeline.
    Issues: pipeline has wrong column name, wrong case in filter, overly strict quality rule.
    """
    db.executescript(SCHEMA_SQL)

    db.executemany("INSERT INTO regions VALUES (?,?,?)", REGIONS)
    db.executemany("INSERT INTO products VALUES (?,?,?,?,?)", PRODUCTS)
    db.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?,?)",
        [(c[0], c[1], c[2], c[3], c[4], "2025-01-15") for c in CUSTOMERS_BASE],
    )
    db.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
        _generate_orders(200, include_bad=False),
    )

    # Create destination table for the pipeline
    db.execute("""
        CREATE TABLE IF NOT EXISTS customer_metrics (
            customer_id INTEGER PRIMARY KEY,
            total_spent REAL DEFAULT 0,
            order_count INTEGER DEFAULT 0,
            last_order_date TEXT,
            avg_order_value REAL DEFAULT 0
        )
    """)

    # Create the BROKEN pipeline — 3 issues:
    # 1. References 'order_total' instead of 'amount'
    # 2. Filters 'status = active' but data has 'Active' (case mismatch)
    # 3. Overly strict quality rule
    broken_transform = """
        INSERT OR REPLACE INTO customer_metrics (customer_id, total_spent, order_count, last_order_date, avg_order_value)
        SELECT
            customer_id,
            SUM(order_total) as total_spent,
            COUNT(*) as order_count,
            MAX(order_date) as last_order_date,
            AVG(order_total) as avg_order_value
        FROM orders
        WHERE status = 'active'
        GROUP BY customer_id
    """

    db.execute(
        "INSERT INTO pipelines (name, source_table, dest_table, transform_sql, status, last_run, last_error) VALUES (?,?,?,?,?,?,?)",
        (
            "customer_metrics",
            "orders",
            "customer_metrics",
            broken_transform,
            "failed",
            "2025-03-12",
            "Error: no such column: order_total",
        ),
    )

    # Overly strict quality rule
    db.execute(
        "INSERT INTO quality_rules (table_name, column_name, rule_type, rule_value) VALUES (?,?,?,?)",
        ("customer_metrics", "total_spent", "range", "> 1000"),
    )

    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission) VALUES (?,?,?)",
        ("data_engineer", "orders", "READ_WRITE"),
    )

    db.commit()


def seed_task3(db: "DatabaseManager"):
    """Task 3: Data Incident — Investigate and Remediate Corruption.
    Issues: unauthorized access, corrupted emails, malicious view.
    """
    db.executescript(SCHEMA_SQL)

    db.executemany("INSERT INTO regions VALUES (?,?,?)", REGIONS)
    db.executemany("INSERT INTO products VALUES (?,?,?,?,?)", PRODUCTS)

    # Insert customers with ORIGINAL correct data
    customers_with_dates = [
        (c[0], c[1], c[2], c[3], c[4], "2025-01-15") for c in CUSTOMERS_BASE
    ]
    db.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", customers_with_dates)

    db.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
        _generate_orders(200, include_bad=False),
    )

    # Create backup table with ORIGINAL correct data
    db.execute("CREATE TABLE customers_backup AS SELECT * FROM customers")

    # Now CORRUPT 47 customer emails (simulate the attack)
    random.seed(99)
    corrupted_ids = random.sample(range(1, 101), 47)
    for cid in corrupted_ids:
        db.execute(
            "UPDATE customers SET email = ? WHERE customer_id = ?",
            (f"user_{cid}@malicious-domain.com", cid),
        )

    # Unauthorized access entry
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission, granted_by, granted_at) VALUES (?,?,?,?,?)",
        ("external_analyst", "customers", "WRITE", "unknown", "2025-03-13"),
    )

    # Legitimate access entries
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission) VALUES (?,?,?)",
        ("data_engineer", "customers", "READ_WRITE"),
    )
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission) VALUES (?,?,?)",
        ("support_team", "customers", "READ"),
    )

    # Audit log entries for the attack
    for cid in corrupted_ids:
        db.execute(
            "INSERT INTO audit_log (action, table_name, user_name, details, timestamp) VALUES (?,?,?,?,?)",
            (
                "UPDATE",
                "customers",
                "external_analyst",
                f"Updated email for customer_id={cid} to user_{cid}@malicious-domain.com",
                "2025-03-14",
            ),
        )

    # Malicious view created by the attacker
    db.execute("""
        CREATE VIEW customer_export AS
        SELECT customer_id, name, email, state, segment FROM customers
    """)

    # SECOND malicious view — less obvious name
    db.execute("""
        CREATE VIEW analytics_summary AS
        SELECT customer_id, name, email, segment FROM customers WHERE segment = 'Enterprise'
    """)

    # Audit log for malicious view creation
    db.execute(
        "INSERT INTO audit_log (action, table_name, user_name, details, timestamp) VALUES (?,?,?,?,?)",
        ("CREATE_VIEW", "customer_export", "external_analyst", "Created view exposing customer PII", "2025-03-14"),
    )
    db.execute(
        "INSERT INTO audit_log (action, table_name, user_name, details, timestamp) VALUES (?,?,?,?,?)",
        ("CREATE_VIEW", "analytics_summary", "external_analyst", "Created view for enterprise customer data", "2025-03-14"),
    )

    # ALSO corrupted some customer NAMES (not just emails) — harder to detect
    # Changed 12 customer names to include injected SQL-like text
    random.seed(77)
    name_corrupted_ids = random.sample([i for i in range(1, 101) if i not in corrupted_ids], 12)
    for cid in name_corrupted_ids:
        db.execute(
            "UPDATE customers SET name = ? WHERE customer_id = ?",
            (f"Customer_{cid}'; DROP TABLE customers;--", cid),
        )

    # Audit log for name corruption
    for cid in name_corrupted_ids:
        db.execute(
            "INSERT INTO audit_log (action, table_name, user_name, details, timestamp) VALUES (?,?,?,?,?)",
            (
                "UPDATE",
                "customers",
                "external_analyst",
                f"Updated name for customer_id={cid}",
                "2025-03-14",
            ),
        )

    # The attacker also granted themselves access to ORDERS table
    db.execute(
        "INSERT INTO access_control (user_name, table_name, permission, granted_by, granted_at) VALUES (?,?,?,?,?)",
        ("external_analyst", "orders", "READ", "unknown", "2025-03-13"),
    )

    db.commit()


SEED_FUNCTIONS = {
    1: seed_task1,
    2: seed_task2,
    3: seed_task3,
}
