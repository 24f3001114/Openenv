"""
SQLite database manager for the DataOps environment.
Each reset() creates a fresh in-memory database seeded with task-specific data.
"""

import sqlite3
from typing import Any, Dict, List, Optional, Tuple


class DatabaseManager:
    """Manages an in-memory SQLite database for the DataOps environment."""

    def __init__(self):
        self.conn: Optional[sqlite3.Connection] = None

    def create(self):
        """Create a fresh in-memory database."""
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, params_list: list) -> sqlite3.Cursor:
        return self.conn.executemany(sql, params_list)

    def executescript(self, sql: str):
        self.conn.executescript(sql)

    def commit(self):
        self.conn.commit()

    def fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cursor = self.conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        cursor = self.conn.execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))

    def table_exists(self, table_name: str) -> bool:
        result = self.fetchone(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return result is not None

    def view_exists(self, view_name: str) -> bool:
        result = self.fetchone(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            (view_name,),
        )
        return result is not None

    def get_tables(self) -> List[str]:
        rows = self.fetchall(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [r["name"] for r in rows]

    def get_views(self) -> List[Dict[str, str]]:
        return self.fetchall(
            "SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name"
        )

    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        return self.fetchall(f"PRAGMA table_info({table_name})")

    def get_row_count(self, table_name: str) -> int:
        result = self.fetchone(f"SELECT COUNT(*) as cnt FROM [{table_name}]")
        return result["cnt"] if result else 0

    def get_sample_rows(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        return self.fetchall(f"SELECT * FROM [{table_name}] LIMIT ?", (limit,))
