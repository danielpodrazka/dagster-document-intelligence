"""
DuckDB-backed data store for cross-partner K-1 validation.

Provides persistent storage of extracted K-1 records, cross-partner
validation results, and processed file tracking for duplicate detection.
Follows the S3Storage resource pattern in resources.py.

DuckDB does not support concurrent writers from multiple processes. Since
Dagster's DefaultRunLauncher spawns each run as a separate subprocess, a
file lock serializes all DuckDB access across processes.
"""

from __future__ import annotations

import fcntl
import os
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import ConfigurableResource
from pydantic import PrivateAttr

_DUCKDB_LOCK_PATH = Path(tempfile.gettempdir()) / "k1_duckdb.lock"


class DuckDBStore(ConfigurableResource):
    """DuckDB-backed storage for cross-partner validation data."""

    db_path: str = os.environ.get("DUCKDB_PATH", "data/k1_validation.duckdb")

    _conn: Any = PrivateAttr(default=None)
    _tables_created: bool = PrivateAttr(default=False)

    @contextmanager
    def _locked_conn(self):
        """Acquire a file lock and yield a DuckDB connection.

        Opens a fresh connection each time to avoid cross-process locking
        issues with DuckDB's single-writer model.
        """
        import duckdb

        fd = open(_DUCKDB_LOCK_PATH, "w")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            conn = duckdb.connect(self.db_path)
            try:
                if not self._tables_created:
                    self._create_tables_on(conn)
                    self._tables_created = True
                yield conn
            finally:
                conn.close()
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()

    def _get_conn(self):
        """Lazy-init DuckDB connection; creates tables on first access.

        Used for read-only operations where locking is not critical.
        """
        if self._conn is None:
            import duckdb

            self._conn = duckdb.connect(self.db_path)
            if not self._tables_created:
                self._create_tables_on(self._conn)
                self._tables_created = True
        return self._conn

    def _create_tables(self):
        """Create tables if they don't exist (legacy helper)."""
        self._create_tables_on(self._conn)

    def _create_tables_on(self, conn):
        """Create tables if they don't exist."""

        conn.execute("""
            CREATE TABLE IF NOT EXISTS k1_records (
                partnership_ein  VARCHAR NOT NULL,
                partner_tin      VARCHAR NOT NULL,
                tax_year         VARCHAR NOT NULL,
                run_id           VARCHAR NOT NULL,
                source_pdf_name  VARCHAR,
                pdf_sha256       VARCHAR,
                ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                partnership_name VARCHAR,
                partner_type     VARCHAR,
                partner_share_percentage DOUBLE,
                ordinary_business_income DOUBLE,
                rental_real_estate_income DOUBLE,
                guaranteed_payments      DOUBLE,
                interest_income          DOUBLE,
                ordinary_dividends       DOUBLE,
                qualified_dividends      DOUBLE,
                short_term_capital_gains  DOUBLE,
                long_term_capital_gains   DOUBLE,
                section_179_deduction    DOUBLE,
                distributions            DOUBLE,
                capital_account_beginning DOUBLE,
                capital_account_ending    DOUBLE,
                self_employment_earnings  DOUBLE,
                foreign_taxes_paid       DOUBLE,
                qbi_deduction            DOUBLE,
                deterministic_passed        BOOLEAN,
                deterministic_critical_count INTEGER DEFAULT 0,
                deterministic_warning_count  INTEGER DEFAULT 0,
                ai_coherence_score          DOUBLE,
                ai_ocr_confidence           DOUBLE,
                PRIMARY KEY (partnership_ein, partner_tin, tax_year)
            )
        """)

        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS cross_partner_validations_seq START 1
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_partner_validations (
                id               INTEGER DEFAULT nextval('cross_partner_validations_seq') PRIMARY KEY,
                rule_id          VARCHAR NOT NULL,
                category         VARCHAR NOT NULL,
                severity         VARCHAR NOT NULL,
                passed           BOOLEAN NOT NULL,
                message          TEXT,
                partnership_ein  VARCHAR,
                tax_year         VARCHAR,
                partner_tin      VARCHAR,
                validated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_id_trigger   VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                pdf_sha256    VARCHAR PRIMARY KEY,
                run_id        VARCHAR NOT NULL,
                file_name     VARCHAR,
                file_size     BIGINT,
                ingested_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    # -- K-1 record operations ------------------------------------------------

    def upsert_k1_record(self, record: dict) -> None:
        """Insert or update a K-1 record (upsert on primary key)."""
        columns = [
            "partnership_ein", "partner_tin", "tax_year", "run_id",
            "source_pdf_name", "pdf_sha256", "ingested_at", "partnership_name",
            "partner_type", "partner_share_percentage",
            "ordinary_business_income", "rental_real_estate_income",
            "guaranteed_payments", "interest_income", "ordinary_dividends",
            "qualified_dividends", "short_term_capital_gains",
            "long_term_capital_gains", "section_179_deduction", "distributions",
            "capital_account_beginning", "capital_account_ending",
            "self_employment_earnings", "foreign_taxes_paid", "qbi_deduction",
            "deterministic_passed", "deterministic_critical_count",
            "deterministic_warning_count", "ai_coherence_score",
            "ai_ocr_confidence",
        ]

        values = [record.get(col) for col in columns]
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)

        with self._locked_conn() as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO k1_records ({col_names}) VALUES ({placeholders})",
                values,
            )

    # -- Query helpers ---------------------------------------------------------

    def get_partnership_k1s(self, ein: str, year: str) -> list[dict]:
        """Get all K-1 records for a partnership in a given tax year."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM k1_records WHERE partnership_ein = ? AND tax_year = ?",
            [ein, year],
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_partner_history(self, tin: str, ein: str) -> list[dict]:
        """Get all K-1 records for a specific partner at a specific partnership."""
        conn = self._get_conn()
        result = conn.execute(
            "SELECT * FROM k1_records WHERE partner_tin = ? AND partnership_ein = ? ORDER BY tax_year",
            [tin, ein],
        )
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_consecutive_year_pairs(self) -> list[tuple[dict, dict]]:
        """Get pairs of K-1 records for the same partner-partnership across consecutive years."""
        conn = self._get_conn()
        result = conn.execute("""
            SELECT
                a.partnership_ein, a.partner_tin, a.tax_year AS year_prior,
                b.tax_year AS year_current
            FROM k1_records a
            JOIN k1_records b
                ON a.partnership_ein = b.partnership_ein
                AND a.partner_tin = b.partner_tin
                AND CAST(a.tax_year AS INTEGER) + 1 = CAST(b.tax_year AS INTEGER)
            ORDER BY a.partnership_ein, a.partner_tin, a.tax_year
        """)

        pairs = []
        for row in result.fetchall():
            ein, tin, year_prior, year_current = row
            prior_records = self.get_partnership_k1s(ein, year_prior)
            current_records = self.get_partnership_k1s(ein, year_current)
            prior = next((r for r in prior_records if r["partner_tin"] == tin), None)
            current = next((r for r in current_records if r["partner_tin"] == tin), None)
            if prior and current:
                pairs.append((prior, current))
        return pairs

    def get_all_partnerships(self) -> list[dict]:
        """Get all unique (partnership_ein, tax_year) groups with their partner count."""
        conn = self._get_conn()
        result = conn.execute("""
            SELECT partnership_ein, tax_year, COUNT(*) as partner_count,
                   MAX(partnership_name) as partnership_name
            FROM k1_records
            GROUP BY partnership_ein, tax_year
            ORDER BY partnership_ein, tax_year
        """)
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_all_k1_records(self) -> list[dict]:
        """Get all K-1 records."""
        conn = self._get_conn()
        result = conn.execute("SELECT * FROM k1_records ORDER BY partnership_ein, tax_year, partner_tin")
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    # -- Validation result operations ------------------------------------------

    def clear_validations_for_scope(
        self, partnership_ein: str | None = None, tax_year: str | None = None
    ) -> int:
        """Clear existing validation results for a given scope. Returns count deleted."""
        with self._locked_conn() as conn:
            if partnership_ein and tax_year:
                result = conn.execute(
                    "DELETE FROM cross_partner_validations WHERE partnership_ein = ? AND tax_year = ?",
                    [partnership_ein, tax_year],
                )
            elif partnership_ein:
                result = conn.execute(
                    "DELETE FROM cross_partner_validations WHERE partnership_ein = ?",
                    [partnership_ein],
                )
            else:
                result = conn.execute("DELETE FROM cross_partner_validations")
            return result.fetchone()[0] if result.description else 0

    def insert_validation_result(self, result: dict) -> None:
        """Insert a single cross-partner validation result."""
        with self._locked_conn() as conn:
            conn.execute(
                """INSERT INTO cross_partner_validations
                   (rule_id, category, severity, passed, message,
                    partnership_ein, tax_year, partner_tin, validated_at, run_id_trigger)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    result["rule_id"],
                    result["category"],
                    result["severity"],
                    result["passed"],
                    result.get("message", ""),
                    result.get("partnership_ein"),
                    result.get("tax_year"),
                    result.get("partner_tin"),
                    result.get("validated_at", datetime.now(timezone.utc).isoformat()),
                    result.get("run_id_trigger"),
                ],
            )

    def insert_validation_results(self, results: list[dict]) -> None:
        """Insert multiple validation results."""
        with self._locked_conn() as conn:
            for r in results:
                conn.execute(
                    """INSERT INTO cross_partner_validations
                       (rule_id, category, severity, passed, message,
                        partnership_ein, tax_year, partner_tin, validated_at, run_id_trigger)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        r["rule_id"],
                        r["category"],
                        r["severity"],
                        r["passed"],
                        r.get("message", ""),
                        r.get("partnership_ein"),
                        r.get("tax_year"),
                        r.get("partner_tin"),
                        r.get("validated_at", datetime.now(timezone.utc).isoformat()),
                        r.get("run_id_trigger"),
                    ],
                )

    def get_validations_for_scope(
        self, partnership_ein: str | None = None, tax_year: str | None = None
    ) -> list[dict]:
        """Get validation results for a scope."""
        conn = self._get_conn()
        if partnership_ein and tax_year:
            result = conn.execute(
                "SELECT * FROM cross_partner_validations WHERE partnership_ein = ? AND tax_year = ? ORDER BY id",
                [partnership_ein, tax_year],
            )
        elif partnership_ein:
            result = conn.execute(
                "SELECT * FROM cross_partner_validations WHERE partnership_ein = ? ORDER BY id",
                [partnership_ein],
            )
        else:
            result = conn.execute("SELECT * FROM cross_partner_validations ORDER BY id")
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_all_validations(self) -> list[dict]:
        """Get all validation results."""
        return self.get_validations_for_scope()

    # -- File tracking (duplicate detection) -----------------------------------

    def check_and_register_file(
        self, pdf_sha256: str, run_id: str, file_name: str, file_size: int
    ) -> dict | None:
        """Check if a file hash has been seen before.

        Returns the existing record if the hash was already registered,
        or None if this is a new file (and registers it).
        """
        with self._locked_conn() as conn:
            result = conn.execute(
                "SELECT * FROM processed_files WHERE pdf_sha256 = ?",
                [pdf_sha256],
            )
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            if rows:
                return dict(zip(columns, rows[0]))

            # Register new file
            conn.execute(
                """INSERT INTO processed_files (pdf_sha256, run_id, file_name, file_size, ingested_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [pdf_sha256, run_id, file_name, file_size,
                 datetime.now(timezone.utc).isoformat()],
            )
            return None

    def get_record_count(self) -> int:
        """Get total number of K-1 records."""
        conn = self._get_conn()
        result = conn.execute("SELECT COUNT(*) FROM k1_records")
        return result.fetchone()[0]


@dg.definitions
def duckdb_resource_defs():
    return dg.Definitions(resources={"duckdb_store": DuckDBStore()})
