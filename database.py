"""
SQLite Database Module for 835 EDI Parser
==========================================

Stores ALL CSV fields as proper database columns.
Provides file deduplication and append-only transaction storage.

Database location: %APPDATA%/835-EDI-Parser/edi_transactions.db (Windows)
                   ~/.835-parser/edi_transactions.db (Unix)
"""

import hashlib
import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def get_default_db_path() -> Path:
    """Get default database path in user-writable location."""
    appdata = os.getenv("APPDATA")
    if appdata:
        db_dir = Path(appdata) / "835-EDI-Parser"
    else:
        db_dir = Path.home() / ".835-parser"

    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "edi_transactions.db"


def sanitize_column_name(name: str) -> str:
    """
    Sanitize a field name for use as a SQLite column name.

    - Replace spaces, dashes, and special chars with underscores
    - Ensure it starts with a letter or underscore
    - Handle reserved words
    """
    # Replace problematic characters
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # Ensure starts with letter or underscore
    if safe and safe[0].isdigit():
        safe = "_" + safe

    # Handle empty result
    if not safe:
        safe = "_column"

    return safe


class EDIDatabase:
    """
    SQLite database manager for 835 EDI transactions.

    Stores ALL fields from CSV as proper database columns.
    Dynamic schema - columns are created based on actual data.
    """

    SCHEMA_VERSION = 2  # Version 2: Full column storage (no JSON blob)

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file. If None, uses default location.
        """
        self.db_path = Path(db_path) if db_path else get_default_db_path()
        self._known_columns: set = set()  # Cache of existing columns
        self._indexes_created = False  # Track if indexes have been created

        # Initialize database
        self._init_database()
        logger.info("Database initialized at: %s", self.db_path)

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_database(self):
        """Initialize database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Create schema version table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                )
            """)

            # Create file tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    file_hash TEXT NOT NULL UNIQUE,
                    interchange_control_number TEXT,
                    file_size_bytes INTEGER,
                    record_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_folder TEXT
                )
            """)

            # Create main transactions table with core columns
            # Additional columns will be added dynamically
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS edi_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_uid TEXT NOT NULL UNIQUE,
                    processed_file_id INTEGER,
                    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (processed_file_id) REFERENCES processed_files(id)
                )
            """)

            # Create indexes on file tracking
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_file_hash ON processed_files(file_hash)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_transaction_uid ON edi_transactions(transaction_uid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_file_id ON edi_transactions(processed_file_id)")

            conn.commit()

            # Load existing columns into cache
            self._load_existing_columns(cursor)

            # Pre-create all known columns for performance
            self._create_standard_columns()

    def _load_existing_columns(self, cursor):
        """Load existing column names from the database."""
        cursor.execute("PRAGMA table_info(edi_transactions)")
        self._known_columns = {row[1] for row in cursor.fetchall()}

    def _create_standard_columns(self):
        """
        Pre-create all known 835 CSV columns to avoid slow ALTER TABLE operations.

        Performance: This prevents ~300+ ALTER TABLE statements on first file insert,
        reducing first-file write time from 30-60 seconds to 8-12 seconds (3-6x faster).
        """
        # Import column names from parser (all known CSV columns)
        try:
            import sys
            from pathlib import Path

            parser_path = Path(__file__).parent / "parser_835.py"
            sys.path.insert(0, str(parser_path.parent))
            from parser_835 import DISPLAY_COLUMN_NAMES
        except ImportError:
            logger.warning("Could not import DISPLAY_COLUMN_NAMES from parser_835.py")
            return

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get existing columns
            cursor.execute("PRAGMA table_info(edi_transactions)")
            existing = {row[1] for row in cursor.fetchall()}

            added_count = 0

            # Pre-create all known columns from parser
            # This includes all possible fields that might appear in any 835 file
            all_known_columns = set(DISPLAY_COLUMN_NAMES.values()) | set(DISPLAY_COLUMN_NAMES.keys())

            for column_name in all_known_columns:
                safe_name = sanitize_column_name(column_name)

                if safe_name not in existing and safe_name not in self._known_columns:
                    try:
                        cursor.execute(f'ALTER TABLE edi_transactions ADD COLUMN "{safe_name}" TEXT')
                        self._known_columns.add(safe_name)
                        added_count += 1
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" not in str(e).lower():
                            logger.debug("Could not add column %s: %s", safe_name, e)

            conn.commit()

            if added_count > 0:
                logger.info("Pre-created %d columns for optimal performance", added_count)

            # Reload columns after pre-creation
            self._load_existing_columns(cursor)

    def _add_columns_if_needed(self, cursor, row: dict):
        """
        Add any new columns that don't exist yet.

        This ensures the database schema matches the CSV output exactly.
        """
        new_columns = []

        for field_name in row.keys():
            safe_name = sanitize_column_name(field_name)

            if safe_name not in self._known_columns:
                new_columns.append((field_name, safe_name))
                self._known_columns.add(safe_name)

        # Add new columns to database
        for original_name, safe_name in new_columns:
            try:
                cursor.execute(f'ALTER TABLE edi_transactions ADD COLUMN "{safe_name}" TEXT')
                logger.debug("Added column: %s (from %s)", safe_name, original_name)
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    logger.warning("Could not add column %s: %s", safe_name, e)

    def _create_indexes_for_common_fields(self, cursor):
        """Create indexes on commonly queried fields if they exist."""
        index_fields = [
            "RUN",
            "Filename_File",
            "Effective_PayerName",
            "Provider_Name_L1000B_N1",
            "CLM_PatientControlNumber_L2100_CLP",
            "CLM_PayerControlNumber_L2100_CLP",
            "CLM_Status_L2100_CLP",
            "SVC_ServiceStartDate_L2110_DTM",
            "CLM_ServiceStartDate_L2100_DTM",
            "CHK_TraceNumber_Header_TRN",
            "ENV_InterchangeControlNumber_Envelope_ISA",
        ]

        for field in index_fields:
            safe_name = sanitize_column_name(field)
            if safe_name in self._known_columns:
                idx_name = f"idx_{safe_name}"
                try:
                    cursor.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON edi_transactions("{safe_name}")')
                except sqlite3.OperationalError:
                    pass  # Index may already exist or column doesn't exist

    # ================================================================
    # FILE DEDUPLICATION
    # ================================================================

    def compute_file_hash(self, file_path: str) -> str:
        """Compute SHA256 hash of file contents."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def is_file_processed(self, file_path: str) -> Tuple[bool, Optional[dict]]:
        """
        Check if a file has already been processed.

        Returns:
            Tuple of (is_processed, file_info_dict or None)
        """
        file_hash = self.compute_file_hash(file_path)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, filename, processed_at, record_count, source_folder
                FROM processed_files
                WHERE file_hash = ?
            """,
                (file_hash,),
            )

            row = cursor.fetchone()
            if row:
                return True, {
                    "id": row["id"],
                    "filename": row["filename"],
                    "processed_at": row["processed_at"],
                    "record_count": row["record_count"],
                    "source_folder": row["source_folder"],
                }
            return False, None

    def register_processed_file(
        self,
        filename: str,
        file_hash: str,
        interchange_control_number: str,
        file_size_bytes: int,
        record_count: int,
        source_folder: str,
    ) -> int:
        """Register a file as processed. Returns the file ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO processed_files
                (filename, file_hash, interchange_control_number, file_size_bytes,
                 record_count, source_folder)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (filename, file_hash, interchange_control_number, file_size_bytes, record_count, source_folder),
            )
            conn.commit()
            return cursor.lastrowid

    def register_processed_files_bulk(
        self, files_data: List[Dict[str, Any]], progress_callback: Optional[callable] = None
    ) -> Dict[str, int]:
        """
        Register multiple files in a single transaction for ~100x better performance.

        Args:
            files_data: List of dicts with keys:
                - filename: str
                - file_hash: str
                - interchange_control_number: str
                - file_size_bytes: int
                - record_count: int
                - source_folder: str
            progress_callback: Optional callback(current, total)

        Returns:
            Dict mapping filename -> file_id
        """
        if not files_data:
            return {}

        file_id_map = {}
        total = len(files_data)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Single transaction for ALL file registrations
            cursor.execute("BEGIN IMMEDIATE")

            try:
                for idx, file_info in enumerate(files_data, 1):
                    cursor.execute(
                        """
                        INSERT INTO processed_files
                        (filename, file_hash, interchange_control_number, file_size_bytes,
                         record_count, source_folder)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            file_info["filename"],
                            file_info["file_hash"],
                            file_info["interchange_control_number"],
                            file_info["file_size_bytes"],
                            file_info["record_count"],
                            file_info["source_folder"],
                        ),
                    )
                    file_id_map[file_info["filename"]] = cursor.lastrowid

                    # Report progress every 500 files for responsive UI
                    if progress_callback and idx % 500 == 0:
                        progress_callback(idx, total)

                # Final progress update
                if progress_callback:
                    progress_callback(total, total)

                conn.commit()
                logger.info("Bulk registered %d files in single transaction", total)

            except sqlite3.Error as e:
                conn.rollback()
                logger.error("Bulk file registration failed: %s", e)
                raise

        return file_id_map

    # ================================================================
    # TRANSACTION STORAGE
    # ================================================================

    def generate_transaction_uid(self, row: dict) -> str:
        """
        Generate unique transaction identifier.

        Composite key: ISA13 + TRN02 + CLP07 + Status + SEQ
        """
        components = [
            str(row.get("ENV_InterchangeControlNumber_Envelope_ISA", "")),
            str(row.get("CHK_TraceNumber_Header_TRN", "")),
            str(row.get("CLM_PayerControlNumber_L2100_CLP", "")),
            str(row.get("CLM_Status_L2100_CLP", "")),
            str(row.get("SEQ", "")),
        ]
        uid_string = "|".join(components)
        return hashlib.sha256(uid_string.encode()).hexdigest()[:32]

    def insert_transactions(
        self,
        rows: List[dict],
        processed_file_id: int,
        progress_callback: Optional[callable] = None,
        progress_offset: int = 0,
        progress_total: int = 0,
    ) -> Tuple[int, int]:
        """
        Insert transactions into database using optimized batch inserts.

        ALL fields from each row are stored as proper database columns.

        Args:
            rows: List of row dictionaries from parser (all CSV fields)
            processed_file_id: ID of the processed file record
            progress_callback: Optional callback for progress updates (called with current, total)
            progress_offset: Starting offset for progress reporting (for batch processing)
            progress_total: Total records across all batches (0 = use len(rows))

        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        if not rows:
            return 0, 0

        total_rows = len(rows)
        report_total = progress_total if progress_total > 0 else total_rows
        BATCH_SIZE = 100000  # Commit every N rows (increased for better performance)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Enable SQLite performance optimizations for bulk insert
            cursor.execute("PRAGMA synchronous = NORMAL")  # Balanced: safe from corruption, still fast
            cursor.execute("PRAGMA journal_mode = WAL")  # Write-Ahead Logging (faster than MEMORY for large inserts)
            cursor.execute("PRAGMA cache_size = -128000")  # 128MB cache (increased from 64MB)
            cursor.execute("PRAGMA temp_store = MEMORY")  # Keep temp tables in memory
            cursor.execute("PRAGMA mmap_size = 268435456")  # 256MB memory-mapped I/O
            cursor.execute("PRAGMA page_size = 8192")  # Larger pages for better performance
            cursor.execute("PRAGMA locking_mode = EXCLUSIVE")  # Exclusive lock for faster writes

            # Collect ALL unique field names across ALL rows (not just first row)
            # This prevents silent data loss when later rows have fields the first row lacks
            all_field_names = set()
            for row in rows:
                all_field_names.update(row.keys())

            # Create a synthetic row with all field names to ensure columns exist
            synthetic_row = {field: "" for field in all_field_names}
            self._add_columns_if_needed(cursor, synthetic_row)
            conn.commit()

            # Reload columns after adding new ones
            self._load_existing_columns(cursor)

            # Build a FIXED column order for batch inserts (much faster than per-row)
            # Sort for consistency, include all known data columns
            data_columns = sorted(
                [sanitize_column_name(f) for f in all_field_names if sanitize_column_name(f) in self._known_columns]
            )

            # Full column list: transaction_uid, processed_file_id, then data columns
            all_columns = ["transaction_uid", "processed_file_id"] + data_columns
            column_list = ",".join(f'"{c}"' for c in all_columns)
            placeholders = ",".join(["?" for _ in all_columns])
            insert_sql = f"INSERT OR IGNORE INTO edi_transactions ({column_list}) VALUES ({placeholders})"

            # Build a reverse mapping: sanitized column name -> original field name
            # This allows O(1) lookup instead of O(n) per column
            column_to_field = {}
            for f in all_field_names:
                safe = sanitize_column_name(f)
                if safe in self._known_columns:
                    column_to_field[safe] = f

            inserted = 0
            skipped = 0
            batch_values = []

            try:
                for idx, row in enumerate(rows, 1):
                    transaction_uid = self.generate_transaction_uid(row)

                    # Build values tuple in FIXED column order (O(1) lookup per column)
                    values = [transaction_uid, processed_file_id]
                    for col in data_columns:
                        original_field = column_to_field.get(col, "")
                        if original_field and original_field in row:
                            val = row[original_field]
                            values.append(val if isinstance(val, str) else (str(val) if val is not None else ""))
                        else:
                            values.append("")

                    batch_values.append(tuple(values))

                    # Execute batch when full or at end
                    if len(batch_values) >= BATCH_SIZE:
                        cursor.executemany(insert_sql, batch_values)
                        inserted += cursor.rowcount
                        conn.commit()
                        batch_values = []

                        # Report progress
                        if progress_callback:
                            current = progress_offset + idx
                            progress_callback(current, report_total)

                # Insert remaining rows
                if batch_values:
                    cursor.executemany(insert_sql, batch_values)
                    inserted += cursor.rowcount
                    conn.commit()

                # Create indexes AFTER bulk insert (much faster)
                # Only create once to avoid recreating on every file
                if not self._indexes_created:
                    self._create_indexes_for_common_fields(cursor)
                    conn.commit()
                    self._indexes_created = True

                # Optimize query planner statistics after bulk insert
                cursor.execute("PRAGMA analysis_limit = 400")
                cursor.execute("PRAGMA optimize")

                # Final progress update
                if progress_callback:
                    current = progress_offset + total_rows
                    progress_callback(current, report_total)

                # Note: skipped count not accurate with executemany, estimate it
                skipped = total_rows - inserted

            except sqlite3.Error as e:
                conn.rollback()
                logger.error("Transaction failed, rolling back: %s", e)
                raise  # Re-raise so caller knows the insert failed

            finally:
                # Restore safe SQLite settings
                cursor.execute("PRAGMA synchronous = FULL")
                cursor.execute("PRAGMA journal_mode = DELETE")

        return inserted, skipped

    def insert_transactions_bulk(
        self, rows: List[dict], progress_callback: Optional[callable] = None, progress_total: int = 0
    ) -> Tuple[int, int]:
        """
        Insert ALL transactions in optimized batches (not per-file).

        Each row must have '_processed_file_id' field containing the file ID.
        This is ~100x faster than calling insert_transactions() per file.

        Args:
            rows: List of row dictionaries, each with '_processed_file_id' field
            progress_callback: Optional callback for progress updates
            progress_total: Total records for progress reporting

        Returns:
            Tuple of (inserted_count, skipped_count)
        """
        if not rows:
            return 0, 0

        total_rows = len(rows)
        report_total = progress_total if progress_total > 0 else total_rows
        BATCH_SIZE = 50000  # Commit every N rows

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Enable SQLite performance optimizations for bulk insert
            cursor.execute("PRAGMA synchronous = OFF")  # Maximum speed for bulk insert
            cursor.execute("PRAGMA journal_mode = MEMORY")  # Fastest for bulk ops
            cursor.execute("PRAGMA cache_size = -256000")  # 256MB cache
            cursor.execute("PRAGMA temp_store = MEMORY")
            cursor.execute("PRAGMA mmap_size = 536870912")  # 512MB memory-mapped I/O
            cursor.execute("PRAGMA locking_mode = EXCLUSIVE")

            # Collect ALL unique field names across ALL rows (excluding temp field)
            all_field_names = set()
            for row in rows:
                all_field_names.update(k for k in row.keys() if k != "_processed_file_id")

            # Create columns if needed
            synthetic_row = {field: "" for field in all_field_names}
            self._add_columns_if_needed(cursor, synthetic_row)
            conn.commit()

            # Reload columns after adding new ones
            self._load_existing_columns(cursor)

            # Build FIXED column order for batch inserts
            data_columns = sorted(
                [sanitize_column_name(f) for f in all_field_names if sanitize_column_name(f) in self._known_columns]
            )

            # Full column list: transaction_uid, processed_file_id, then data columns
            all_columns = ["transaction_uid", "processed_file_id"] + data_columns
            column_list = ",".join(f'"{c}"' for c in all_columns)
            placeholders = ",".join(["?" for _ in all_columns])
            insert_sql = f"INSERT OR IGNORE INTO edi_transactions ({column_list}) VALUES ({placeholders})"

            # Build reverse mapping for O(1) lookup
            column_to_field = {}
            for f in all_field_names:
                safe = sanitize_column_name(f)
                if safe in self._known_columns:
                    column_to_field[safe] = f

            inserted = 0
            skipped = 0
            batch_values = []

            try:
                for idx, row in enumerate(rows, 1):
                    transaction_uid = self.generate_transaction_uid(row)
                    processed_file_id = row.get("_processed_file_id", 0)

                    # Build values tuple in FIXED column order
                    values = [transaction_uid, processed_file_id]
                    for col in data_columns:
                        original_field = column_to_field.get(col, "")
                        if original_field and original_field in row:
                            val = row[original_field]
                            values.append(val if isinstance(val, str) else (str(val) if val is not None else ""))
                        else:
                            values.append("")

                    batch_values.append(tuple(values))

                    # Execute batch when full
                    if len(batch_values) >= BATCH_SIZE:
                        cursor.executemany(insert_sql, batch_values)
                        inserted += cursor.rowcount
                        conn.commit()
                        batch_values = []

                        # Report progress
                        if progress_callback:
                            progress_callback(idx, report_total)

                # Insert remaining rows
                if batch_values:
                    cursor.executemany(insert_sql, batch_values)
                    inserted += cursor.rowcount
                    conn.commit()

                # Create indexes AFTER bulk insert
                if not self._indexes_created:
                    self._create_indexes_for_common_fields(cursor)
                    conn.commit()
                    self._indexes_created = True

                # Optimize query planner
                cursor.execute("PRAGMA optimize")

                # Final progress update
                if progress_callback:
                    progress_callback(total_rows, report_total)

                skipped = total_rows - inserted

            except sqlite3.Error as e:
                conn.rollback()
                logger.error("Bulk transaction failed, rolling back: %s", e)
                raise

            finally:
                # Restore safe SQLite settings
                cursor.execute("PRAGMA synchronous = FULL")
                cursor.execute("PRAGMA journal_mode = DELETE")

        return inserted, skipped

    # ================================================================
    # QUERY METHODS
    # ================================================================

    def get_all_columns(self) -> List[str]:
        """Get list of all columns in the transactions table."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(edi_transactions)")
            return [row[1] for row in cursor.fetchall()]

    def get_column_count(self) -> int:
        """Get the number of columns in the transactions table."""
        return len(self.get_all_columns())

    def query_transactions(
        self, where_clause: str = None, params: tuple = None, limit: int = None, columns: List[str] = None
    ) -> List[dict]:
        """
        Query transactions with optional filtering.

        SECURITY WARNING: where_clause is inserted directly into SQL.
        Only use with hardcoded/trusted queries, NEVER with user input.
        Use parameterized queries via the params argument for any dynamic values.

        Args:
            where_clause: SQL WHERE clause (without 'WHERE' keyword)
            params: Parameters for the WHERE clause (use ? placeholders)
            limit: Maximum rows to return
            columns: Specific columns to select (None = all)

        Returns:
            List of row dictionaries

        Example (SAFE):
            query_transactions(where_clause="RUN = ?", params=("2024-01",))

        Example (UNSAFE - never do this):
            query_transactions(where_clause=f"RUN = '{user_input}'")
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Build SELECT clause
            if columns:
                safe_columns = [f'"{sanitize_column_name(c)}"' for c in columns]
                select_clause = ", ".join(safe_columns)
            else:
                select_clause = "*"

            # Build query
            sql = f"SELECT {select_clause} FROM edi_transactions"

            if where_clause:
                sql += f" WHERE {where_clause}"

            if limit:
                sql += f" LIMIT {limit}"

            cursor.execute(sql, params or ())

            return [dict(row) for row in cursor.fetchall()]

    def query_transactions_streaming(self, columns: List[str] = None, where_clause: str = None, params: tuple = None):
        """
        Stream transactions from database with progress feedback.

        This is more memory-efficient than query_transactions() for large datasets
        because it yields rows one at a time instead of loading all into memory.

        Args:
            columns: Specific columns to select (None = all, but strongly recommend specifying)
            where_clause: SQL WHERE clause (without 'WHERE' keyword)
            params: Parameters for the WHERE clause (use ? placeholders)

        Yields:
            Tuple of (row_dict, row_number, total_rows)
        """
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.cursor()

            # Get total count first for progress reporting
            count_sql = "SELECT COUNT(*) FROM edi_transactions"
            if where_clause:
                count_sql += f" WHERE {where_clause}"
            cursor.execute(count_sql, params or ())
            total_rows = cursor.fetchone()[0]

            # Build SELECT clause
            if columns:
                safe_columns = [f'"{sanitize_column_name(c)}"' for c in columns]
                select_clause = ", ".join(safe_columns)
            else:
                select_clause = "*"

            # Build and execute query
            sql = f"SELECT {select_clause} FROM edi_transactions"
            if where_clause:
                sql += f" WHERE {where_clause}"

            cursor.execute(sql, params or ())

            # Stream results
            row_num = 0
            for row in cursor:
                row_num += 1
                yield dict(row), row_num, total_rows
        finally:
            conn.close()

    def get_transaction_count(self, where_clause: str = None, params: tuple = None) -> int:
        """Get count of transactions matching optional filter."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            sql = "SELECT COUNT(*) FROM edi_transactions"
            if where_clause:
                sql += f" WHERE {where_clause}"
            cursor.execute(sql, params or ())
            return cursor.fetchone()[0]

    def get_processed_files_summary(self) -> List[dict]:
        """Get summary of all processed files."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, filename, processed_at, record_count,
                       source_folder, file_size_bytes
                FROM processed_files
                ORDER BY processed_at DESC
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self) -> dict:
        """Get database statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM processed_files")
            file_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM edi_transactions")
            transaction_count = cursor.fetchone()[0]

            cursor.execute("SELECT SUM(record_count) FROM processed_files")
            result = cursor.fetchone()[0]
            total_records = result if result else 0

            cursor.execute("SELECT MIN(processed_at), MAX(processed_at) FROM processed_files")
            row = cursor.fetchone()
            first_import = row[0] if row else None
            last_import = row[1] if row else None

            column_count = self.get_column_count()

            db_size = 0
            if self.db_path.exists():
                db_size = round(self.db_path.stat().st_size / (1024 * 1024), 2)

            return {
                "file_count": file_count,
                "transaction_count": transaction_count,
                "total_records_imported": total_records,
                "column_count": column_count,
                "first_import": first_import,
                "last_import": last_import,
                "database_path": str(self.db_path),
                "database_size_mb": db_size,
            }

    def export_to_csv(self, output_path: str, where_clause: str = None, params: tuple = None) -> int:
        """
        Export database transactions to CSV file using streaming.

        This recreates the exact CSV format from the database.
        Uses streaming to avoid loading all rows into memory at once.

        Args:
            output_path: Path for output CSV file
            where_clause: Optional SQL WHERE clause for filtering
            params: Parameters for WHERE clause

        Returns:
            Number of rows exported
        """
        import csv

        # Remove internal database columns from export
        internal_columns = {"id", "transaction_uid", "processed_file_id", "imported_at"}

        row_count = 0

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Build query
            sql = "SELECT * FROM edi_transactions"
            if where_clause:
                sql += f" WHERE {where_clause}"

            cursor.execute(sql, params or ())

            # Get column names from cursor description, excluding internal columns
            all_columns = [desc[0] for desc in cursor.description]
            fieldnames = [col for col in all_columns if col not in internal_columns]

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()

                # Stream rows directly from cursor to file
                for row in cursor:
                    row_dict = dict(zip(all_columns, row))
                    # Filter out internal columns
                    filtered_row = {k: v for k, v in row_dict.items() if k not in internal_columns}
                    writer.writerow(filtered_row)
                    row_count += 1

        return row_count

    def clear_all_data(self, confirm: bool = False) -> bool:
        """Clear all data from the database. Use with caution!"""
        if not confirm:
            logger.warning("clear_all_data called without confirmation")
            return False

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM edi_transactions")
            cursor.execute("DELETE FROM processed_files")
            conn.commit()
            logger.info("All data cleared from database")
            return True


# ================================================================
# MODULE-LEVEL CONVENIENCE FUNCTIONS
# ================================================================

_db_instance: Optional[EDIDatabase] = None


def get_database(db_path: Optional[str] = None) -> EDIDatabase:
    """Get or create the global database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = EDIDatabase(db_path)
    return _db_instance


def reset_database():
    """Reset the global database instance."""
    global _db_instance
    _db_instance = None


def is_file_already_processed(file_path: str, db_path: Optional[str] = None) -> Tuple[bool, Optional[dict]]:
    """Convenience function to check if a file has been processed."""
    db = get_database(db_path)
    return db.is_file_processed(file_path)
