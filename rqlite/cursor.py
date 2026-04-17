"""Cursor class implementing DB-API 2.0 for rqlite.

Note on Transaction Warnings:
    When executing explicit transaction SQL commands (BEGIN/COMMIT/ROLLBACK/SAVEPOINT)
    **without a lock**, a `UserWarning` is issued:

        UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.

    This warning indicates that explicit transaction SQL commands are not supported
    in rqlite's traditional sense. This is **expected behavior** and is fine if you
    understand rqlite's queue-based transaction model.

    To suppress this warning and indicate intentional handling of transaction
    limitations, provide a lock when connecting:

        >>> from rqlite import ThreadLock
        >>> conn = connect(lock=ThreadLock())

    For true ACID compliance with proper isolation guarantees, it is recommended
    to use a lock.
"""
from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from .connection import Connection
    from .types import LockProtocol

from .exceptions import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from .types import adapt_value


class Cursor:
    """DB-API 2.0 compliant cursor for rqlite database.

    Attributes:
        connection: The Connection object that created this cursor.
        description: Column metadata after executing a SELECT statement.
        rowcount: Number of rows affected (for writes) or -1 (for reads).
        arraysize: Number of rows to fetch at once with fetchmany().

    Example:
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM users WHERE id=?", (42,))
        >>> row = cursor.fetchone()
        >>> cursor.close()
    """

    def __init__(
        self,
        connection: Connection,
        lock: LockProtocol | None = None,
    ) -> None:
        """Initialize cursor with connection.

        Args:
            connection: The parent Connection object.
            lock: Optional lock for transaction support (from connection).
        """
        # PUBLIC API ATTRIBUTES (PEP 249 DB-API 2.0 compliant)
        # These are intentionally public - they're part of the DB-API contract
        self.connection = connection  # read-only (by convention)
        self.description: list[tuple[Any, ...]] | None = None  # read-only
        self.rowcount: int = -1  # read-only
        self.arraysize: int = 1  # read/write

        # DB-API extension (optional but commonly used)
        self._lastrowid: int | None = None  # read-only, computed

        # INTERNAL STATE (implementation details, not part of public API)
        self._results: list[dict[str, Any]] = []
        self._current_row: int = 0
        self._closed = False
        self._lock: LockProtocol | None = lock
        self._execution_count: int = 0  # Track number of execute() calls

    def execute(
        self,
        operation: str,
        parameters: dict[str, Any] | tuple[Any, ...] | None = None,
    ) -> Cursor:
        """Execute a SQL statement.

        Args:
            operation: SQL statement to execute.
            parameters: Parameters for the statement (positional or named).

        Returns:
            Self for method chaining.

        Raises:
            InterfaceError: If cursor is closed.
            ProgrammingError: If SQL is invalid.
            OperationalError: If request fails.

        Example:
            >>> # Positional parameters
            >>> cursor.execute("SELECT * FROM users WHERE id=?", (42,))
            >>>
            >>> # Named parameters
            >>> cursor.execute(
            ...     "SELECT * FROM users WHERE name=:name",
            ...     {"name": "Alice"}
            ... )
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        # Check for explicit transaction SQL
        operation_upper = operation.upper().strip()
        if any(cmd in operation_upper for cmd in ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT"]):
            # Only warn if no lock is provided (lock indicates user handles transactions)
            if not self._lock:
                warnings.warn(
                    "Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.",
                    UserWarning,
                    stacklevel=3,
                )
            # Skip execution - rqlite doesn't support explicit transaction SQL
            # These commands are handled by the Python API (commit(), rollback())
            self._execution_count += 1
            return self

        # Execute statement immediately
        # Note: rqlite supports transactions via ?transaction=true parameter,
        # but our simple model executes each statement individually.
        # For atomic multi-statement operations, use executemany() or manually
        # batch statements and call connection._execute_batch().
        result = self._execute_single(operation, parameters)
        self._parse_result(result)
        self._execution_count += 1
        return self

    def executemany(
        self,
        operation: str,
        seq_of_parameters: list[dict[str, Any]] | list[tuple[Any, ...]],
    ) -> Cursor:
        """Execute the same operation with multiple parameter sets.

        Args:
            operation: SQL statement to execute.
            seq_of_parameters: Sequence of parameter dicts or tuples.

        Returns:
            Self for method chaining.

        Note:
            This executes statements sequentially, not as a batch.
            For better performance, consider using transactions.
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        # Execute each parameter set
        for params in seq_of_parameters:
            self.execute(operation, params)

        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row.

        Returns:
            A single row as a tuple, or None if no more rows.

        Raises:
            InterfaceError: If cursor is closed.
        """
        if self._closed:
            raise InterfaceError("Cursor is closed")

        # Only warn if fetchone() called without any execute() call
        # Empty results from a SELECT are valid and should not trigger warning
        if self._execution_count == 0:
            warnings.warn(
                "No results to fetch. Execute a SELECT statement first.",
                UserWarning,
                stacklevel=2,
            )
            return None

        if self._current_row >= len(self._results):
            return None

        row = self._results[self._current_row]
        self._current_row += 1

        # Convert dict to tuple in column order
        if self.description:
            columns = [desc[0] for desc in self.description]
            return tuple(row.get(col) for col in columns)

        return tuple(row.values())

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows.

        Args:
            size: Number of rows to fetch (defaults to arraysize).

        Returns:
            List of rows as tuples.
        """
        if size is None:
            size = self.arraysize

        rows = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows.

        Returns:
            List of all remaining rows as tuples.
        """
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)

        return rows

    def close(self) -> None:
        """Close the cursor.

        Raises:
            InterfaceError: If already closed.
        """
        if self._closed:
            return

        self._results = []
        self.description = None
        self.rowcount = -1
        self._current_row = 0
        self._execution_count = 0
        self._closed = True

    def _execute_single(
        self,
        sql: str,
        params: dict[str, Any] | tuple[Any, ...] | None,
    ) -> dict[str, Any]:
        """Execute a single SQL statement.

        Args:
            sql: SQL statement to execute.
            params: Parameters (dict for named, tuple for positional).

        Returns:
            Result dictionary from rqlite.
        """
        # Build query for request
        # rqlite expects an array of queries, where each query is either:
        # - A string (no params): "SELECT * FROM users"
        # - An array with params: ["INSERT INTO users (?)", "Alice"]
        if isinstance(params, dict):
            # Named parameters
            single_query = [sql, {k: adapt_value(v) for k, v in params.items()}]
        elif params:
            # Positional parameters
            single_query = [sql] + [adapt_value(p) for p in params]
        else:
            # No parameters - just the SQL string
            single_query = sql

        # Wrap in array for rqlite (it expects array of queries)
        query = [single_query]

        url = f"{self.connection._base_url}/db/request"

        # Add read consistency parameter for SELECT queries
        # Only apply to read operations, not writes
        params_dict = {}
        sql_upper = sql.upper().strip()
        if sql_upper.startswith("SELECT"):
            params_dict["level"] = self.connection.read_consistency.to_query_param()

        if params_dict:
            url += "?" + "&".join(f"{k}={v}" for k, v in params_dict.items())

        try:
            response = requests.post(
                url,
                json=query,
                auth=self.connection._auth,
                timeout=self.connection.timeout,
            )

            if response.status_code != 200:
                error_data = response.json() if response.content else {"error": "Unknown error"}
                error_msg = error_data.get("error", f"HTTP {response.status_code}")

                # Classify error
                if "SQL logic error" in error_msg or "no such table" in error_msg:
                    raise ProgrammingError(error_msg)
                raise OperationalError(error_msg)

            result = response.json()
            return result.get("results", [{}])[0]

        except requests.RequestException as e:
            raise OperationalError(f"Request failed: {e}") from e

    def _parse_result(self, result: dict[str, Any]) -> None:
        """Parse rqlite response and populate cursor attributes.

        Args:
            result: Result dictionary from rqlite.
        """
        self._results = []
        self._current_row = 0

        # Check for errors in the response first
        if "error" in result:
            error_msg = result["error"]
            # Classify the error
            if "UNIQUE constraint failed" in error_msg or "NOT NULL constraint failed" in error_msg:
                raise IntegrityError(error_msg)
            elif "SQL logic error" in error_msg or "no such table" in error_msg:
                raise ProgrammingError(error_msg)
            else:
                raise DatabaseError(error_msg)

        # Check if this is a SELECT (has columns) or write operation
        # rqlite uses 'values' by default, 'rows' only with ?associative=true
        # Note: 'values' key may be missing for empty result sets
        if "columns" in result:
            # SELECT query - could have values, rows, or neither (empty result)
            columns = result["columns"]

            # Set description regardless of whether there are results
            self.description = [
                (col, rtype, None, None, None, None, True)
                for col, rtype in zip(columns, result.get("types", []), strict=False)
            ]

            if "values" in result:
                # Array format (default)
                values = result.get("values", [])
                self._results = [
                    dict(zip(columns, row, strict=False)) for row in values
                ]
            elif "rows" in result:
                # Associative format (?associative=true)
                self._results = result.get("rows", [])
            else:
                # Empty result set
                self._results = []

            self.rowcount = -1  # Unknown for SELECT

        elif "rows_affected" in result:
            # Write operation (INSERT/UPDATE/DELETE)
            self.description = None
            self._results = []
            self.rowcount = result.get("rows_affected", 0)

            # Store last insert ID if available
            if "last_insert_id" in result:
                self._lastrowid = result["last_insert_id"]

        else:
            # Unknown response format
            self.description = None
            self._results = []
            self.rowcount = 0

    @property
    def lastrowid(self) -> int | None:
        """Return the ID of the last inserted row (read-only DB-API extension).

        Returns:
            The last inserted row ID, or None if not available.
        """
        return self._lastrowid

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        """Make cursor iterable over result set."""
        return self

    def __next__(self) -> tuple[Any, ...]:
        """Return next row or raise StopIteration."""
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Cursor:
        """Enter context manager protocol.

        Returns:
            Self for use in with statement.
        """
        return self

    def __exit__(self, exc_type: type[Exception] | None,
                  exc_val: Exception | None, exc_tb: object) -> None:
        """Exit context manager protocol and close cursor.

        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception instance if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self.close()
