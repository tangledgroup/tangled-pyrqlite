"""SQLAlchemy dialect implementation for rqlite.

This dialect enables SQLAlchemy to work with rqlite databases by translating
SQLAlchemy operations into HTTP requests to the rqlite server.

Key Features:
    - Full SQLAlchemy Core and ORM support via SQLite dialect extension
    - Read consistency levels (LINEARIZABLE, WEAK, NONE, STRONG, AUTO)
    - Transaction support with optional locking mechanism
    - Connection URL format: rqlite://host:port or rqlite://user:pass@host:port

Note on Transaction Warnings:
    When using this dialect **without a lock**, you will receive a `UserWarning`:

        UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.

    This warning indicates that explicit transaction SQL commands are not supported
    in rqlite's traditional sense. This is **expected behavior** and is fine if you
    understand rqlite's queue-based transaction model.

    To suppress this warning and indicate intentional handling of transaction
    limitations, provide a lock via connect_args:

        >>> from sqlalchemy import create_engine
        >>> from rqlite import ThreadLock
        >>> engine = create_engine(
        ...     "rqlite://localhost:4001",
        ...     connect_args={"lock": ThreadLock()}
        ... )

    For true ACID compliance with proper isolation guarantees, it is recommended
    to use a lock."

Usage Examples:
    Basic engine (uses LINEARIZABLE consistency by default):
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("rqlite://localhost:4001")

    With read consistency via URL query parameter:
        >>> engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

    With read consistency and lock via connect_args:
        >>> from rqlite import ReadConsistency, ThreadLock
        >>> engine = create_engine(
        ...     "rqlite://localhost:4001",
        ...     connect_args={
        ...         "read_consistency": ReadConsistency.WEAK,
        ...         "lock": ThreadLock()
        ...     }
        ... )

Note on connect_args:
    Custom parameters like `read_consistency` and `lock` must be passed via
    the `connect_args` dictionary to `create_engine()`, not as direct keyword
    arguments. This is because SQLAlchemy validates kwargs against known
    Engine/Pool parameters before instantiating the dialect.

    ✅ Correct:
        engine = create_engine("rqlite://localhost:4001", connect_args={"lock": lock})

    ❌ Incorrect (will raise Invalid argument error):
        engine = create_engine("rqlite://localhost:4001", lock=lock)
"""

import warnings
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    String,
    text,
)
from sqlalchemy.dialects.sqlite.base import (
    SQLiteCompiler,
    SQLiteDialect,
    SQLiteExecutionContext,
    SQLiteIdentifierPreparer,
)

import rqlite
from rqlite.connection import Connection as RQLiteConnection
from rqlite.types import LockProtocol, ReadConsistency


class RQLiteExecutionContext(SQLiteExecutionContext):
    """Execution context for rqlite."""

    def pre_exec(self):
        """Prepare for statement execution."""
        super().pre_exec()

        # Warn about transactions with SELECT statements (only if no lock)
        if self.compiled and self.statement:
            stmt_str = str(self.statement).upper()
            if "SELECT" in stmt_str and (self.isinsert or self.isupdate or self.isdelete):
                # Check if connection has a lock (suppress warning if present)
                has_lock = False
                if getattr(self.dialect, "_lock", None):  # type: ignore[unresolved-attribute]
                    has_lock = True
                elif self.connection:
                    # Check if dbapi_connection has _lock attribute
                    dbapi_conn = getattr(self.connection, 'connection', None)
                    if dbapi_conn and hasattr(dbapi_conn, '_conn'):
                        rqlite_conn = dbapi_conn._conn
                        if hasattr(rqlite_conn, '_lock') and rqlite_conn._lock:
                            has_lock = True

                if not has_lock:
                    warnings.warn(
                        "rqlite transactions require all statements upfront. "
                        "SELECT results cannot be used within the same transaction.",
                        UserWarning,
                        stacklevel=4,
                    )


class RQLiteCompiler(SQLiteCompiler):
    """SQL compiler for rqlite."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class RQLiteIdentifierPreparer(SQLiteIdentifierPreparer):
    """Identifier preparer for rqlite."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)


class _RQLiteDBAPIConnectionAdapter:
    """Adapter to make RQLiteConnection work as DBAPI connection for SQLAlchemy.

    SQLAlchemy expects a DB-API 2.0 compliant connection object. This adapter
    wraps our Connection class to provide the expected interface.
    """

    def __init__(self, conn: RQLiteConnection):
        self._conn = conn
        self.connection = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


class RQLiteDialect(SQLiteDialect):
    """SQLAlchemy dialect for rqlite.

    This dialect extends SQLiteDialect to work with rqlite servers via HTTP.

    Connection URL format:
        rqlite://host:port
        rqlite://user:pass@host:port

    Read Consistency:
        Configure via URL query parameter or connect_args:
        - URL: rqlite://localhost:4001?read_consistency=weak
        - connect_args: {"read_consistency": ReadConsistency.WEAK}

    Locking for Transactions:
        Provide a lock via connect_args to suppress transaction warnings:
        connect_args={"lock": ThreadLock()}

    Examples:
        Basic usage:
            >>> from sqlalchemy import create_engine
            >>> engine = create_engine("rqlite://localhost:4001")

        With read consistency (URL parameter):
            >>> engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

        With lock for transaction support:
            >>> from rqlite import ThreadLock
            >>> engine = create_engine(
            ...     "rqlite://localhost:4001",
            ...     connect_args={"lock": ThreadLock()}
            ... )

        Combined read_consistency and lock:
            >>> from rqlite import ReadConsistency, ThreadLock
            >>> engine = create_engine(
            ...     "rqlite://localhost:4001",
            ...     connect_args={
            ...         "read_consistency": ReadConsistency.WEAK,
            ...         "lock": ThreadLock()
            ...     }
            ... )
    """

    name = "rqlite"
    driver = "rqlite"

    # Use our custom components
    execution_ctx_cls = RQLiteExecutionContext
    statement_compiler = RQLiteCompiler
    preparer = RQLiteIdentifierPreparer

    # rqlite-specific settings
    supports_sane_rowcount = False  # rqlite doesn't provide row count for SELECT
    supports_sane_multi_rowcount = False
    supports_sequences = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_comments = False
    supports_statement_cache = True  # Enable statement caching for SQLAlchemy 2.0+

    # Transaction support (limited)
    supports_transactions = True
    supports_isolated_transactions = False  # No isolation levels in rqlite

    def __init__(
        self,
        *args: Any,
        read_consistency: ReadConsistency = ReadConsistency.LINEARIZABLE,
        lock: LockProtocol | None = None,
        **kwargs: Any,
    ):
        """Initialize the rqlite dialect.

        Args:
            *args: Positional arguments passed to parent SQLiteDialect.
            read_consistency: Default read consistency level for queries.
                            Accepts ReadConsistency enum or string ("weak",
                            "linearizable", "none", "strong", "auto").
                            Default: ReadConsistency.LINEARIZABLE
                            Can also be set via URL query parameter or connect_args.
            lock: Optional lock for transaction support. If provided, suppresses
                  warnings about explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL.
                  Accepts threading.Lock, rqlite.ThreadLock, or any object
                  satisfying LockProtocol. Passed via connect_args={"lock": ...}
            **kwargs: Keyword arguments passed to parent SQLiteDialect.

        Note:
            These parameters are typically set via create_engine() connect_args:
                engine = create_engine(
                    "rqlite://localhost:4001",
                    connect_args={
                        "read_consistency": ReadConsistency.WEAK,
                        "lock": ThreadLock()
                    }
                )
        """
        super().__init__(*args, **kwargs)
        self.read_consistency = read_consistency
        self._lock: LockProtocol | None = lock

    @classmethod
    def import_dbapi(cls):
        """Import the DBAPI module."""
        return rqlite

    def create_connect_args(self, url):
        """Extract connection arguments from URL.

        Parses the SQLAlchemy URL to extract host, port, credentials, and
        rqlite-specific query parameters like read_consistency.

        Args:
            url: SQLAlchemy URL object containing connection details.
                Supports formats:
                - rqlite://host:port
                - rqlite://user:pass@host:port
                - rqlite://host:port?read_consistency=weak

        Returns:
            Tuple of (args, kwargs) for RQLiteConnection():
            - args: Empty list (all params are keyword-based)
            - kwargs: Dictionary with host, port, username, password,
                     and read_consistency parsed from URL.

        Note:
            The `lock` parameter cannot be passed via URL (it's an object),
            so it must be provided through connect_args to create_engine().

        Example:
            >>> engine = create_engine("rqlite://localhost:4001?read_consistency=weak")
            # URL query param 'read_consistency' is parsed here
        """
        # Parse URL components
        host = url.host or "localhost"
        port = url.port or 4001
        username = url.username
        password = url.password

        # Parse query parameters from URL
        consistency = ReadConsistency.LINEARIZABLE
        if url.query.get("read_consistency"):
            try:
                consistency = ReadConsistency[url.query["read_consistency"].upper()]
            except (ValueError, KeyError):
                warnings.warn(
                    f"Invalid read_consistency: {url.query['read_consistency']}. "
                    f"Using LINEARIZABLE.",
                    UserWarning,
                    stacklevel=2,
                )

        kwargs = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "read_consistency": consistency,
        }

        return ([], kwargs)

    def connect(self, *args: Any, **kwargs: Any):
        """Create a new DB-API connection.

        This method is called by SQLAlchemy's engine to create connections.
        It extracts rqlite-specific parameters from kwargs (passed via connect_args)
        and creates an RQLiteConnection with those parameters.

        Args:
            *args: Positional arguments passed to RQLiteConnection.
            **kwargs: Connection keyword arguments. May include:
                - host: rqlite server hostname (default: "localhost")
                - port: rqlite server port (default: 4001)
                - username: Authentication username (optional)
                - password: Authentication password (optional)
                - read_consistency: Read consistency level (enum or string)
                - lock: Lock object for transaction support (optional)

        Returns:
            DB-API 2.0 compliant connection wrapped in _RQLiteDBAPIConnectionAdapter.

        Example:
            >>> from sqlalchemy import create_engine
            >>> from rqlite import ThreadLock
            >>> engine = create_engine(
            ...     "rqlite://localhost:4001",
            ...     connect_args={"lock": ThreadLock()}
            ... )
            # The lock is extracted from kwargs and passed to RQLiteConnection
        """
        # Warn user about transaction limitations only if no lock is provided
        lock = kwargs.get("lock", self._lock)

        if not lock:
            warnings.warn(
                "Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.",
                UserWarning,
                stacklevel=3,
            )

        conn = RQLiteConnection(*args, **kwargs)
        return _RQLiteDBAPIConnectionAdapter(conn)

    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a SQL statement.

        Args:
            cursor: DB-API cursor.
            statement: SQL statement string.
            parameters: Parameters for the statement.
            context: Execution context.
        """
        # Handle parameter format
        if parameters:
            if isinstance(parameters, (list, tuple)):
                cursor.execute(statement, tuple(parameters))
            else:
                cursor.execute(statement, parameters)
        else:
            cursor.execute(statement)

    def do_commit(self, connection) -> None:  # type: ignore[invalid-method-override]
        """Commit the current transaction."""
        connection.commit()

    def do_rollback(self, connection) -> None:  # type: ignore[invalid-method-override]
        """Rollback the current transaction."""
        connection.rollback()

    def do_begin(self, connection) -> None:  # type: ignore[invalid-method-override]
        """Begin a transaction.

        Note: In rqlite, transactions are implicit - statements are queued
        until commit() is called.

        Args:
            connection: DB-API connection.
        """
        # Transaction starts implicitly when first statement is executed
        pass

    def get_isolation_level(self, dbapi_connection):
        """Return rqlite's default read consistency level.

        rqlite uses 'weak' as default read consistency per:
        https://rqlite.io/docs/api/read-consistency/

        This method returns a static value without executing any SQL.

        Args:
            dbapi_connection: DB-API connection (unused).

        Returns:
            "weak" - rqlite's default read consistency level.
        """
        return "weak"

    def set_isolation_level(self, dbapi_connection, level) -> None:  # type: ignore[reportInvalidTypeForm]
        """Warn that isolation levels are not supported in rqlite.

        rqlite does not support traditional SQL isolation levels.
        The read consistency is always 'weak' by default and is controlled
        via connection parameters, not transaction isolation.

        Application logic must implement any required locking, synchronization,
        or isolation mechanisms for concurrent transactions.

        Args:
            dbapi_connection: DB-API connection (unused).
            level: Requested isolation level (ignored).

        Raises:
            UserWarning: Informs user that isolation levels are not supported.
        """
        warnings.warn(
            "rqlite does not support SQL isolation levels. "
            "Read consistency is always 'weak' by default. "
            "Application logic must implement locking/sync/isolation mechanisms "
            "for concurrent transactions.",
            UserWarning,
            stacklevel=3
        )

    def has_table(self, connection, table_name, schema=None, **kw):
        """Check if a table exists.

        Args:
            connection: DB-API connection.
            table_name: Name of the table to check.
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            True if table exists, False otherwise.
        """
        # Query sqlite_master for table info
        query = text(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name=:name"
        )
        try:
            result = connection.execute(query, {"name": table_name})
            return result.scalar() is not None
        except Exception:
            return False

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Get column information for a table.

        Args:
            connection: DB-API connection.
            table_name: Name of the table.
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            List of column dictionaries.
        """
        # Use PRAGMA table_info to get column info
        # Quote identifier to prevent SQL injection
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA table_info({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            columns = []
            for row in rows:
                # PRAGMA table_info returns: cid, name, type, notnull, default_value, pk
                # Index:              0      1      2       3         4              5
                columns.append({
                    "name": row[1],
                    "type": self._get_column_type(row[2]),
                    "nullable": not row[3],  # notnull field
                    "default": row[4],
                    "primary_key": row[5] == 1,
                    "autoincrement": False,  # rqlite/SQLite doesn't expose this via PRAGMA
                })
            return columns
        except Exception:
            return []

    def _get_column_type(self, type_string):
        """Convert rqlite column type to SQLAlchemy type.

        Args:
            type_string: Type string from rqlite.

        Returns:
            SQLAlchemy type object.
        """
        type_lower = type_string.lower()
        if "int" in type_lower:
            return Integer()
        elif "text" in type_lower:
            return String()
        elif "real" in type_lower or "float" in type_lower:
            return Float()
        elif "blob" in type_lower:
            return LargeBinary()
        elif "bool" in type_lower:
            return Boolean()
        elif "date" in type_lower or "time" in type_lower:
            return DateTime()
        else:
            # Default to String for unknown types
            return String()

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Get primary key constraint for a table.

        Args:
            connection: DB-API connection.
            table_name: Name of the table.
            schema: Schema name (unused).
            **kw: Additional keyword arguments.

        Returns:
            Dictionary with primary key info.
        """
        # Quote identifier to prevent SQL injection
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA table_info({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            pk_columns = [row[1] for row in rows if row[5] == 1]
            return {
                "constrained_columns": pk_columns,
            }
        except Exception:
            return {"constrained_columns": []}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Get foreign keys for a table.

        Args:
            connection: DB-API connection.
            table_name: Name of the table.
            schema: Schema name (unused).
            **kw: Additional keyword arguments.

        Returns:
            List of foreign key dictionaries.
        """
        # Quote identifier to prevent SQL injection
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA foreign_key_list({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            fks = []
            for row in rows:
                fks.append({
                    "name": None,  # rqlite doesn't provide FK names
                    "constrained_columns": [row[3]],
                    "referred_schema": None,
                    "referred_table": row[2],
                    "referred_columns": [row[4]],
                    "options": {},
                })
            return fks
        except Exception:
            return []


# Alias for compatibility
RQLiteDialect_pyrlite = RQLiteDialect
