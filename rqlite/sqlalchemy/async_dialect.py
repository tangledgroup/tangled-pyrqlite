"""Async SQLAlchemy dialect implementation for rqlite.

This dialect enables SQLAlchemy's async engine to work with rqlite databases
by translating SQLAlchemy operations into HTTP requests via the rqlite
async DB-API client (aiohttp-based).

Key Features:
    - Full SQLAlchemy Core and ORM support via SQLite dialect extension
    - Async/await support for use with create_async_engine()
    - Read consistency levels (LINEARIZABLE, WEAK, NONE, STRONG, AUTO)
    - Transaction support with optional async locking mechanism
    - Connection URL format: rqlite+aiorqlite://host:port

Note on Transaction Warnings:
    When using this dialect **without a lock**, you will receive a `UserWarning`:

        UserWarning: Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.

    This warning indicates that explicit transaction SQL commands are not supported
    in rqlite's traditional sense. This is **expected behavior** and is fine if you
    understand rqlite's queue-based transaction model.

    To suppress this warning and indicate intentional handling of transaction
    limitations, provide a lock via connect_args:

        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> from rqlite import AioLock
        >>> engine = create_async_engine(
        ...     "rqlite+aiorqlite://localhost:4001",
        ...     connect_args={"lock": AioLock()}
        ... )

    For true ACID compliance with proper isolation guarantees, it is recommended
    to use a lock.

Usage Examples:
    Basic async engine (uses LINEARIZABLE consistency by default):
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> engine = create_async_engine("rqlite+aiorqlite://localhost:4001")

    With read consistency via URL query parameter:
        >>> engine = create_async_engine(
        ...     "rqlite+aiorqlite://localhost:4001?read_consistency=weak"
        ... )

    With lock for transaction support:
        >>> from rqlite import AioLock
        >>> engine = create_async_engine(
        ...     "rqlite+aiorqlite://localhost:4001",
        ...     connect_args={"lock": AioLock()}
        ... )

Note on connect_args:
    Custom parameters like `read_consistency` and `lock` must be passed via
    the `connect_args` dictionary to `create_async_engine()`, not as direct
    keyword arguments. This is because SQLAlchemy validates kwargs against known
    Engine/Pool parameters before instantiating the dialect.

    ✅ Correct:
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001",
            connect_args={"lock": lock}
        )

    ❌ Incorrect (will raise Invalid argument error):
        engine = create_async_engine(
            "rqlite+aiorqlite://localhost:4001", lock=lock
        )
"""

from __future__ import annotations

import asyncio
import warnings
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
)

from sqlalchemy import text
from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_module,
    AsyncAdapt_terminate,
)
from sqlalchemy.dialects.sqlite.base import (
    SQLiteCompiler,
    SQLiteExecutionContext,
    SQLiteIdentifierPreparer,
)
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import AdaptedConnection
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import await_only

from rqlite.async_connection import AsyncConnection
from rqlite.async_types import AsyncLockProtocol
from rqlite.exceptions import (
    DatabaseError,
    Error,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from rqlite.types import ReadConsistency

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.engine.interfaces import (
        DBAPIConnection as SAConnection,
    )
    from sqlalchemy.engine.interfaces import (
        DBAPICursor,
    )


class AioRQLiteCursor:
    """DB-API 2.0 compliant async cursor adapted for SQLAlchemy's async engine.

    Wraps rqlite's AsyncCursor and adapts it to the synchronous DB-API 2.0
    interface that SQLAlchemy's async engine expects. All I/O operations
    are bridged via await_only().

    Results are eagerly fetched into a deque during execute(), so fetchone/
    fetchmany/fetchall are synchronous (no I/O).
    """

    __slots__ = (
        "_adapt_connection",
        "_cursor",
        "description",
        "await_",
        "_rows",
        "arraysize",
        "rowcount",
        "lastrowid",
    )

    def __init__(self, adapt_connection: AioRQLiteDBAPIConnectionAdapter) -> None:
        """Initialize cursor with adapted connection.

        Args:
            adapt_connection: The adapted connection providing await_ bridge.
        """
        self._adapt_connection = adapt_connection
        self.await_: Any = adapt_connection.await_
        self.arraysize: int = 1
        self.rowcount: int = -1
        self.description: tuple | None = None
        self._rows: deque = deque()
        self.lastrowid: int | None = None

    async def _async_soft_close(self) -> None:
        """Signal that the async result is no longer needed.

        No-op for rqlite since results are already fully buffered in memory.
        """

    def close(self) -> None:
        """Close the cursor. Clears all buffered results."""
        self._rows.clear()
        self.description = None
        self.rowcount = -1
        self.lastrowid = None

    def execute(
        self,
        operation: str,
        parameters: tuple | dict | None = None,
    ) -> None:
        """Execute a SQL statement and fetch all results.

        This method is synchronous from the DB-API perspective but internally
        bridges to async via await_only().

        Args:
            operation: SQL statement to execute.
            parameters: Parameters (tuple for positional, dict for named).
        """
        try:
            self._cursor = self.await_(self._adapt_connection._connection.cursor())  # type: ignore[union-attr]

            if parameters is None:
                self.await_(self._cursor.execute(operation))
            else:
                self.await_(self._cursor.execute(operation, parameters))

            # Parse result from the async cursor (it stores results internally)
            if hasattr(self._cursor, "description") and self._cursor.description:
                self.description = self._cursor.description
                self.lastrowid = self.rowcount = -1
                # Fetch all results eagerly into deque.
                # Note: AsyncCursor.fetchall() is synchronous (HTTP I/O
                # already happened in execute()), so no await needed.
                if hasattr(self._cursor, "fetchall"):
                    rows = self._cursor.fetchall()
                    self._rows = deque(rows)
            else:
                self.description = None
                if hasattr(self._cursor, "lastrowid"):
                    self.lastrowid = self._cursor.lastrowid  # type: ignore[assignment]
                if hasattr(self._cursor, "rowcount"):
                    self.rowcount = self._cursor.rowcount  # type: ignore[assignment]

            # Close the underlying async cursor (results already buffered)
            self.await_(self._cursor.close())
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[tuple | dict],
    ) -> None:
        """Execute the same operation with multiple parameter sets.

        Args:
            operation: SQL statement to execute.
            seq_of_parameters: Sequence of parameter tuples or dicts.
        """
        try:
            self._cursor = self.await_(self._adapt_connection._connection.cursor())  # type: ignore[union-attr]
            for params in seq_of_parameters:
                if params is None:
                    self.await_(self._cursor.execute(operation))
                else:
                    self.await_(self._cursor.execute(operation, params))

            # Get last result info
            if hasattr(self._cursor, "rowcount"):
                self.rowcount = self._cursor.rowcount  # type: ignore[assignment]
            if hasattr(self._cursor, "lastrowid"):
                self.lastrowid = self._cursor.lastrowid  # type: ignore[assignment]
            self.description = None

            self.await_(self._cursor.close())
        except Exception as error:
            self._adapt_connection._handle_exception(error)

    def setinputsizes(self, *inputsizes: Any) -> None:
        """No-op for compatibility."""

    def __iter__(self) -> Any:
        while self._rows:
            yield self._rows.popleft()

    def fetchone(self) -> tuple | None:
        """Fetch a single row from the pre-buffered result set."""
        if self._rows:
            return self._rows.popleft()
        return None

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        """Fetch up to `size` rows from the pre-buffered result set."""
        if size is None:
            size = self.arraysize
        rr = self._rows
        return [rr.popleft() for _ in range(min(size, len(rr)))]

    def fetchall(self) -> list[tuple]:
        """Fetch all remaining rows from the pre-buffered result set."""
        retval = list(self._rows)
        self._rows.clear()
        return retval


class AioRQLiteDBAPIConnectionAdapter(AsyncAdapt_terminate, AdaptedConnection):
    """DB-API 2.0 compliant connection adapter for rqlite async connection.

    Wraps rqlite's AsyncConnection to provide a synchronous DB-API 2.0
    interface that SQLAlchemy's async engine can use. All async operations
    are bridged via await_only().
    """

    await_ = staticmethod(await_only)
    __slots__ = ("dbapi", "_connection")

    def __init__(self, dbapi: Any, connection: AsyncConnection) -> None:
        """Initialize the connection adapter.

        Args:
            dbapi: The AioRQLiteDBAPI module wrapper.
            connection: The underlying rqlite AsyncConnection.
        """
        self.dbapi = dbapi  # type: ignore[assignment]
        self._connection = connection  # type: ignore[assignment,invalid-assignment]

    @property
    def isolation_level(self) -> str | None:
        """Return the current isolation level.

        rqlite doesn't support traditional SQL isolation levels.
        Returns 'weak' as the default read consistency.
        """
        return "weak"

    @isolation_level.setter
    def isolation_level(self, value: str | None) -> None:
        """Set isolation level (not supported in rqlite)."""
        warnings.warn(
            "rqlite does not support SQL isolation levels.",
            UserWarning,
            stacklevel=2,
        )

    def cursor(self, server_side: bool = False) -> AioRQLiteCursor:
        """Create and return a new cursor.

        Args:
            server_side: Not supported by rqlite (ignored).

        Returns:
            A new AioRQLiteCursor instance.
        """
        return AioRQLiteCursor(self)

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self.await_(self._connection.commit())  # type: ignore[union-attr]
        except Exception as error:
            self._handle_exception(error)

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self.await_(self._connection.rollback())  # type: ignore[union-attr]
        except Exception as error:
            self._handle_exception(error)

    def close(self) -> None:
        """Close the connection.

        Any pending transaction is discarded.
        """
        try:
            self.await_(self._connection.close())  # type: ignore[union-attr]
        except ValueError:
            # Connection already closed — not an error
            pass
        except Exception as error:
            self._handle_exception(error)

    def _handle_exception(self, error: Exception) -> None:
        """Convert rqlite exceptions to DB-API compatible errors.

        Args:
            error: The exception to convert.

        Raises:
            DatabaseError: For general database errors.
            OperationalError: For operational/connection errors.
            ProgrammingError: For SQL syntax/semantic errors.
            IntegrityError: For constraint violations.
        """
        if isinstance(
            error, (DatabaseError, OperationalError, ProgrammingError, IntegrityError)
        ):
            raise error
        # Re-raise as-is for unexpected errors
        raise error

    def create_function(
        self,
        name: str,
        num_params: int,
        func,  # noqa: ANN001
        deterministic: bool = False,
    ) -> None:
        """Create a user-defined function.

        rqlite doesn't support UDFs directly. This is a no-op placeholder
        to satisfy SQLAlchemy's SQLite dialect event handlers.

        Args:
            name: Function name.
            num_params: Number of parameters.
            func: Python function to register.
            deterministic: Whether the function is deterministic.
        """
        # rqlite doesn't support UDFs — no-op for compatibility
        pass

    async def _terminate_graceful_close(self) -> None:
        """Try to close connection gracefully."""
        await self._connection.close()  # type: ignore[union-attr]

    def _terminate_force_close(self) -> None:
        """Terminate the connection forcefully."""
        try:
            asyncio.get_event_loop().run_until_complete(
                self._connection.close()
            )
        except Exception:
            pass


class AioRQLiteDBAPI(AsyncAdapt_dbapi_module):
    """DB-API 2.0 module wrapper for rqlite async driver.

    Provides the module-level interface that SQLAlchemy's async engine expects,
    including exception types, paramstyle, and a connect() method.
    """

    def __init__(self) -> None:
        """Initialize the DBAPI module wrapper."""
        self.paramstyle = "qmark"  # Use ? for positional parameters
        self._init_dbapi_attributes()

    def _init_dbapi_attributes(self) -> None:
        """Set up DB-API 2.0 required attributes from rqlite exceptions."""
        # Exception hierarchy (DB-API 2.0)
        self.Error = Error  # type: ignore[assignment,invalid-assignment]
        self.DatabaseError = DatabaseError  # type: ignore[assignment,invalid-assignment]
        self.OperationalError = OperationalError  # type: ignore[assignment,invalid-assignment]
        self.IntegrityError = IntegrityError  # type: ignore[assignment,invalid-assignment]
        self.ProgrammingError = ProgrammingError  # type: ignore[assignment,invalid-assignment]
        self.InterfaceError = InterfaceError  # type: ignore[assignment,invalid-assignment]
        self.NotSupportedError = NotSupportedError  # type: ignore[assignment,invalid-assignment]

        # SQLite compatibility (required by SQLAlchemy SQLite dialect base)
        self.sqlite_version_info = (3, 45, 0)

    def connect(self, *args: Any, **kwargs: Any) -> AioRQLiteDBAPIConnectionAdapter:
        """Create a new connection to rqlite.

        Parses connection arguments and creates an AsyncConnection, then wraps
        it in AioRQLiteDBAPIConnectionAdapter for SQLAlchemy compatibility.

        Args:
            *args: Positional arguments passed to AsyncConnection.
            **kwargs: Connection keyword arguments (host, port, username,
                     password, read_consistency, lock, timeout).

        Returns:
            An adapted connection ready for use with SQLAlchemy's async engine.
        """
        # Extract rqlite-specific params before creating connection
        host = kwargs.pop("host", "localhost")
        port = kwargs.pop("port", 4001)
        username = kwargs.pop("username", None)
        password = kwargs.pop("password", None)
        timeout = kwargs.pop("timeout", 30.0)
        read_consistency = kwargs.pop(
            "read_consistency", ReadConsistency.LINEARIZABLE
        )
        lock = kwargs.pop("lock", None)

        conn = AsyncConnection(
            host=host,
            port=port,
            username=username,
            password=password,
            timeout=timeout,
            read_consistency=read_consistency,
            lock=lock,
        )
        return AioRQLiteDBAPIConnectionAdapter(self, conn)


class AioRQLiteExecutionContext(SQLiteExecutionContext):
    """Execution context for rqlite async dialect.

    Provides the same transaction warning as the sync dialect: warns when
    SELECT is mixed with INSERT/UPDATE/DELETE in a transaction without a lock.
    """

    def pre_exec(self) -> None:
        """Prepare for statement execution, checking for transaction warnings."""
        super().pre_exec()

        if self.compiled and self.statement:
            stmt_str = str(self.statement).upper()
            if "SELECT" in stmt_str and (self.isinsert or self.isupdate or self.isdelete):
                # Check if connection has a lock (suppress warning if present)
                has_lock = False
                if getattr(self.dialect, "_lock", None):  # type: ignore[unresolved-attribute]
                    has_lock = True
                elif self.connection:
                    dbapi_conn = getattr(self.connection, "connection", None)
                    if dbapi_conn and hasattr(dbapi_conn, "_conn"):
                        rqlite_conn = dbapi_conn._conn
                        if hasattr(rqlite_conn, "_lock") and rqlite_conn._lock:
                            has_lock = True

                if not has_lock:
                    warnings.warn(
                        "rqlite transactions require all statements upfront. "
                        "SELECT results cannot be used within the same transaction.",
                        UserWarning,
                        stacklevel=4,
                    )


class AioRQLiteCompiler(SQLiteCompiler):
    """SQL compiler for rqlite async dialect."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class AioRQLiteIdentifierPreparer(SQLiteIdentifierPreparer):
    """Identifier preparer for rqlite async dialect."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class AioRQLiteDialect(SQLiteDialect_pysqlite):
    """SQLAlchemy async dialect for rqlite.

    This dialect extends SQLiteDialect_pysqlite to work with rqlite servers
    via HTTP using the async rqlite client (aiohttp-based).

    Connection URL format:
        rqlite+aiorqlite://host:port
        rqlite+aiorqlite://user:pass@host:port

    Read Consistency:
        Configure via URL query parameter or connect_args:
        - URL: rqlite+aiorqlite://localhost:4001?read_consistency=weak
        - connect_args: {"read_consistency": ReadConsistency.WEAK}

    Locking for Transactions:
        Provide a lock via connect_args to suppress transaction warnings:
        connect_args={"lock": AioLock()}

    Examples:
        Basic usage:
            >>> from sqlalchemy.ext.asyncio import create_async_engine
            >>> engine = create_async_engine("rqlite+aiorqlite://localhost:4001")

        With read consistency (URL parameter):
            >>> engine = create_async_engine(
            ...     "rqlite+aiorqlite://localhost:4001?read_consistency=weak"
            ... )

        With lock for transaction support:
            >>> from rqlite import AioLock
            >>> engine = create_async_engine(
            ...     "rqlite+aiorqlite://localhost:4001",
            ...     connect_args={"lock": AioLock()}
            ... )

        Combined read_consistency and lock:
            >>> from rqlite import ReadConsistency, AioLock
            >>> engine = create_async_engine(
            ...     "rqlite+aiorqlite://localhost:4001",
            ...     connect_args={
            ...         "read_consistency": ReadConsistency.WEAK,
            ...         "lock": AioLock()
            ...     }
            ... )
    """

    name = "rqlite"
    driver = "aiorqlite"

    # Use our custom components
    execution_ctx_cls = AioRQLiteExecutionContext
    statement_compiler = AioRQLiteCompiler
    preparer = AioRQLiteIdentifierPreparer

    # rqlite-specific settings (same as sync dialect)
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_sequences = False
    supports_native_decimal = True
    supports_native_boolean = True
    supports_comments = False
    supports_statement_cache = True

    # Transaction support (limited, same as sync dialect)
    supports_transactions = True
    supports_isolated_transactions = False

    # Async flag required for SQLAlchemy async engine
    is_async = True
    has_terminate = True

    @classmethod
    def get_pool_class(cls, url) -> type[AsyncAdaptedQueuePool]:
        """Return the appropriate pool class.

        rqlite is always a network database (never a file DB), so we
        always use AsyncAdaptedQueuePool.

        Args:
            url: SQLAlchemy URL object (unused, present for API compatibility).

        Returns:
            AsyncAdaptedQueuePool class.
        """
        return AsyncAdaptedQueuePool

    def is_disconnect(
        self,
        e: Exception,
        connection: SAConnection | None = None,
        cursor: DBAPICursor | None = None,
    ) -> bool:  # type: ignore[invalid-method-override]
        """Detect disconnection errors.

        rqlite raises OperationalError with 'no active connection' message
        when the underlying HTTP connection is lost.

        Args:
            e: The exception that occurred.
            connection: DB-API connection (unused).
            cursor: DB-API cursor (unused).

        Returns:
            True if the error indicates a disconnection.
        """
        if isinstance(e, OperationalError) and "no active connection" in str(e):
            return True
        return super().is_disconnect(e, connection, cursor)  # type: ignore[arg-type,invalid-argument-type]

    def do_terminate(self, dbapi_connection: SAConnection) -> None:
        """Force-terminate a connection.

        Args:
            dbapi_connection: The adapted connection to terminate.
        """
        if hasattr(dbapi_connection, 'terminate'):
            dbapi_connection.terminate()

    def get_driver_connection(
        self, connection: SAConnection,
    ) -> Any:
        """Return the raw driver connection from the adapted connection.

        Args:
            connection: The adapted DB-API connection.

        Returns:
            The underlying AsyncConnection.
        """
        return connection._connection  # type: ignore[no-any-return]

    def __init__(
        self,
        *args: Any,
        read_consistency: ReadConsistency = ReadConsistency.LINEARIZABLE,
        lock: AsyncLockProtocol | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async rqlite dialect.

        Args:
            *args: Positional arguments passed to parent SQLiteDialect_pysqlite.
            read_consistency: Default read consistency level for queries.
                            Accepts ReadConsistency enum or string ("weak",
                            "linearizable", "none", "strong", "auto").
                            Default: ReadConsistency.LINEARIZABLE
                            Can also be set via URL query parameter or connect_args.
            lock: Optional async lock for transaction support. If provided,
                  suppresses warnings about explicit BEGIN/COMMIT/ROLLBACK/
                  SAVEPOINT SQL. Accepts AioLock, or any object satisfying
                  AsyncLockProtocol. Passed via connect_args={"lock": ...}
            **kwargs: Keyword arguments passed to parent SQLiteDialect_pysqlite.

        Note:
            These parameters are typically set via create_async_engine() connect_args:
                engine = create_async_engine(
                    "rqlite+aiorqlite://localhost:4001",
                    connect_args={
                        "read_consistency": ReadConsistency.WEAK,
                        "lock": AioLock()
                    }
                )
        """
        super().__init__(*args, **kwargs)
        self.read_consistency = read_consistency
        self._lock: AsyncLockProtocol | None = lock

    @classmethod
    def import_dbapi(cls) -> AioRQLiteDBAPI:  # type: ignore[invalid-method-override]
        """Import the async DBAPI module.

        Returns:
            An AioRQLiteDBAPI instance providing the DB-API 2.0 interface.
        """
        return AioRQLiteDBAPI()

    def create_connect_args(self, url) -> tuple[list, dict]:
        """Extract connection arguments from URL.

        Parses the SQLAlchemy URL to extract host, port, credentials, and
        rqlite-specific query parameters like read_consistency.

        Args:
            url: SQLAlchemy URL object containing connection details.
                Supports formats:
                - rqlite+aiorqlite://host:port
                - rqlite+aiorqlite://user:pass@host:port
                - rqlite+aiorqlite://host:port?read_consistency=weak

        Returns:
            Tuple of (args, kwargs) for AsyncConnection():
            - args: Empty list (all params are keyword-based)
            - kwargs: Dictionary with host, port, username, password,
                     read_consistency, and timeout parsed from URL.

        Example:
            >>> engine = create_async_engine(
            ...     "rqlite+aiorqlite://localhost:4001?read_consistency=weak"
            ... )
        """
        host = url.host or "localhost"
        port = url.port or 4001
        username = url.username
        password = url.password

        consistency = ReadConsistency.LINEARIZABLE
        if url.query.get("read_consistency"):
            try:
                consistency = ReadConsistency[
                    url.query["read_consistency"].upper()
                ]
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
            "timeout": 30.0,
        }

        return ([], kwargs)

    def connect(  # type: ignore[invalid-method-override]
        self, *args: Any, **kwargs: Any
    ) -> AioRQLiteDBAPIConnectionAdapter:
        """Create a new DB-API async connection.

        This method is called by SQLAlchemy's async engine to create connections.
        It extracts rqlite-specific parameters from kwargs (passed via connect_args)
        and creates an AsyncConnection with those parameters.

        Args:
            *args: Positional arguments passed to AsyncConnection.
            **kwargs: Connection keyword arguments. May include:
                - host: rqlite server hostname (default: "localhost")
                - port: rqlite server port (default: 4001)
                - username: Authentication username (optional)
                - password: Authentication password (optional)
                - read_consistency: Read consistency level (enum or string)
                - lock: Async lock object for transaction support (optional)
                - timeout: Request timeout in seconds (default: 30.0)

        Returns:
            DB-API 2.0 compliant connection wrapped in
            AioRQLiteDBAPIConnectionAdapter.

        Example:
            >>> from sqlalchemy.ext.asyncio import create_async_engine
            >>> from rqlite import AioLock
            >>> engine = create_async_engine(
            ...     "rqlite+aiorqlite://localhost:4001",
            ...     connect_args={"lock": AioLock()}
            ... )
        """
        lock = kwargs.get("lock", self._lock)

        if not lock:
            warnings.warn(
                "Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.",
                UserWarning,
                stacklevel=3,
            )

        dbapi = self.import_dbapi()
        conn = dbapi.connect(*args, **kwargs)
        return conn

    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a SQL statement.

        Args:
            cursor: DB-API cursor (AioRQLiteCursor).
            statement: SQL statement string.
            parameters: Parameters for the statement.
            context: Execution context.
        """
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
        """
        pass

    def get_isolation_level(self, dbapi_connection):
        """Return rqlite's default read consistency level.

        rqlite uses 'weak' as default read consistency per:
        https://rqlite.io/docs/api/read-consistency/

        Args:
            dbapi_connection: DB-API connection (unused).

        Returns:
            "weak" - rqlite's default read consistency level.
        """
        return "weak"

    def set_isolation_level(self, dbapi_connection, level) -> None:  # type: ignore[reportInvalidTypeForm]
        """Warn that isolation levels are not supported in rqlite.

        Args:
            dbapi_connection: DB-API connection (unused).
            level: Requested isolation level (ignored).
        """
        warnings.warn(
            "rqlite does not support SQL isolation levels. "
            "Read consistency is always 'weak' by default. "
            "Application logic must implement locking/sync/isolation mechanisms "
            "for concurrent transactions.",
            UserWarning,
            stacklevel=3,
        )

    def has_table(self, connection, table_name, schema=None, **kw):
        """Check if a table exists.

        Args:
            connection: DB-API connection (adapted).
            table_name: Name of the table to check.
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            True if table exists, False otherwise.
        """
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
            connection: DB-API connection (adapted).
            table_name: Name of the table.
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            List of column dictionaries.
        """
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA table_info({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            columns = []
            for row in rows:
                columns.append({
                    "name": row[1],
                    "type": self._get_column_type(row[2]),
                    "nullable": not row[3],
                    "default": row[4],
                    "primary_key": row[5] == 1,
                    "autoincrement": False,
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
            return "INTEGER"
        elif "text" in type_lower:
            return "TEXT"
        elif "real" in type_lower or "float" in type_lower:
            return "REAL"
        elif "blob" in type_lower:
            return "BLOB"
        elif "bool" in type_lower:
            return "BOOLEAN"
        elif "date" in type_lower or "time" in type_lower:
            return "DATETIME"
        else:
            return "TEXT"

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Get primary key constraint for a table.

        Args:
            connection: DB-API connection (adapted).
            table_name: Name of the table.
            schema: Schema name (unused).
            **kw: Additional keyword arguments.

        Returns:
            Dictionary with primary key info.
        """
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA table_info({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            pk_columns = [row[1] for row in rows if row[5] == 1]
            return {"constrained_columns": pk_columns}
        except Exception:
            return {"constrained_columns": []}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Get foreign keys for a table.

        Args:
            connection: DB-API connection (adapted).
            table_name: Name of the table.
            schema: Schema name (unused).
            **kw: Additional keyword arguments.

        Returns:
            List of foreign key dictionaries.
        """
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA foreign_key_list({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            fks = []
            for row in rows:
                fks.append({
                    "name": None,
                    "constrained_columns": [row[3]],
                    "referred_schema": None,
                    "referred_table": row[2],
                    "referred_columns": [row[4]],
                    "options": {},
                })
            return fks
        except Exception:
            return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Get indexes for a table.

        Args:
            connection: DB-API connection (adapted).
            table_name: Name of the table.
            schema: Schema name (unused).
            **kw: Additional keyword arguments.

        Returns:
            List of index dictionaries.
        """
        quoted_table = self.identifier_preparer.quote_identifier(table_name)
        query = text(f"PRAGMA index_list({quoted_table})")
        try:
            result = connection.execute(query)
            rows = result.fetchall()
            indexes = []
            for row in rows:
                # PRAGMA index_list returns: seq, name, unique, origin, partial
                idx_name = row[1]
                # Get index columns via PRAGMA index_info
                col_query = text(f"PRAGMA index_info({idx_name})")
                col_result = connection.execute(col_query)
                col_rows = col_result.fetchall()
                indexes.append({
                    "name": idx_name,
                    "unique": bool(row[2]),
                    "column_names": [r[2] for r in col_rows],
                })
            return indexes
        except Exception:
            return []

    def get_table_names(  # type: ignore[invalid-method-override]
        self, connection, schema=None, **kw
    ):
        """Get list of table names.

        Args:
            connection: DB-API connection (adapted).
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            List of table name strings.
        """
        return self._get_table_names(connection, schema, **kw)

    def _get_table_names(self, connection, schema=None, **kw):
        """Internal: Get list of table names."""
        query = text(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        try:
            result = connection.execute(query)
            return [row[0] for row in result.fetchall()]
        except Exception:
            return []

    def get_view_names(  # type: ignore[invalid-method-override]
        self, connection, schema=None, **kw
    ):
        """Get list of view names.

        Args:
            connection: DB-API connection (adapted).
            schema: Schema name (unused in rqlite).
            **kw: Additional keyword arguments.

        Returns:
            List of view name strings.
        """
        query = text(
            "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
        )
        try:
            result = connection.execute(query)
            return [row[0] for row in result.fetchall()]
        except Exception:
            return []

    def get_schema_names(self, connection, **kw):
        """Get list of schema names.

        rqlite uses a single database per node.

        Args:
            connection: DB-API connection (adapted).
            **kw: Additional keyword arguments.

        Returns:
            List containing only 'main' (rqlite's default database).
        """
        return ["main"]


# Alias for compatibility with sync dialect naming
AioRQLiteDialect_pyrlite = AioRQLiteDialect
