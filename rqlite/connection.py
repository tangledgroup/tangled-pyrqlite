"""Connection class implementing DB-API 2.0 for rqlite.

Note on Transaction Warnings:
    When creating a Connection **without a lock**, a `UserWarning` is issued:
    
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

import warnings
from typing import TYPE_CHECKING, Any, Literal

import requests

if TYPE_CHECKING:
    from .types import LockProtocol

from .cursor import Cursor
from .exceptions import OperationalError
from .types import ReadConsistency

ReadConsistencyStr = Literal["weak", "linearizable", "none", "strong", "auto"]


class Connection:
    """DB-API 2.0 compliant connection to rqlite database.

    Attributes:
        host: Hostname or IP address of the rqlite server.
        port: Port number of the rqlite server.
        username: Username for authentication (optional).
        password: Password for authentication (optional).
        db_name: Database name (rqlite uses single database per node).

    Example:
        >>> conn = connect(host="localhost", port=4001)
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM users")
        >>> rows = cursor.fetchall()
        >>> conn.close()
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 4001,
        username: str | None = None,
        password: str | None = None,
        db_name: str | None = None,
        timeout: float = 30.0,
        read_consistency: ReadConsistency | ReadConsistencyStr = ReadConsistency.LINEARIZABLE,
        lock: LockProtocol | None = None,
    ) -> None:
        """Initialize connection to rqlite server.

        Args:
            host: Hostname or IP address of the rqlite server.
            port: Port number (default: 4001).
            username: Username for authentication (optional).
            password: Password for authentication (optional).
            db_name: Database name (optional, rqlite uses single DB per node).
            timeout: Request timeout in seconds (default: 30.0).
            read_consistency: Read consistency level for queries.
                            Default: ReadConsistency.LINEARIZABLE
                            Accepts ReadConsistency enum or string ("weak", "linearizable",
                            "none", "strong", "auto").
            lock: Optional lock for transaction support. If provided, suppresses
                  warnings about transaction limitations. Can be threading.Lock,
                  rqlite.Lock, or any object satisfying LockProtocol.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.timeout = timeout
        self.read_consistency = self._parse_read_consistency(read_consistency)
        self._lock: LockProtocol | None = lock

        # Warn about transaction limitations if no lock is provided
        if not lock:
            warnings.warn(
                "Explicit BEGIN/COMMIT/ROLLBACK/SAVEPOINT SQL is not supported.",
                UserWarning,
                stacklevel=3,
            )

        # Build base URL
        self._base_url = f"http://{host}:{port}"

        # Authentication headers if credentials provided
        self._auth = None
        if username and password:
            self._auth = (username, password)

        # Transaction queue - statements pending commit
        self._pending_statements: list[tuple[str, dict[str, Any] | tuple[Any, ...]]] = []
        self._in_transaction = False

        # Track if connection is closed
        self._closed = False

    def _parse_read_consistency(
        self,
        value: ReadConsistency | ReadConsistencyStr,
    ) -> ReadConsistency:
        """Parse read consistency from enum or string.

        Args:
            value: ReadConsistency enum or string value.

        Returns:
            ReadConsistency enum value.

        Raises:
            ValueError: If value is not a valid consistency level.
        """
        if isinstance(value, ReadConsistency):
            return value
        # Try to match by value (case-insensitive)
        value_lower = str(value).lower()
        for level in ReadConsistency:
            if level.value.lower() == value_lower:
                return level
        valid = ", ".join(f"'{level.value}'" for level in ReadConsistency)
        raise ValueError(
            f"Invalid read_consistency: '{value}'. "
            f"Must be one of: {valid}"
        )

    def cursor(self) -> Cursor:
        """Create and return a new Cursor object for this connection.

        Returns:
            A new Cursor instance.

        Raises:
            InterfaceError: If the connection is closed.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self, lock=self._lock)

    def commit(self) -> None:
        """Commit any pending transaction.

        In rqlite, transactions require all statements to be sent upfront.
        This method sends all queued statements in a single atomic request.

        Raises:
            OperationalError: If the connection is closed or transaction fails.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")

        if not self._pending_statements:
            return

        try:
            # Send all statements with transaction=true
            self._execute_batch(
                self._pending_statements,
                transaction=True,
            )
            self._pending_statements = []
            self._in_transaction = False
        except Exception as e:
            # Clear pending statements on failure
            self._pending_statements = []
            self._in_transaction = False
            raise OperationalError(f"Transaction commit failed: {e}") from e

    def rollback(self) -> None:
        """Rollback any pending transaction.

        In rqlite, rollback simply discards queued statements that haven't
        been executed yet. Since rqlite doesn't support partial transactions,
        there's nothing to roll back on the server side.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")

        # Simply discard pending statements
        self._pending_statements = []
        self._in_transaction = False

    def close(self) -> None:
        """Close the connection.

        Any pending transaction is discarded.
        """
        if self._closed:
            return

        # Discard any pending statements
        self._pending_statements = []
        self._in_transaction = False
        self._closed = True

    def _execute_batch(
        self,
        statements: list[tuple[str, dict[str, Any] | tuple[Any, ...]]],
        transaction: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute a batch of statements via HTTP.

        Args:
            statements: List of (sql, params) tuples.
            transaction: If True, execute as atomic transaction.

        Returns:
            List of result dictionaries from rqlite.

        Raises:
            OperationalError: If request fails.
            ProgrammingError: If SQL is invalid.
        """
        # Build request body
        queries = []
        for sql, params in statements:
            if isinstance(params, dict):
                # Named parameters
                queries.append([sql, params])
            elif params:
                # Positional parameters
                queries.append([sql] + list(params))
            else:
                # No parameters - wrap in array for rqlite
                queries.append([sql])

        url = f"{self._base_url}/db/request"
        if transaction:
            url += "?transaction=true"

        try:
            response = requests.post(
                url,
                json=queries,
                auth=self._auth,
                timeout=self.timeout,
            )

            if response.status_code != 200:
                error_data = response.json() if response.content else {"error": "Unknown error"}
                raise OperationalError(error_data.get("error", f"HTTP {response.status_code}"))

            result = response.json()
            return result.get("results", [])

        except requests.RequestException as e:
            raise OperationalError(f"Request failed: {e}") from e

    def __enter__(self) -> Connection:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        if exc_type is not None:
            # Rollback on exception
            self.rollback()
        self.close()


class InterfaceError(Exception):
    """Raised when connection is used after being closed."""


def connect(
    host: str = "localhost",
    port: int = 4001,
    username: str | None = None,
    password: str | None = None,
    db_name: str | None = None,
    timeout: float = 30.0,
    read_consistency: ReadConsistency | ReadConsistencyStr = ReadConsistency.LINEARIZABLE,
    lock: LockProtocol | None = None,
) -> Connection:
    """Create a new connection to rqlite server.

    Args:
        host: Hostname or IP address of the rqlite server.
        port: Port number (default: 4001).
        username: Username for authentication (optional).
        password: Password for authentication (optional).
        db_name: Database name (optional).
        timeout: Request timeout in seconds (default: 30.0).
        read_consistency: Read consistency level for queries.
                        Default: ReadConsistency.LINEARIZABLE
                        Accepts ReadConsistency enum or string.
        lock: Optional lock for transaction support. If provided, suppresses
              warnings about transaction limitations. Can be threading.Lock,
              rqlite.Lock, or any object satisfying LockProtocol.

    Returns:
        A new Connection instance.

    Example:
        >>> conn = connect(host="localhost", port=4001)
        >>> cursor = conn.cursor()
        >>> cursor.execute("SELECT * FROM users")
        >>> conn.close()
        >>> # With locking for transaction support
        >>> import threading
        >>> conn = connect(host="localhost", port=4001, lock=threading.Lock())
    """
    return Connection(
        host=host,
        port=port,
        username=username,
        password=password,
        db_name=db_name,
        timeout=timeout,
        read_consistency=read_consistency,
        lock=lock,
    )
