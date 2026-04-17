"""Custom exception classes for rqlite DB-API 2.0 compliance."""



class Error(Exception):
    """Base exception for all rqlite errors."""


class InterfaceError(Error):
    """Error related to the database interface rather than the database itself."""


class DatabaseError(Error):
    """Error related to the database."""


class DataError(DatabaseError):
    """Error due to problem with the processed data."""


class OperationalError(DatabaseError):
    """Error related to operation of the database (e.g., connection issues)."""


class IntegrityError(DatabaseError):
    """Error indicating integrity error in the database (e.g., foreign key violation)."""


class InternalError(DatabaseError):
    """Error indicating internal database error."""


class ProgrammingError(DatabaseError):
    """Error due to programming error (e.g., invalid SQL syntax)."""


class NotSupportedError(DatabaseError):
    """Error for when an operation is not supported by the database."""
