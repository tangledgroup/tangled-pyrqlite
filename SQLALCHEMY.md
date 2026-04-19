# SQLAlchemy Usage

## Connection URLs

**Note:** For SQLAlchemy, custom parameters like `read_consistency` and `lock` must be passed via `connect_args` dictionary, not directly to `create_engine()`. This is because SQLAlchemy validates kwargs before passing them to the dialect.

```python
# Basic (uses LINEARIZABLE consistency by default)
engine = create_engine("rqlite://localhost:4001")

# With authentication
engine = create_engine("rqlite://admin:secret@localhost:4001")

# Enable SQL echo for debugging
engine = create_engine("rqlite://localhost:4001", echo=True)

# Custom read consistency via URL query parameter
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")
```

```python
# Custom read consistency via connect_args
from rqlite import ReadConsistency

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"read_consistency": ReadConsistency.WEAK}
)
```

## Basic ORM Setup and Usage

```python
from sqlalchemy import Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from rqlite import ReadConsistency, ThreadLock


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    email: Mapped[str | None] = mapped_column(unique=True)


# Basic engine (uses LINEARIZABLE consistency by default)
engine = create_engine("rqlite://localhost:4001")

# With custom read consistency and lock via connect_args
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={
        "read_consistency": ReadConsistency.WEAK,  # or "weak" string
        "lock": ThreadLock()  # Suppresses transaction warnings
    }
)

# Or use URL query parameter for read_consistency only
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")

# Create tables (echo=True shows SQL)
engine = create_engine("rqlite://localhost:4001", echo=True)
Base.metadata.create_all(engine)

# Use with Session
with Session(engine) as session:
    user = User(name="Charlie", email="charlie@example.com")
    session.add(user)
    session.commit()
    
    # Query
    user = session.query(User).filter_by(name="Charlie").first()
    print(user.name)  # Charlie
```

## SQLAlchemy Support

| Feature | Status | Notes |
|---------|--------|-------|
| Core SELECT/INSERT/UPDATE/DELETE | ✅ | Full support |
| ORM Models | ✅ | Full support |
| Relationships | ✅ | Via SQLite dialect |
| Sessions | ✅ | Standard SQLAlchemy sessions |
| Transactions | ⚠️ | Limited (see below) |
| Reflection | ⚠️ | Basic table/column introspection |

## Connection URLs — SQLAlchemy

**Note:** For SQLAlchemy, custom parameters like `read_consistency` and `lock` must be passed via `connect_args` dictionary, not directly to `create_engine()`. This is because SQLAlchemy validates kwargs before passing them to the dialect.

```python
# Basic (uses LINEARIZABLE consistency by default)
engine = create_engine("rqlite://localhost:4001")

# With authentication
engine = create_engine("rqlite://admin:secret@localhost:4001")

# Enable SQL echo for debugging
engine = create_engine("rqlite://localhost:4001", echo=True)

# Custom read consistency via URL query parameter
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")
```

```python
# Custom read consistency via connect_args
from rqlite import ReadConsistency

engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"read_consistency": ReadConsistency.WEAK}
)
```
