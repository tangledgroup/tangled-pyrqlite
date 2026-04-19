# Read Consistency Levels

rqlite supports multiple read consistency levels to balance between data freshness and performance. The client defaults to **LINEARIZABLE** for guaranteed fresh reads.

| Level | Speed | Freshness | Best For |
|-------|-------|-----------|----------|
| **LINEARIZABLE** (default) | Moderate | Guaranteed fresh | Critical reads requiring latest data |
| **WEAK** | Fast | Usually current (sub-second staleness possible) | General-purpose reads |
| **NONE** | Fastest | No guarantee | Read-only nodes, max performance |
| **STRONG** | Slow | Guaranteed fresh + applied | Testing only |
| **AUTO** | Varies | Varies | Mixed node type clusters |

## Usage

### DB-API 2.0

```python
import rqlite
from rqlite import ReadConsistency

# Use LINEARIZABLE (default) for guaranteed fresh reads
conn = rqlite.connect()

# Use WEAK for faster reads with possible sub-second staleness
# Supports both enum and string:
conn = rqlite.connect(read_consistency=ReadConsistency.WEAK)
conn = rqlite.connect(read_consistency="weak")

# Use NONE for read-only nodes or maximum performance
conn = rqlite.connect(read_consistency="none")
```

### SQLAlchemy

```python
from sqlalchemy import create_engine

# Via URL query parameter
engine = create_engine("rqlite://localhost:4001?read_consistency=weak")
```

```python
from rqlite import ReadConsistency

# Via connect_args
engine = create_engine(
    "rqlite://localhost:4001",
    connect_args={"read_consistency": ReadConsistency.WEAK}
)
```

See [rqlite documentation](https://rqlite.io/docs/db_api/#read-consistency-levels) for detailed explanations of each consistency level.
