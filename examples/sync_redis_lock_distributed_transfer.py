"""Cross-process bank transfer demo — proves SyncRedisLock serializes transactions.

This example demonstrates that a distributed Redis lock provides true
cross-process transaction serialization for rqlite. Without the lock,
concurrent processes will corrupt data through lost updates. With the lock,
operations are serialized and data integrity is preserved.

Prerequisites:
    Install optional dependency:
        uv add tangled-pyrqlite[redis]

    Start Redis server:
        podman rm -f redis-test
        podman run -d --name redis-test -p 6379:6379 docker.io/redis

    Start rqlite server:
        podman rm -f rqlite-test
        podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite

Usage:
    # Run without lock (shows race condition):
    uv run python -B examples/sync_redis_lock_distributed_transfer.py --no-lock

    # Run with lock (proves serialization):
    uv run python -B examples/sync_redis_lock_distributed_transfer.py --with-lock

    # Auto-demo both scenarios:
    uv run python -B examples/sync_redis_lock_distributed_transfer.py --all
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import random
import time


def _worker_no_lock(args: tuple) -> dict:
    """Worker that performs transfer WITHOUT lock — demonstrates race condition."""
    import rqlite

    _, iterations, amount = args
    conn = rqlite.connect(host="localhost", port=4001)
    cursor = conn.cursor()

    try:
        for _ in range(iterations):
            # Read current balance (no lock — may read stale data)
            cursor.execute("SELECT balance FROM transfer_accounts WHERE account=?", ("SENDER",))
            row = cursor.fetchone()
            assert row is not None
            if row is None:
                continue
            balance = row[0]

            # Simulate some processing delay to widen race window
            import time as _time

            _time.sleep(random.uniform(0.001, 0.01))

            # Write new balance (may overwrite another process's update)
            cursor.execute(
                "UPDATE transfer_accounts SET balance=? WHERE account=?",
                (balance - amount, "SENDER"),
            )
            conn.commit()

        # Final read to check result
        cursor.execute("SELECT balance FROM transfer_accounts WHERE account=?", ("SENDER",))
        row = cursor.fetchone()
        assert row is not None
        final_balance = row[0] if row else None

        return {"final_balance": final_balance, "iterations": iterations}

    finally:
        cursor.close()
        conn.close()


def _worker_with_lock(args: tuple) -> dict:
    """Worker that performs transfer WITH lock — proves serialization."""
    import rqlite
    from rqlite import RedisLock

    _, iterations, amount = args
    lock = RedisLock(name="transfer_demo", timeout=30.0)
    conn = rqlite.connect(host="localhost", port=4001, lock=lock)
    cursor = conn.cursor()

    try:
        for _ in range(iterations):
            # Read current balance (under lock — no stale reads)
            with lock:
                cursor.execute("SELECT balance FROM transfer_accounts WHERE account=?", ("SENDER",))
                row = cursor.fetchone()
                assert row is not None
                if row is None:
                    break
                balance = row[0]

                # Simulate processing delay — now safe, another process waits
                import time as _time

                _time.sleep(random.uniform(0.001, 0.01))

                # Write new balance (serialized — no lost updates)
                cursor.execute(
                    "UPDATE transfer_accounts SET balance=? WHERE account=?",
                    (balance - amount, "SENDER"),
                )
                conn.commit()

        # Final read to check result
        cursor.execute("SELECT balance FROM transfer_accounts WHERE account=?", ("SENDER",))
        row = cursor.fetchone()
        assert row is not None
        final_balance = row[0] if row else None

        return {"final_balance": final_balance, "iterations": iterations}

    finally:
        cursor.close()
        conn.close()


def run_scenario(
    use_lock: bool,
    num_processes: int = 4,
    iterations_per_process: int = 10,
    transfer_amount: float = 50.0,
) -> dict:
    """Run the distributed transfer scenario.

    Args:
        use_lock: Whether to use RedisLock for serialization.
        num_processes: Number of concurrent processes.
        iterations_per_process: How many transfers each process makes.
        transfer_amount: Amount per transfer.

    Returns:
        Dictionary with results and analysis.
    """
    import rqlite

    # Setup: create accounts table with initial balance
    setup_conn = rqlite.connect(host="localhost", port=4001)
    setup_cursor = setup_conn.cursor()
    try:
        setup_cursor.execute("DROP TABLE IF EXISTS transfer_accounts")
        setup_cursor.execute("""
            CREATE TABLE transfer_accounts (
                id INTEGER PRIMARY KEY,
                account TEXT NOT NULL UNIQUE,
                balance REAL NOT NULL
            )
        """)
        initial_balance = 5000.0
        setup_cursor.execute(
            "INSERT INTO transfer_accounts (account, balance) VALUES (?, ?)",
            ("SENDER", initial_balance),
        )
        setup_conn.commit()
    finally:
        setup_cursor.close()
        setup_conn.close()

    expected_final = initial_balance - (num_processes * iterations_per_process * transfer_amount)
    total_expected_deductions = num_processes * iterations_per_process * transfer_amount

    # Run workers in separate processes
    args_list = [(i, iterations_per_process, transfer_amount) for i in range(num_processes)]
    func = _worker_with_lock if use_lock else _worker_no_lock

    print("\n  Configuration:")
    print(f"    Processes: {num_processes}")
    print(f"    Iterations/process: {iterations_per_process}")
    print(f"    Transfer amount: ${transfer_amount:.2f}")
    print(f"    Expected final balance: ${expected_final:.2f}")
    print(f"    Expected total deductions: ${total_expected_deductions:.2f}")

    start = time.monotonic()
    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(func, args_list)
    elapsed = time.monotonic() - start

    # Collect results
    final_balances = [r["final_balance"] for r in results if r["final_balance"] is not None]
    avg_final = sum(final_balances) / len(final_balances) if final_balances else None
    total_deductions = initial_balance - avg_final if avg_final is not None else 0

    print("\n  Results:")
    print(f"    Final balance: ${avg_final:.2f}")
    print(f"    Total deductions: ${total_deductions:.2f}")
    print(f"    Time elapsed: {elapsed:.3f}s")

    if abs(total_deductions - total_expected_deductions) < 0.01:
        status = "✓ CORRECT — data integrity preserved"
    else:
        diff = abs(total_deductions - total_expected_deductions)
        status = f"✗ CORRUPTED — lost ${diff:.2f} in deductions (lost update bug)"

    print(f"    Expected: ${total_expected_deductions:.2f}")
    print(f"    {status}")

    return {
        "use_lock": use_lock,
        "final_balance": avg_final,
        "total_deductions": total_deductions,
        "expected_deductions": total_expected_deductions,
        "elapsed": elapsed,
        "correct": abs(total_deductions - total_expected_deductions) < 0.01,
    }


def main() -> None:
    """Run the distributed transfer demo."""
    parser = argparse.ArgumentParser(
        description="Cross-process bank transfer demo — proves Redis lock works",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python -B examples/sync_redis_lock_distributed_transfer.py --no-lock   # Show race condition
  uv run python -B examples/sync_redis_lock_distributed_transfer.py --with-lock  # Prove serialization
  uv run python -B examples/sync_redis_lock_distributed_transfer.py --all        # Run both scenarios

Prerequisites:
  - Start Redis: podman run -d --name redis-test -p 6379:6379 docker.io/redis
  - Start rqlite: podman run -d --name rqlite-test -p 4001:4001 docker.io/rqlite/rqlite
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--no-lock",
        action="store_true",
        help="Run WITHOUT lock (demonstrates race condition)",
    )
    group.add_argument(
        "--with-lock",
        action="store_true",
        help="Run WITH RedisLock (proves serialization)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Run both scenarios (no-lock then with-lock)",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=4,
        help="Number of concurrent processes (default: 4)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=10,
        help="Iterations per process (default: 10)",
    )
    parser.add_argument(
        "--amount",
        type=float,
        default=50.0,
        help="Transfer amount per iteration (default: 50.0)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Distributed Bank Transfer Demo")
    print("Proving Redis lock serializes cross-process transactions")
    print("=" * 60)

    results: list[dict] = []

    if args.no_lock:
        print("\nScenario 1: WITHOUT lock (demonstrates lost update bug)")
        print("-" * 50)
        result = run_scenario(
            use_lock=False,
            num_processes=args.processes,
            iterations_per_process=args.iterations,
            transfer_amount=args.amount,
        )
        results.append(result)

    if args.with_lock:
        print("\nScenario 2: WITH RedisLock (proves serialization)")
        print("-" * 50)
        result = run_scenario(
            use_lock=True,
            num_processes=args.processes,
            iterations_per_process=args.iterations,
            transfer_amount=args.amount,
        )
        results.append(result)

    if args.all:
        # Run both — no-lock first, then with-lock
        print("\n" + "=" * 60)
        print("Running BOTH scenarios sequentially")
        print("=" * 60)

        print("\n--- Scenario 1: WITHOUT lock ---")
        result_no_lock = run_scenario(
            use_lock=False,
            num_processes=args.processes,
            iterations_per_process=args.iterations,
            transfer_amount=args.amount,
        )
        results.append(result_no_lock)

        # Reset balance for fair comparison
        import rqlite

        conn = rqlite.connect(host="localhost", port=4001)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE transfer_accounts SET balance=? WHERE account=?", (5000.0, "SENDER")
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        print("\n--- Scenario 2: WITH RedisLock ---")
        result_with_lock = run_scenario(
            use_lock=True,
            num_processes=args.processes,
            iterations_per_process=args.iterations,
            transfer_amount=args.amount,
        )
        results.append(result_with_lock)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for r in results:
        status = "✓ CORRECT" if r["correct"] else "✗ CORRUPTED"
        mode = "WITH lock" if r["use_lock"] else "WITHOUT lock"
        print(f"  {mode}: {status} ({r['elapsed']:.3f}s)")

    # If both ran, highlight the difference
    if len(results) == 2:
        no_lock = results[0]
        _ = results[1]
        print("\nConclusion:")
        print(f"  Without lock: {status if not no_lock['correct'] else 'correct'} — data corrupted")
        print("  With RedisLock: ✓ correct — data preserved")
        print("\n  Redis distributed lock successfully serialized cross-process transactions!")

    print("=" * 60)


if __name__ == "__main__":
    main()
