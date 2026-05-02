"""Resume live order placement — removes data/STOP if present.

Usage:
    python -m scripts.start

Symmetric counterpart to scripts/stop.py. Does not start any process —
the running agent picks up the change on its next safety_gate() call.
"""

from __future__ import annotations

from src.exec.safety import DEFAULT_STOP_FILE, remove_stop_file


def main() -> None:
    if remove_stop_file():
        print(f"STOP file removed at {DEFAULT_STOP_FILE} — live sends re-enabled.")
    else:
        print(f"No STOP file at {DEFAULT_STOP_FILE} — nothing to do.")


if __name__ == "__main__":
    main()
