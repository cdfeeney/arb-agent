"""Emergency kill switch — touches data/STOP to halt all live order placement.

Usage:
    python -m scripts.stop                 # default reason
    python -m scripts.stop "BTC crash"     # custom reason

Effect: every executor's safety_gate() will refuse real sends until the
file is removed. Idempotent — overwrites if the file already exists.
"""

from __future__ import annotations

import sys

from src.exec.safety import DEFAULT_STOP_FILE, create_stop_file


def main() -> None:
    raw = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "manual stop"
    # Sanitize: collapse newlines so the reason can't smuggle multi-line
    # content into log streams; cap length so we don't write a novel.
    reason = raw.replace("\n", " ").replace("\r", " ").strip()[:200] or "manual stop"
    create_stop_file(reason)
    print(f"STOP file created at {DEFAULT_STOP_FILE}: {reason}")
    print("Remove with: python -m scripts.start")


if __name__ == "__main__":
    main()
