#!/usr/bin/env python3
"""Decide the Codec Benchmark Regression check from its two worker jobs.

The first pass benchmarks the base and head commits on one runner and reports
whether it measured a regression through its ``needs_confirmation`` output. The
confirmation job re-measures on a separate runner and only runs when that
output is ``true``. This script is the required check: it passes only when the
first pass succeeded and either no confirmation was needed (and none ran) or
the separate-runner confirmation succeeded. Any other combination, including a
missing output or a cancelled worker, fails closed.

Inputs come from the environment: ``FIRST_PASS`` and ``CONFIRMATION`` are job
results (``success``, ``failure``, ``cancelled``, ``skipped``) and
``NEEDS_CONFIRMATION`` is the first pass's output.
"""

from __future__ import annotations

import os
import sys


def decide(first_pass: str, needs_confirmation: str, confirmation: str) -> tuple[bool, str]:
    """Return whether the gate passes and a one-line reason."""
    if first_pass != "success":
        return False, f"first pass did not succeed (result: {first_pass or 'missing'})"
    if needs_confirmation == "false":
        if confirmation != "skipped":
            return False, (
                "first pass measured no regression but the confirmation job "
                f"did not skip (result: {confirmation or 'missing'})"
            )
        return True, "no regression measured; confirmation not needed"
    if needs_confirmation == "true":
        if confirmation == "success":
            return True, "first-pass regression did not reproduce on a separate runner"
        return False, (
            "first-pass regression was not cleared by the separate-runner "
            f"confirmation (result: {confirmation or 'missing'})"
        )
    return False, (
        "first pass reported no confirmation decision "
        f"(needs_confirmation: {needs_confirmation or 'missing'})"
    )


def main() -> int:
    ok, reason = decide(
        os.environ.get("FIRST_PASS", ""),
        os.environ.get("NEEDS_CONFIRMATION", ""),
        os.environ.get("CONFIRMATION", ""),
    )
    print(f"Codec benchmark gate: {'PASS' if ok else 'FAIL'}: {reason}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
