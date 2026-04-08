"""Clear machine-level GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK for local dev.

Stub/placeholder Lambdas often return 409 {"error":"state conflict"}; the API can retry
via the in-repo handler when this flag is not set.

- **Startup** (`apply_local_governance_remediation_stub_409_policy`): skips clearing when
  ENV/GRAPHSUITE_ENV is production/prod, or when GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1.
- **Runtime** (`clear_disable_stub_409_on_placeholder_lambda_409`): when that exact stub
  response is seen, clear DISABLE so in-repo retry can run. PRESERVE only affects **startup**
  policy above, not this path (otherwise run.ps1 docs lead users to block runtime clear too).
  The Lambda proxy treats placeholder 409 by popping BLOCK+DISABLE unless
  GRAPHSUITE_STRICT_STUB_409_NO_LOCAL=1. BLOCK is still stripped at API startup (see
  apply_local_placeholder_409_block_env_policy) to tidy pytest leakage.
"""

from __future__ import annotations

import os

_KEY = "GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK"
_BLOCK_RUNTIME_CLEAR = "GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR"
_PRESERVE_BLOCK = "GRAPHSUITE_PRESERVE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR"
_PRESERVE_STRICT = "GRAPHSUITE_PRESERVE_STRICT_STUB_409_NO_LOCAL"
_STRICT_NO_LOCAL_STUB_409 = "GRAPHSUITE_STRICT_STUB_409_NO_LOCAL"


def is_strict_stub_409_no_local() -> bool:
    """True when local retry on placeholder 409 must be skipped (tests / explicit ops only)."""
    return str(os.getenv(_STRICT_NO_LOCAL_STUB_409, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def is_placeholder_409_clear_blocked() -> bool:
    """True when GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR prevents stub-409 pop/local retry."""
    return str(os.getenv(_BLOCK_RUNTIME_CLEAR, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _preserve_disable_stub_409() -> bool:
    return str(os.getenv("GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def clear_disable_stub_409_on_placeholder_lambda_409() -> None:
    """Clear DISABLE when we already know the Lambda returned the stub 409 body.

    In-repo RemediationConflictError does not use this exact message; safe to treat as
    placeholder. Does **not** consult GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK (that
    flag is for startup/run.ps1 only). Block clearing with GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR=1.
    """
    if is_placeholder_409_clear_blocked():
        return
    os.environ.pop(_KEY, None)


def apply_local_placeholder_409_block_env_policy() -> None:
    """Remove GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR unless PRESERVE_BLOCK is set.

    This flag exists for pytest / negative tests only. Laptops often set ENV=production while
    hitting stub Lambdas; skipping clears on that profile left BLOCK=true forever (see NDJSON
    placeholder_409_clear_blocked). Real prod should use PRESERVE_BLOCK if blocking is required.
    """
    block = (os.getenv(_BLOCK_RUNTIME_CLEAR) or "").strip()
    preserve = str(os.getenv(_PRESERVE_BLOCK, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if preserve:
        if block:
            print(
                f"[graphsuite] {_BLOCK_RUNTIME_CLEAR} is set; stub 409 runtime clear stays blocked "
                f"({_PRESERVE_BLOCK}=1).",
                flush=True,
            )
        return
    if block:
        print(
            f"[graphsuite] clearing {_BLOCK_RUNTIME_CLEAR} (pytest/local stub 409 path). "
            f"To keep blocking: {_PRESERVE_BLOCK}=1.",
            flush=True,
        )
    os.environ.pop(_BLOCK_RUNTIME_CLEAR, None)


def apply_local_strict_stub_409_env_policy() -> None:
    """Remove GRAPHSUITE_STRICT_STUB_409_NO_LOCAL unless PRESERVE_STRICT is set.

    Leftover from pytest/IDE launch configs forces stub_409_fallback_allowed=false with no BLOCK/DISABLE.
    """
    val = (os.getenv(_STRICT_NO_LOCAL_STUB_409) or "").strip()
    preserve = str(os.getenv(_PRESERVE_STRICT, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if preserve:
        if val:
            print(
                f"[graphsuite] {_STRICT_NO_LOCAL_STUB_409} is set; strict stub-409 opt-out preserved "
                f"({_PRESERVE_STRICT}=1).",
                flush=True,
            )
        return
    if val:
        print(
            f"[graphsuite] clearing {_STRICT_NO_LOCAL_STUB_409} for local dev (stub 409 -> in-repo retry). "
            f"To keep: {_PRESERVE_STRICT}=1.",
            flush=True,
        )
    os.environ.pop(_STRICT_NO_LOCAL_STUB_409, None)


def apply_local_governance_remediation_stub_409_policy() -> None:
    """Drop DISABLE_STUB_409 in local/staging dev so stub Lambda 409 can fall back in-process.

    Skips when ENV/GRAPHSUITE_ENV is **production** (prod/production only) or
    GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1.
    """
    dis = (os.getenv(_KEY) or "").strip().lower()
    preserve = _preserve_disable_stub_409()
    profile = (os.getenv("GRAPHSUITE_ENV") or os.getenv("ENV") or "").strip().lower()
    if profile in {"production", "prod"}:
        if dis in {"1", "true", "yes", "on"}:
            print(
                f"[graphsuite] {_KEY} is set; stub 409 in-repo retry disabled (production profile).",
                flush=True,
            )
        return
    if preserve:
        if dis in {"1", "true", "yes", "on"}:
            print(
                f"[graphsuite] {_KEY} is set; stub 409 in-repo retry disabled "
                "(GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1).",
                flush=True,
            )
        return
    if dis in {"1", "true", "yes", "on"}:
        print(
            f"[graphsuite] clearing {_KEY} for non-production dev (stub Lambda 409 -> in-repo handler). "
            "To keep disabling: GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1 or ENV=production.",
            flush=True,
        )
    os.environ.pop(_KEY, None)
