"""Concurrency: process lock.

Implements an exclusive per-table lock to prevent two simultaneous syncs on
the same machine: ``fcntl.flock`` on POSIX, ``msvcrt.locking`` on Windows. In
both cases the operating system releases the lock automatically if the process
dies (including SIGKILL).
"""

from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
import socket
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from ducklake_iceberg.errors import DuckLakeIcebergError

if sys.platform == "win32":
    import msvcrt

    #: byte locked by ``msvcrt.locking``: far past the metadata, so that a
    #: waiting process can still read the holder recorded at offset 0
    _WIN_LOCK_OFFSET: Final[int] = 0x7FFFFFFF

    def _try_lock(fd: int) -> None:
        os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            # a lock held by another process surfaces as EACCES/EDEADLK
            if exc.errno in (errno.EACCES, errno.EDEADLK):
                raise OSError(errno.EWOULDBLOCK, str(exc)) from exc
            raise

    def _unlock(fd: int) -> None:
        os.lseek(fd, _WIN_LOCK_OFFSET, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


LOG = logging.getLogger("ducklake_iceberg")

#: errno for "lock already held" on a non-blocking ``flock`` (Linux, BSD/macOS).
_HELD_ERRNOS: Final[frozenset[int]] = frozenset({errno.EAGAIN, errno.EWOULDBLOCK})
#: maximum size read from the lock file (diagnostics only).
_HOLDER_MAX_BYTES: Final[int] = 64 * 1024


# -- data structures -----------------------------------------------------------------


@dataclass(frozen=True)
class LockInfo:
    """Information about an acquired lock."""

    key: str
    path: str
    pid: int
    host: str
    user: str
    command: str
    acquired_at: datetime
    expires_at: datetime | None  # None for the local lock (flock)


# -- exceptions -------------------------------------------------------------------


class LockHeld(DuckLakeIcebergError):  # noqa: N818 - name is part of the public contract
    """Another process holds the table lock (exit 1)."""

    def __init__(self, message: str, *, info: LockInfo) -> None:
        super().__init__(message)
        self.info = info


class LockError(DuckLakeIcebergError):
    """Error while handling a lock."""


# -- state directory --------------------------------------------------------


def _lock_dir() -> Path:
    """Return the locks directory, creating it if necessary.

    Priority order:
    1. $DUCKLAKE_ICEBERG_STATE_DIR/locks
    2. $XDG_STATE_HOME/ducklake-iceberg/locks
    3. $HOME/.local/state/ducklake-iceberg/locks

    Returns:
        Path of the locks directory.

    Raises:
        LockError: If the directory cannot be created.
    """
    # 1. DUCKLAKE_ICEBERG_STATE_DIR
    state_dir = os.environ.get("DUCKLAKE_ICEBERG_STATE_DIR")
    if state_dir:
        lock_path = Path(state_dir) / "locks"
    else:
        # 2. XDG_STATE_HOME
        xdg_state = os.environ.get("XDG_STATE_HOME")
        if xdg_state:
            lock_path = Path(xdg_state) / "ducklake-iceberg" / "locks"
        else:
            # 3. HOME/.local/state (USERPROFILE on Windows, where HOME is unusual)
            home = os.environ.get("HOME") or (
                os.environ.get("USERPROFILE") if sys.platform == "win32" else None
            )
            if not home:
                raise LockError(
                    "None of DUCKLAKE_ICEBERG_STATE_DIR, XDG_STATE_HOME or HOME is "
                    "set; "
                    "cannot create the locks directory"
                )
            lock_path = Path(home) / ".local" / "state" / "ducklake-iceberg" / "locks"

    # Create the directory if it does not exist
    try:
        lock_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise LockError(f"Could not create {lock_path}: {e}") from e

    # Warn if on a network filesystem (heuristic)
    path_str = str(lock_path.absolute())
    if any(
        net_prefix in path_str
        for net_prefix in ("/mnt/", "/net/", "//", "\\\\", "/Volumes/")
    ):
        LOG.warning(
            "Locks directory on a network filesystem: %s. "
            "flock may not be exclusive. Set DUCKLAKE_ICEBERG_STATE_DIR "
            "to a local path.",
            lock_path,
        )

    return lock_path


# -- key generation -----------------------------------------------------------


def lock_key(*, catalog: str, identifier: tuple[str, ...]) -> str:
    """Generate the lock key for a published table.

    Args:
        catalog: stable description of the target catalog
            (``catalogs.catalog_key``).
        identifier: the table identifier, ``(*namespace, table)``.

    Returns:
        Truncated sha256 hash (16 chars).
    """
    key_str = "|".join((catalog, *identifier))
    digest = hashlib.sha256(key_str.encode()).hexdigest()
    return digest[:16]


# -- process lock -----------------------------------------------------------


@contextmanager
def table_lock(
    key: str,
    *,
    wait_s: float = 0.0,
    metadata: dict[str, object] | None = None,
    skip: bool = False,
) -> Iterator[LockInfo | None]:
    """Acquire an exclusive process lock for a table.

    The lock is a non-blocking exclusive lock (``flock``/``msvcrt.locking``)
    on a file in the state directory. The operating system releases it
    automatically when the process dies, including on SIGKILL.

    Args:
        key: Lock key (hash of profile|db.table).
        wait_s: Maximum wait time in seconds (0 = non-blocking).
        metadata: Optional metadata to write to the lock file.
        skip: If True, skip acquiring the lock (--no-lock).

    Yields:
        LockInfo for the acquired lock, or None if skip=True.

    Raises:
        LockHeld: Lock already held by another process.
        LockError: Error creating/opening the lock file.
    """
    if skip:
        LOG.warning("Lock disabled (--no-lock)")
        yield None
        return

    lock_dir = _lock_dir()
    lock_path = lock_dir / f"{key}.lock"

    # Open the lock file (create if missing)
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    except OSError as e:
        raise LockError(f"Error opening {lock_path}: {e}") from e

    # Holder information
    pid = os.getpid()
    host = socket.gethostname()
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
    command = " ".join(sys.argv)

    # Try to acquire, with optional wait (--wait) and 1 s backoff
    deadline = time.monotonic() + max(wait_s, 0.0)
    while True:
        try:
            _try_lock(fd)
            break
        except OSError as e:
            if e.errno not in _HELD_ERRNOS:
                os.close(fd)
                raise LockError(f"Error acquiring lock {lock_path}: {e}") from e
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                holder = _read_holder(fd, key=key, path=str(lock_path))
                os.close(fd)
                waited = f" after waiting {wait_s:g}s" if wait_s > 0 else ""
                raise LockHeld(
                    f"table locked by pid {holder.pid or '?'} on "
                    f"{holder.host} since {holder.acquired_at.isoformat()} "
                    f"({holder.command}){waited}",
                    info=holder,
                ) from None
            time.sleep(min(1.0, remaining))

    # Lock acquired: write metadata (the holder fields take precedence)
    acquired_at = datetime.now(UTC)
    try:
        lock_metadata = {
            **(metadata or {}),
            "pid": pid,
            "host": host,
            "user": user,
            "command": command,
            "acquired_at": acquired_at.isoformat(),
        }
        content_bytes = json.dumps(lock_metadata, indent=2, default=str).encode()
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, content_bytes)
        os.fsync(fd)
    except OSError as e:
        LOG.warning("Error writing lock metadata: %s", e)

    lock_info = LockInfo(
        key=key,
        path=str(lock_path),
        pid=pid,
        host=host,
        user=user,
        command=command,
        acquired_at=acquired_at,
        expires_at=None,
    )

    LOG.info("Lock acquired: %s (pid=%d, host=%s)", key, pid, host)

    try:
        yield lock_info
    finally:
        # Release the lock (close would also release it; explicit unlock is hygiene)
        try:
            _unlock(fd)
            LOG.info("Lock released: %s", key)
        except OSError as e:
            LOG.warning("Error releasing lock: %s", e)
        finally:
            os.close(fd)


def _read_holder(fd: int, *, key: str, path: str) -> LockInfo:
    """Best effort: read the holder recorded in the lock file.

    The content is for diagnostics only and may be empty, partial (holder
    killed mid-write) or corrupted; none of that may prevent a clean
    ``LockHeld``. Unreadable fields become neutral values.
    """
    raw: object = None
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        raw = json.loads(os.read(fd, _HOLDER_MAX_BYTES).decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        LOG.debug("lock file %s unreadable", path, exc_info=True)
    data = raw if isinstance(raw, dict) else {}

    def text(name: str) -> str:
        value = data.get(name)
        return value if isinstance(value, str) and value else "unknown"

    pid = data.get("pid")
    acquired_at = datetime.fromtimestamp(0, UTC)
    stamp = data.get("acquired_at")
    if isinstance(stamp, str):
        with suppress(ValueError):
            acquired_at = datetime.fromisoformat(stamp)
    return LockInfo(
        key=key,
        path=path,
        pid=pid if isinstance(pid, int) and not isinstance(pid, bool) else 0,
        host=text("host"),
        user=text("user"),
        command=text("command"),
        acquired_at=acquired_at,
        expires_at=None,
    )
