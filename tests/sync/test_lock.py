"""Per-table process lock."""

from __future__ import annotations

import errno
import fcntl
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from ducklake_iceberg.sync.lock import (
    LockError,
    LockHeld,
    _lock_dir,
    lock_key,
    table_lock,
)


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(tmp_path / "state"))
    return tmp_path / "state" / "locks"


def test_lock_key_is_stable_and_scoped() -> None:
    a = lock_key(profile=None, glue_database="db", glue_table="t")
    assert a == lock_key(profile="default", glue_database="db", glue_table="t")
    assert len(a) == 16
    assert a != lock_key(profile="prod", glue_database="db", glue_table="t")
    assert a != lock_key(profile=None, glue_database="db", glue_table="u")


def test_lock_dir_resolution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUCKLAKE_ICEBERG_STATE_DIR", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg"))
    assert _lock_dir() == tmp_path / "xdg" / "ducklake-iceberg" / "locks"

    monkeypatch.delenv("XDG_STATE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert (
        _lock_dir()
        == tmp_path / "home" / ".local" / "state" / "ducklake-iceberg" / "locks"
    )

    monkeypatch.delenv("HOME")
    with pytest.raises(LockError):
        _lock_dir()


def test_lock_dir_unwritable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", str(blocker))
    with pytest.raises(LockError, match="Could not create"), table_lock("k"):
        pass


def test_network_dir_warns(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("ducklake_iceberg.sync.lock.Path.mkdir", lambda *a, **k: None)
    monkeypatch.setenv("DUCKLAKE_ICEBERG_STATE_DIR", "/mnt/share/state")
    _lock_dir()
    assert "network" in caplog.text


def test_acquire_writes_holder_and_releases(state_dir: Path) -> None:
    with table_lock("k1", metadata={"table": "s.t", "pid": -5}) as info:
        assert info is not None
        assert info.pid == os.getpid()
        assert info.expires_at is None
        content = json.loads((state_dir / "k1.lock").read_text())
        assert content["table"] == "s.t"
        assert content["pid"] == os.getpid()  # holder fields take precedence
    # released: acquiring again works
    with table_lock("k1") as again:
        assert again is not None


def test_second_acquire_in_same_process_is_held(state_dir: Path) -> None:
    with (
        table_lock("k2", metadata={"table": "s.t"}),
        pytest.raises(LockHeld) as exc,
        table_lock("k2"),
    ):
        pass
    held = exc.value
    assert held.exit_code == 1
    assert held.info.pid == os.getpid()
    assert held.info.key == "k2"
    assert str(os.getpid()) in held.message


def test_released_on_exception(state_dir: Path) -> None:
    with pytest.raises(RuntimeError), table_lock("k3"):
        raise RuntimeError("failure")
    with table_lock("k3") as info:
        assert info is not None


def test_skip_yields_none(state_dir: Path, caplog: pytest.LogCaptureFixture) -> None:
    with table_lock("k4", skip=True) as info:
        assert info is None
    assert not state_dir.exists()
    assert "--no-lock" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"{not json",
        b"\xff\xfe",
        b"[1, 2]",
        json.dumps({"pid": "abc", "host": 3, "acquired_at": "yesterday"}).encode(),
        json.dumps({"pid": True, "acquired_at": 7}).encode(),
    ],
)
def test_corrupt_holder_file_still_raises_lockheld(
    state_dir: Path, content: bytes
) -> None:
    state_dir.mkdir(parents=True)
    path = state_dir / "k5.lock"
    path.write_bytes(content)
    fd = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(LockHeld) as exc, table_lock("k5"):
            pass
    finally:
        os.close(fd)
    info = exc.value.info
    assert info.pid == 0
    assert info.host == "unknown"
    assert info.acquired_at.year == 1970


def test_wait_times_out_with_holder_info(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [0.0]
    sleeps: list[float] = []

    def sleep(s: float) -> None:
        sleeps.append(s)
        now[0] += s

    monkeypatch.setattr("ducklake_iceberg.sync.lock.time.monotonic", lambda: now[0])
    monkeypatch.setattr("ducklake_iceberg.sync.lock.time.sleep", sleep)
    with (
        table_lock("k6"),
        pytest.raises(LockHeld) as exc,
        table_lock("k6", wait_s=2.5),
    ):
        pass
    assert sleeps == [1.0, 1.0, 0.5]
    assert exc.value.info.pid == os.getpid()
    assert "2.5s" in exc.value.message


def test_wait_acquires_when_released(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_flock = fcntl.flock
    calls = [0]

    def flaky(fd: int, op: int) -> None:
        calls[0] += 1
        if calls[0] == 1:
            raise OSError(errno.EWOULDBLOCK, "busy")
        real_flock(fd, op)

    monkeypatch.setattr("ducklake_iceberg.sync.lock.fcntl.flock", flaky)
    monkeypatch.setattr("ducklake_iceberg.sync.lock.time.sleep", lambda s: None)
    with table_lock("k7", wait_s=5) as info:
        assert info is not None
    assert calls[0] >= 2


def test_unexpected_flock_error(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(fd: int, op: int) -> None:
        raise OSError(errno.EBADF, "bad")

    monkeypatch.setattr("ducklake_iceberg.sync.lock.fcntl.flock", broken)
    with pytest.raises(LockError, match="Error acquiring"), table_lock("k8"):
        pass


def test_open_error(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*a: object, **k: object) -> int:
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr("ducklake_iceberg.sync.lock.os.open", fail)
    with pytest.raises(LockError, match="Error opening"), table_lock("k9"):
        pass


def test_metadata_write_error_is_only_a_warning(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fail(fd: int, length: int) -> None:
        raise OSError(errno.EIO, "io")

    monkeypatch.setattr("ducklake_iceberg.sync.lock.os.ftruncate", fail)
    with table_lock("k10") as info:
        assert info is not None
    assert "lock metadata" in caplog.text


def test_unlock_error_is_only_a_warning(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    real_flock = fcntl.flock

    def flock(fd: int, op: int) -> None:
        if op == fcntl.LOCK_UN:
            raise OSError(errno.EIO, "io")
        real_flock(fd, op)

    monkeypatch.setattr("ducklake_iceberg.sync.lock.fcntl.flock", flock)
    with table_lock("k11"):
        pass
    assert "releasing lock" in caplog.text


def test_lock_held_by_other_process_and_released_on_kill(state_dir: Path) -> None:
    """Holder in another process; SIGKILL releases the lock."""
    script = textwrap.dedent(
        """
        import sys, time
        from ducklake_iceberg.sync.lock import table_lock
        with table_lock("kp", metadata={"table": "s.t"}):
            print("ok", flush=True)
            time.sleep(60)
        """
    )
    proc = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        env={**os.environ},
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == b"ok"
        with pytest.raises(LockHeld) as exc, table_lock("kp"):
            pass
        assert exc.value.info.pid == proc.pid
    finally:
        proc.kill()
        proc.wait()
        if proc.stdout is not None:
            proc.stdout.close()
    with table_lock("kp") as info:
        assert info is not None
