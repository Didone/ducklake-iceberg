"""Object stores (S3 via moto, local), guards, and safety aborts."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO

from ducklake_iceberg import s3
from ducklake_iceberg.errors import ExitCode
from ducklake_iceberg.maintenance import (
    CleanError,
    OrphanKind,
    Reachable,
    clean_table,
    compute_reachable,
    find_orphans,
    reachable_from,
    validate_grace,
)
from ducklake_iceberg.maintenance.orphans import CleanReport, OrphanCandidate
from ducklake_iceberg.s3 import (
    LocalStore,
    PrefixGuardError,
    PrefixOverlapError,
    S3Object,
    S3Store,
    StorageError,
    canonical_uri,
    check_parquet_in_prefix,
    classify_object,
    is_under,
    object_store_for,
    validate_prefix_guards,
)

REGION = "us-east-1"
NOW = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
DAY = timedelta(days=1)
KEY = "0123456789abcdef01234567"
UUID = "00000000-0000-0000-0000-000000000000"


# -- paths and classification ---------------------------------------------------


def test_canonical_and_is_under() -> None:
    assert canonical_uri("file:///srv//x/./y/") == "/srv/x/y"
    assert canonical_uri("S3://b//p/./k") == "s3://b/p/k"
    assert is_under("s3://b/p/t/metadata/v1.metadata.json", "s3://b/p/t")
    assert not is_under("s3://b/p/t2/v1.metadata.json", "s3://b/p/t")
    assert is_under("/a/b", "/a/b/")


@pytest.mark.parametrize(
    ("key", "kind"),
    [
        ("s3://b/t/metadata/v3.metadata.json", OrphanKind.METADATA_JSON),
        ("s3://b/t/metadata/snap-1-1-x.avro", OrphanKind.MANIFEST_LIST),
        ("s3://b/t/metadata/x-m0.avro", OrphanKind.MANIFEST),
        (f"s3://b/t/deletes/{KEY}-{UUID}-deletes.parquet", OrphanKind.POSITION_DELETE),
        ("s3://b/t/data/x.parquet", OrphanKind.UNKNOWN),
        ("s3://b/t/_ducklake_iceberg_last_export.json", OrphanKind.UNKNOWN),
        ("s3://b/t/_locks/sync.lock.json", OrphanKind.UNKNOWN),
        ("/x/t/metadata/v1.metadata.json.crc", OrphanKind.UNKNOWN),
    ],
)
def test_classify_object(key: str, kind: OrphanKind) -> None:
    assert classify_object(key) is kind


def _obj(key: str, age: timedelta = 2 * DAY, size: int = 10) -> S3Object:
    return S3Object(key=key, size_bytes=size, last_modified=NOW - age)


def test_check_parquet_ignores_position_deletes() -> None:
    objs = [
        _obj(f"s3://b/t/deletes/{KEY}-{UUID}-deletes.parquet"),
        _obj("s3://b/t/data/x.parquet"),
        _obj("s3://b/t/metadata/v1.metadata.json"),
    ]
    assert check_parquet_in_prefix(objs) == ["s3://b/t/data/x.parquet"]


# -- guards ---------------------------------------------------------------------


def test_prefix_guards_pass() -> None:
    validate_prefix_guards(
        prefix="s3://b/iceberg/db/t",
        metadata_location="s3://b/iceberg/db/t/metadata/v1.metadata.json",
        data_path="s3://b/data/",
    )


def test_guard_g1_other_table() -> None:
    with pytest.raises(PrefixGuardError, match="another table") as exc:
        validate_prefix_guards(
            prefix="s3://b/iceberg/db/t",
            metadata_location="s3://b/iceberg/db/t2/metadata/v1.metadata.json",
            data_path="s3://b/data/",
        )
    assert exc.value.exit_code is ExitCode.ERROR


@pytest.mark.parametrize(
    "data_path", ["s3://b/iceberg/", "s3://b/iceberg/db/t/metadata", "s3://b/iceberg"]
)
def test_guard_g2_overlap(data_path: str) -> None:
    with pytest.raises(PrefixOverlapError, match="must not overlap") as exc:
        validate_prefix_guards(
            prefix="s3://b/iceberg/db/t",
            metadata_location="s3://b/iceberg/db/t/metadata/v1.metadata.json",
            data_path=data_path,
        )
    assert exc.value.exit_code is ExitCode.BLOCKED


def test_guard_g2_uses_segment_boundary() -> None:
    # "s3://b/ice" does not contain "s3://b/iceberg/...": string prefix only
    validate_prefix_guards(
        prefix="s3://b/iceberg/db/t",
        metadata_location="s3://b/iceberg/db/t/metadata/v1.metadata.json",
        data_path="s3://b/ice",
    )


def test_guard_g3_root() -> None:
    with pytest.raises(PrefixGuardError, match="root"):
        validate_prefix_guards(
            prefix="s3://b/t",
            metadata_location="s3://b/t/metadata/v1.metadata.json",
            data_path="s3://other/data",
        )
    with pytest.raises(PrefixGuardError, match="root"):
        validate_prefix_guards(
            prefix="gs://b/t",
            metadata_location="gs://b/t/v1.metadata.json",
            data_path="s3://other/data",
        )


@pytest.mark.parametrize(
    ("grace", "ok"),
    [(timedelta(hours=1), True), (DAY, True), (timedelta(minutes=59), False)],
)
def test_validate_grace(grace: timedelta, ok: bool) -> None:
    if ok:
        validate_grace(grace)
    else:
        with pytest.raises(CleanError, match="floor"):
            validate_grace(grace)


# -- find_orphans ---------------------------------------------------------------


def test_find_orphans_decisions() -> None:
    reachable = Reachable(
        metadata_files=frozenset({"s3://b/t/metadata/v1.metadata.json"}),
        manifest_lists=frozenset(),
        manifests=frozenset(),
        missing=frozenset(),
        delete_files=frozenset({f"s3://b/t/deletes/{KEY}-{UUID}-deletes.parquet"}),
    )
    objs = [
        _obj("s3://b//t/metadata/v1.metadata.json"),  # reachable (canonical)
        _obj(f"s3://b/t/deletes/{KEY}-{UUID}-deletes.parquet"),
        _obj("s3://b/t/metadata/v2.metadata.json"),
        _obj("s3://b/t/metadata/v3.metadata.json", age=timedelta(minutes=10)),
        _obj("s3://b/t/metadata/v4.metadata.json", age=-DAY),
        _obj("s3://b/t/_ducklake_iceberg_last_export.json", age=10 * DAY),
    ]
    out = find_orphans(objs, reachable, grace=DAY, now=NOW)
    by_name = {c.obj.key.rsplit("/", 1)[-1]: c for c in out}
    assert by_name["v1.metadata.json"].reason == "reachable"
    assert not by_name[f"{KEY}-{UUID}-deletes.parquet"].deletable
    assert by_name["v2.metadata.json"].deletable
    assert by_name["v3.metadata.json"].reason.startswith("within the grace period")
    assert "future" in by_name["v4.metadata.json"].reason
    assert not by_name["_ducklake_iceberg_last_export.json"].deletable
    assert out[0].obj.key.endswith("_ducklake_iceberg_last_export.json")


# -- S3Store (moto) -------------------------------------------------------------


@pytest.fixture
def s3_client() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(Bucket="bkt")
        yield client


def test_s3_store_lists_recursively_and_deletes_in_batches(
    s3_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    for key in ("p/t/metadata/v1.metadata.json", "p/t/deletes/x.parquet", "p/t2/z"):
        s3_client.put_object(Bucket="bkt", Key=key, Body=b"abc")
    store = S3Store(s3_client)
    objs = list(store.list_objects("s3://bkt/p/t"))
    assert sorted(o.key for o in objs) == [
        "s3://bkt/p/t/deletes/x.parquet",
        "s3://bkt/p/t/metadata/v1.metadata.json",
    ]
    assert all(o.size_bytes == 3 and o.last_modified.tzinfo for o in objs)

    monkeypatch.setattr(s3, "DELETE_BATCH", 1)
    deleted, failed = store.delete_objects([o.key for o in objs])
    assert sorted(deleted) == sorted(o.key for o in objs)
    assert failed == []
    assert [o.key for o in store.list_objects("s3://bkt/p")] == ["s3://bkt/p/t2/z"]
    assert list(store.list_objects("s3://bkt")) != []


def test_s3_store_list_error(s3_client: Any) -> None:
    with pytest.raises(StorageError, match="failed to list"):
        list(S3Store(s3_client).list_objects("s3://nope/p/t"))


class _FailingS3:
    def __init__(self, *, raise_on_delete: bool) -> None:
        self.raise_on_delete = raise_on_delete

    def delete_objects(self, **kwargs: Any) -> dict[str, Any]:
        if self.raise_on_delete:
            raise EndpointConnectionError(endpoint_url="https://s3")
        keys = [o["Key"] for o in kwargs["Delete"]["Objects"]]
        return {
            "Deleted": [{"Key": keys[0]}],
            "Errors": [
                {"Key": k, "Code": "AccessDenied", "Message": "no"} for k in keys[1:]
            ],
        }


def test_s3_store_delete_partial_and_transport_errors() -> None:
    deleted, failed = S3Store(_FailingS3(raise_on_delete=False)).delete_objects(
        ["s3://b/a", "s3://b/c"]
    )
    assert deleted == ["s3://b/a"]
    assert failed == [("s3://b/c", "AccessDenied: no")]
    deleted, failed = S3Store(_FailingS3(raise_on_delete=True)).delete_objects(
        ["s3://b/a"]
    )
    assert deleted == []
    assert failed[0][0] == "s3://b/a"


def test_object_store_for(s3_client: Any) -> None:
    assert isinstance(object_store_for("s3://bkt/p", region=REGION), S3Store)
    assert isinstance(object_store_for("/srv/p", region=REGION), LocalStore)
    assert isinstance(object_store_for("file:///srv/p", region=REGION), LocalStore)
    with pytest.raises(PrefixGuardError, match="unsupported scheme"):
        object_store_for("gs://b/p", region=REGION)


# -- LocalStore -----------------------------------------------------------------


def test_local_store(tmp_path: Path) -> None:
    store = LocalStore()
    assert list(store.list_objects(str(tmp_path / "missing"))) == []
    (tmp_path / "f").write_text("x")
    with pytest.raises(StorageError, match="is not a directory"):
        list(store.list_objects(str(tmp_path / "f")))
    nested = tmp_path / "t" / "metadata"
    nested.mkdir(parents=True)
    (nested / "v1.metadata.json").write_text("{}")
    objs = list(store.list_objects(f"file://{tmp_path / 't'}"))
    assert [Path(o.key).name for o in objs] == ["v1.metadata.json"]
    deleted, failed = store.delete_objects([objs[0].key, str(nested / "nope")])
    assert deleted == [objs[0].key]
    assert failed[0][0] == str(nested / "nope")


# -- reachable set --------------------------------------------------------------


def _write_metadata(path: Path, *, log: list[str] = (), lists: list[str] = ()) -> str:  # type: ignore[assignment]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "format-version": 2,
                "metadata-log": [{"metadata-file": m, "timestamp-ms": 0} for m in log],
                "snapshots": [{"manifest-list": m} for m in lists],
            }
        )
    )
    return str(path)


def test_reachable_from_follows_log_and_flags_missing(tmp_path: Path) -> None:
    io = PyArrowFileIO()
    v1 = _write_metadata(tmp_path / "metadata" / "v1.metadata.json")
    v2 = _write_metadata(
        tmp_path / "metadata" / "v2.metadata.json",
        log=[v1, str(tmp_path / "metadata" / "v0.metadata.json")],
        lists=[str(tmp_path / "metadata" / "snap-1-1-x.avro")],
    )
    reachable = reachable_from(io, f"file://{v2}", [v1])
    assert reachable.metadata_files == {v1, v2}
    assert reachable.metadata_location == v2
    assert reachable.missing == {
        str(tmp_path / "metadata" / "v0.metadata.json"),
        str(tmp_path / "metadata" / "snap-1-1-x.avro"),
    }


def test_reachable_from_reads_log_one_level_deep(tmp_path: Path) -> None:
    """A version outside the root's log is unreachable, even if cited by another."""
    io = PyArrowFileIO()
    meta = tmp_path / "metadata"
    v1 = _write_metadata(meta / "v1.metadata.json", lists=[str(meta / "snap-1.avro")])
    v2 = _write_metadata(meta / "v2.metadata.json", log=[v1])
    v3 = _write_metadata(meta / "v3.metadata.json", log=[v2])
    reachable = reachable_from(io, v3)
    assert reachable.metadata_files == {v2, v3}
    assert reachable.missing == set()  # snap-1.avro of v1 is not even looked up
    # v2 as an extra root (previous_metadata_location) expands its own log
    assert reachable_from(io, v3, [v2]).metadata_files == {v1, v2, v3}


def test_reachable_from_aborts_on_unreadable(tmp_path: Path) -> None:
    io = PyArrowFileIO()
    garbage_list = tmp_path / "metadata" / "snap-1-1-x.avro"
    garbage_list.parent.mkdir(parents=True)
    garbage_list.write_bytes(b"not avro")
    root = _write_metadata(
        tmp_path / "metadata" / "v1.metadata.json", lists=[str(garbage_list)]
    )
    with pytest.raises(CleanError, match=r"manifest list .* unreadable"):
        reachable_from(io, root)

    not_object = tmp_path / "metadata" / "v2.metadata.json"
    not_object.write_text("[]")
    with pytest.raises(CleanError, match=r"metadata\.json .* unreadable"):
        reachable_from(io, str(not_object))

    with pytest.raises(CleanError, match="missing or unreadable"):
        reachable_from(io, str(tmp_path / "metadata" / "v404.metadata.json"))


class _DeniedIO(PyArrowFileIO):
    def new_input(self, location: str) -> Any:
        raise PermissionError("AccessDenied")


def test_reachable_from_permission_error_never_means_orphan(tmp_path: Path) -> None:
    with pytest.raises(CleanError, match="PermissionError"):
        reachable_from(_DeniedIO(), str(tmp_path / "v1.metadata.json"))


# -- Glue and clean_table -------------------------------------------------------


class _FakeGlueClient:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = responses

    def get_table(self, **_kwargs: Any) -> dict[str, Any]:
        item = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(item, Exception):
            raise item
        return {"Table": {"Parameters": item}}


class _FakeCatalog:
    def __init__(self, *responses: Any) -> None:
        self.glue = _FakeGlueClient(list(responses))


def _catalog(*responses: Any) -> GlueCatalog:
    return cast("GlueCatalog", _FakeCatalog(*responses))


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "m"}}, "GetTable")


@pytest.mark.parametrize(
    ("response", "match"),
    [
        (_client_error("EntityNotFoundException"), "missing from Glue"),
        (_client_error("AccessDeniedException"), "AccessDeniedException"),
        (EndpointConnectionError(endpoint_url="https://glue"), "failed"),
        ({"table_type": "ICEBERG"}, "has no metadata_location"),
    ],
)
def test_compute_reachable_glue_errors(response: Any, match: str) -> None:
    with pytest.raises(CleanError, match=match):
        compute_reachable(_catalog(response), PyArrowFileIO(), database="db", table="t")


def _table_dir(tmp_path: Path) -> tuple[Path, str]:
    location = tmp_path / "ice" / "db" / "t"
    root = _write_metadata(location / "metadata" / "v1.metadata.json")
    return location, root


def _clean(tmp_path: Path, *responses: Any, **kw: Any) -> CleanReport:
    location, _root = _table_dir(tmp_path)
    params: dict[str, Any] = {
        "glue_identifier": ("db", "t"),
        "prefix": str(location),
        "data_path": str(tmp_path / "data"),
        "now": datetime.now(UTC) + 3 * DAY,
        **kw,
    }
    return clean_table(_catalog(*responses), PyArrowFileIO(), LocalStore(), **params)


def test_compute_reachable_with_previous(tmp_path: Path) -> None:
    location, root = _table_dir(tmp_path)
    previous = _write_metadata(location / "metadata" / "v0.metadata.json")
    reachable = compute_reachable(
        _catalog({"metadata_location": root, "previous_metadata_location": previous}),
        PyArrowFileIO(),
        database="db",
        table="t",
    )
    assert reachable.metadata_files == {root, previous}


def test_clean_table_apply_and_concurrent_commit(tmp_path: Path) -> None:
    location, root = _table_dir(tmp_path)
    orphan = location / "metadata" / "v9.metadata.json"
    orphan.write_text("{}")
    report = _clean(tmp_path, {"metadata_location": root})
    assert report.dry_run
    assert [Path(c.obj.key).name for c in report.deletable] == ["v9.metadata.json"]
    assert report.to_json()["deletable"] == 1

    # concurrent commit between the scan and --apply: aborts without deleting
    moved = {"metadata_location": str(location / "metadata" / "v2.metadata.json")}
    with pytest.raises(CleanError, match="concurrent commit"):
        _clean(tmp_path, {"metadata_location": root}, moved, apply=True)
    assert orphan.exists()

    report = _clean(tmp_path, {"metadata_location": root}, apply=True)
    assert report.deleted == (str(orphan),)
    assert report.bytes_reclaimed == 2
    assert not orphan.exists()


def test_clean_table_reports_failed_deletes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    location, root = _table_dir(tmp_path)
    (location / "metadata" / "v9.metadata.json").write_text("{}")
    monkeypatch.setattr(
        LocalStore,
        "delete_objects",
        lambda self, keys: ([], [(k, "denied") for k in keys]),
    )
    report = _clean(tmp_path, {"metadata_location": root}, apply=True)
    assert report.deleted == ()
    assert report.failed[0][1] == "denied"
    assert report.to_json()["failed"] == [
        {"key": report.failed[0][0], "error": "denied"}
    ]


def test_clean_table_aborts(tmp_path: Path) -> None:
    location, root = _table_dir(tmp_path)
    with pytest.raises(CleanError, match="max-deletes"):
        _clean(tmp_path, {"metadata_location": root}, max_deletes=0)
    broken = _write_metadata(
        location / "metadata" / "v2.metadata.json",
        lists=[str(location / "metadata" / "snap-404.avro")],
    )
    with pytest.raises(CleanError, match="do not exist"):
        _clean(tmp_path, {"metadata_location": broken})
    with pytest.raises(PrefixOverlapError):
        _clean(tmp_path, {"metadata_location": root}, data_path=str(location))


def test_candidate_dataclass_roundtrip() -> None:
    c = OrphanCandidate(
        obj=_obj("s3://b/t/x.avro"),
        kind=OrphanKind.MANIFEST,
        age=DAY,
        deletable=True,
        reason="r",
    )
    report = CleanReport(
        prefix="s3://b/t",
        grace=DAY,
        scanned=1,
        reachable=0,
        candidates=(c,),
        deleted=(),
        bytes_reclaimed=0,
        dry_run=True,
        truncated=False,
    )
    doc = report.to_json()
    assert doc["grace_s"] == 86400
    assert doc["candidates"] == [
        {
            "key": "s3://b/t/x.avro",
            "kind": "manifest",
            "size_bytes": 10,
            "last_modified": c.obj.last_modified.isoformat(),
            "age_s": 86400,
            "deletable": True,
            "reason": "r",
        }
    ]
