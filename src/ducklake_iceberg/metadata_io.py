"""Single-request metadata I/O; Parquet input keeps native ranged readers.

Only generated objects under the configured metadata prefix can be written.
Immutable outputs use conditional S3 commits rather than HEAD-then-PUT races.
"""

from __future__ import annotations

import io
import tempfile
from contextlib import suppress
from types import TracebackType
from typing import Any, NoReturn
from urllib.parse import urlsplit

from botocore.exceptions import ClientError
from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile, OutputStream

_PART_SIZE = 8 * 1024 * 1024
_MISSING = {"404", "NoSuchKey", "NotFound"}


def _translate(exc: ClientError, location: str) -> NoReturn:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    if code in _MISSING:
        raise FileNotFoundError(location) from None
    if code in {"403", "AccessDenied"}:
        raise PermissionError(location) from None
    if code in {"412", "PreconditionFailed"}:
        raise FileExistsError(f"{location} already exists") from None
    raise exc


class _Input(InputFile):
    def __init__(self, location: str, client: Any) -> None:
        super().__init__(location)
        self.client = client
        parsed = urlsplit(location)
        self.params = {"Bucket": parsed.netloc, "Key": parsed.path.lstrip("/")}
        self.size: int | None = None

    def __len__(self) -> int:
        if self.size is None:
            try:
                self.size = int(self.client.head_object(**self.params)["ContentLength"])
            except ClientError as exc:
                _translate(exc, self.location)
        assert self.size is not None
        return self.size

    def exists(self) -> bool:
        try:
            self.client.head_object(**self.params)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) in _MISSING:
                return False
            _translate(exc, self.location)
        return True

    def open(self, seekable: bool = True) -> InputStream:
        try:
            response = self.client.get_object(**self.params)
        except ClientError as exc:
            _translate(exc, self.location)
        # Only JSON/Avro metadata reaches this path, never source data bodies.
        with response["Body"] as body:
            contents = body.read()
        self.size = len(contents)
        return io.BytesIO(contents)


class _Stream(io.BufferedIOBase):
    def __init__(self, output: _Output, overwrite: bool) -> None:
        self.output, self.overwrite = output, overwrite
        self.file = tempfile.SpooledTemporaryFile(  # noqa: SIM115 - owned by this stream
            max_size=_PART_SIZE, mode="w+b"
        )
        self.commit = True

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        return self.file.write(data)

    def tell(self) -> int:
        return self.file.tell()

    def seek(self, offset: int, whence: int = 0) -> int:
        return self.file.seek(offset, whence)

    def flush(self) -> None:
        if not self.file.closed:
            self.file.flush()

    def _upload(self) -> None:
        client, params = self.output.client, self.output.params
        size = self.file.seek(0, 2)
        self.file.seek(0)
        condition = {} if self.overwrite else {"IfNoneMatch": "*"}
        if size <= _PART_SIZE:
            client.put_object(**params, Body=self.file.read(), **condition)
            return
        upload = client.create_multipart_upload(**params)["UploadId"]
        parts: list[dict[str, Any]] = []
        try:
            while chunk := self.file.read(_PART_SIZE):
                number = len(parts) + 1
                part = client.upload_part(
                    **params, UploadId=upload, PartNumber=number, Body=chunk
                )
                parts.append({"PartNumber": number, "ETag": part["ETag"]})
            client.complete_multipart_upload(
                **params, UploadId=upload, MultipartUpload={"Parts": parts}, **condition
            )
        except BaseException:
            with suppress(Exception):
                client.abort_multipart_upload(**params, UploadId=upload)
            raise

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self.commit:
                try:
                    self._upload()
                except ClientError as exc:
                    _translate(exc, self.output.location)
        finally:
            super().close()
            self.file.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.commit = False
        self.close()


class _Output(OutputFile):
    def __init__(self, location: str, client: Any) -> None:
        super().__init__(location)
        self.client = client
        self.input = _Input(location, client)
        self.params = self.input.params

    def __len__(self) -> int:
        return len(self.input)

    def exists(self) -> bool:
        return self.input.exists()

    def to_input_file(self) -> InputFile:
        return self.input

    def create(self, overwrite: bool = False) -> OutputStream:
        from ducklake_iceberg.handoff import POINTER_NAME

        # UUID-named writers historically requested overwrite to avoid a HEAD.
        # Conditional PUT makes even those generated objects immutable.
        replaceable = self.location.endswith("/" + POINTER_NAME)
        return _Stream(self, overwrite and replaceable)


class MetadataFileIO(FileIO):
    """One shared FileIO adapter: SDK metadata, native Parquet input."""

    tracks_sdk_calls = True

    def __init__(self, delegate: FileIO, client: Any, prefix: str) -> None:
        super().__init__(delegate.properties)
        self.delegate, self.client, self.prefix = (
            delegate,
            client,
            prefix.rstrip("/") + "/",
        )

    def new_input(self, location: str) -> InputFile:
        if location.startswith(self.prefix) and location.endswith((".json", ".avro")):
            return _Input(location, self.client)
        return self.delegate.new_input(location)

    def new_output(self, location: str) -> OutputFile:
        if not location.startswith(self.prefix):
            raise PermissionError("writes outside the metadata prefix are forbidden")
        return _Output(location, self.client)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        path = location if isinstance(location, str) else location.location
        if not path.startswith(self.prefix):
            raise PermissionError("deletes outside the metadata prefix are forbidden")
        self.client.delete_object(**_Input(path, self.client).params)


class _MemoryInput(InputFile):
    def __init__(self, location: str, contents: bytes) -> None:
        super().__init__(location)
        self.contents = contents

    def __len__(self) -> int:
        return len(self.contents)

    def exists(self) -> bool:
        return True

    def open(self, seekable: bool = True) -> InputStream:
        return io.BytesIO(self.contents)


class KnownMetadataFileIO(FileIO):
    """Registration-only view of an already committed immutable metadata object.

    Never cached across commands; unknown locations still use the shared FileIO.
    """

    def __init__(self, delegate: FileIO, location: str, contents: bytes) -> None:
        super().__init__(delegate.properties)
        from pyiceberg.serializers import Compressor

        self.delegate, self.location = delegate, location
        self.contents = Compressor.get_compressor(location).bytes_compressor()(contents)

    def new_input(self, location: str) -> InputFile:
        if location == self.location:
            return _MemoryInput(location, self.contents)
        return self.delegate.new_input(location)

    def new_output(self, location: str) -> OutputFile:
        return self.delegate.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        self.delegate.delete(location)
