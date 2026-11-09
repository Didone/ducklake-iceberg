# syntax=docker/dockerfile:1.7
FROM python:3.12-slim-bookworm AS builder

ARG POETRY_VERSION=2.5.1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:${PATH}"

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"
RUN python -m venv /opt/venv
WORKDIR /src
COPY pyproject.toml poetry.lock README.md LICENSE ./
COPY src ./src
RUN poetry install --only main --no-root --sync \
 && poetry build -f wheel \
 && pip install --no-cache-dir --no-deps dist/*.whl

FROM python:3.12-slim-bookworm AS runtime

ARG VERSION=0.0.0
ARG REVISION=unknown
ARG SOURCE=unknown
ARG CREATED=unknown

LABEL org.opencontainers.image.title="ducklake-iceberg" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.created="${CREATED}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/home/ducklake-iceberg \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:${PATH}" \
    DUCKLAKE_ICEBERG_LOG_LEVEL=INFO \
    DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY=/home/ducklake-iceberg/.duckdb/extensions

RUN groupadd --gid 10001 ducklake-iceberg \
 && useradd --uid 10001 --gid 10001 --create-home \
      --shell /usr/sbin/nologin ducklake-iceberg

COPY --from=builder /opt/venv /opt/venv

USER 10001:10001
# Extensions are embedded in $HOME/.duckdb/extensions/v<duckdb>/<platform>/; the
# DUCKLAKE_ICEBERG_DUCKDB_EXTENSION_DIRECTORY setting (above) points to the same place,
# which keeps LOAD working with a different or read-only HOME.
RUN python -c "import duckdb; c = duckdb.connect(); \
[c.execute(f'INSTALL {e}') for e in ('ducklake', 'postgres', 'httpfs', 'aws')]; \
print(c.execute(\"SELECT extension_name, installed FROM duckdb_extensions() \
WHERE extension_name IN ('ducklake', 'postgres_scanner', 'httpfs', 'aws') \
ORDER BY extension_name\").fetchall())"

WORKDIR /home/ducklake-iceberg
RUN --network=none python -c "import duckdb; \
c = duckdb.connect(':memory:', config={'autoinstall_known_extensions': False, \
'autoload_known_extensions': False}); \
[c.execute(f'LOAD {e}') for e in ('ducklake', 'postgres', 'httpfs', 'aws')]; \
loaded = {r[0] for r in c.execute('SELECT extension_name FROM duckdb_extensions() \
WHERE loaded').fetchall()}; \
assert {'ducklake', 'postgres_scanner', 'httpfs', 'aws'} <= loaded, loaded; \
print(sorted(loaded))"
# Pinned versions: DuckDB and PyIceberg come from poetry.lock (== in
# pyproject), but the core repository may replace an extension build without
# changing the DuckDB version. The build fails if the hash of any extension (or
# the duckdb/pyiceberg version) differs from ducklake_iceberg.deps.
RUN --network=none python -m ducklake_iceberg.deps
RUN --network=none ducklake-iceberg --version && ducklake-iceberg doctor --help >/dev/null

ENTRYPOINT ["ducklake-iceberg"]
CMD ["--help"]
