"""Create a Polaris catalog backed by MinIO for the commit-mode spike.

Polaris runs in Docker (``apache/polaris``) and MinIO on the host. The catalog
uses static credentials (no STS on MinIO), ``endpointInternal`` for the server
and ``endpoint`` for the clients, and the root principal gets
``CATALOG_MANAGE_CONTENT``::

    docker run -d --name spike-polaris -p 8282:8181 -p 8283:8182 \\
      --add-host host.docker.internal:host-gateway \\
      -e POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,s3cr3t \\
      -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \\
      -e AWS_REGION=us-east-1 apache/polaris:latest
    python scripts/spike/polaris_setup.py [ALLOWED_LOCATION ...]   # default s3://spike/
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://127.0.0.1:8282"


def main() -> int:
    allowed = sys.argv[1:] or ["s3://spike/"]
    token = json.load(
        urllib.request.urlopen(
            urllib.request.Request(
                BASE + "/api/catalog/v1/oauth/tokens",
                data=urllib.parse.urlencode(
                    {
                        "grant_type": "client_credentials",
                        "client_id": "root",
                        "client_secret": "s3cr3t",
                        "scope": "PRINCIPAL_ROLE:ALL",
                    }
                ).encode(),
            )
        )
    )["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def call(method: str, path: str, body: object) -> None:
        request = urllib.request.Request(
            BASE + path, data=json.dumps(body).encode(), headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request) as response:
                print(method, path, response.status)
        except urllib.error.HTTPError as exc:
            print(method, path, exc.code, exc.read()[:300])

    call(
        "POST",
        "/api/management/v1/catalogs",
        {
            "catalog": {
                "name": "lake",
                "type": "INTERNAL",
                "properties": {"default-base-location": "s3://spike/warehouse"},
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": allowed,
                    "endpoint": "http://127.0.0.1:9000",
                    "endpointInternal": "http://host.docker.internal:9000",
                    "pathStyleAccess": True,
                    "stsUnavailable": True,
                },
            }
        },
    )
    call(
        "PUT",
        "/api/management/v1/principal-roles/service_admin/catalog-roles/lake",
        {"catalogRole": {"name": "catalog_admin"}},
    )
    call(
        "PUT",
        "/api/management/v1/catalogs/lake/catalog-roles/catalog_admin/grants",
        {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}},
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
