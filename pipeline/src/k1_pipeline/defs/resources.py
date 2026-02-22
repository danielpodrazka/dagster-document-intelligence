"""
S3Storage — Dagster ConfigurableResource wrapping boto3 for S3 I/O.

Defaults target LocalStack for local development.
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field, PrivateAttr


class S3Storage(ConfigurableResource):
    """S3-backed storage resource with LocalStack defaults."""

    bucket_name: str = Field(default_factory=lambda: os.environ.get("S3_BUCKET_NAME", "dagster-document-intelligence-etl"))
    endpoint_url: str = Field(default_factory=lambda: os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"))
    region_name: str = Field(default_factory=lambda: os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
    aws_access_key_id: str = Field(default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    aws_secret_access_key: str = Field(default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))

    _client_instance: Any = PrivateAttr(default=None)

    def _client(self):
        if self._client_instance is None:
            import boto3

            self._client_instance = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        return self._client_instance

    # -- read helpers ----------------------------------------------------------

    def read_bytes(self, key: str) -> bytes:
        resp = self._client().get_object(Bucket=self.bucket_name, Key=key)
        return resp["Body"].read()

    def read_text(self, key: str) -> str:
        return self.read_bytes(key).decode("utf-8")

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_text(key))

    # -- write helpers ---------------------------------------------------------

    def write_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self._client().put_object(
            Bucket=self.bucket_name, Key=key, Body=data, ContentType=content_type,
        )

    def write_text(self, key: str, text: str, content_type: str = "text/plain") -> None:
        self.write_bytes(key, text.encode("utf-8"), content_type=content_type)

    def write_json(self, key: str, data: Any) -> None:
        self.write_text(key, json.dumps(data, indent=2), content_type="application/json")

    # -- listing / existence ---------------------------------------------------

    def list_objects(self, prefix: str, suffix: str = "") -> list[str]:
        client = self._client()
        keys: list[str] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if not suffix or k.endswith(suffix):
                    keys.append(k)
        return sorted(keys)

    def exists(self, key: str) -> bool:
        try:
            self._client().head_object(Bucket=self.bucket_name, Key=key)
            return True
        except self._client().exceptions.NoSuchKey:
            return False
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") == "404":
                return False
            raise

    # -- copy / move / delete --------------------------------------------------

    def copy_object(self, source_key: str, dest_key: str) -> None:
        self._client().copy_object(
            Bucket=self.bucket_name,
            CopySource={"Bucket": self.bucket_name, "Key": source_key},
            Key=dest_key,
        )

    def move_object(self, source_key: str, dest_key: str) -> None:
        self.copy_object(source_key, dest_key)
        self.delete_object(source_key)

    def delete_object(self, key: str) -> None:
        self._client().delete_object(Bucket=self.bucket_name, Key=key)

    # -- file transfer helpers -------------------------------------------------

    def download_to_tempfile(self, key: str, suffix: str = "") -> str:
        data = self.read_bytes(key)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(data)
        tmp.close()
        return tmp.name

    def upload_from_file(self, local_path: str, key: str, content_type: str = "application/octet-stream") -> None:
        with open(local_path, "rb") as f:
            self.write_bytes(key, f.read(), content_type=content_type)

    # -- key builders ----------------------------------------------------------

    def staging_key(self, run_id: str, filename: str) -> str:
        if run_id:
            return f"staging/{run_id}/{filename}"
        return f"staging/{filename}"

    def output_key(self, dirname: str, filename: str) -> str:
        return f"output/{dirname}/{filename}"

    def input_key(self, filename: str) -> str:
        return f"input/{filename}"


@dg.definitions
def resources():
    return dg.Definitions(resources={"s3": S3Storage()})
