"""Shared utilities for xls2pdf Lambda handlers."""

import base64
import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger("xls2pdf")
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")
SSM_CLIENT = boto3.client("ssm")

PDF_CONTENT_TYPE = "application/pdf"
XLSX_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
SUPPORTED_INPUT_FORMAT = "xlsx"
RESULT_OBJECT_NAME = "result"


class ConversionError(Exception):
    """Raised when an operation within the conversion pipeline fails."""


def get_default_bucket() -> str:
    bucket = os.getenv("DEFAULT_TARGET_BUCKET")
    if not bucket:
        raise ConversionError("DEFAULT_TARGET_BUCKET is not configured")
    return bucket


def calculate_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sanitize_filename(raw: Optional[str]) -> str:
    if not raw:
        return ""
    candidate = Path(raw).name
    if not candidate:
        return ""
    if not candidate.lower().endswith(f".{SUPPORTED_INPUT_FORMAT}"):
        raise ConversionError(f"Filename must end with .{SUPPORTED_INPUT_FORMAT}: '{raw}'")
    return candidate


def load_workbook_bytes(data: str, *, default_bucket: Optional[str]) -> Tuple[bytes, str]:
    if data.startswith("s3://"):
        bucket, key = parse_s3_uri(data)
        LOGGER.info("Downloading workbook from %s", data)
        try:
            response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            raise ConversionError(f"Unable to read source object {data}") from error
        workbook_bytes = response["Body"].read()
        if not workbook_bytes:
            raise ConversionError("Workbook content is empty")
        return workbook_bytes, data

    if data.startswith("/") and default_bucket:
        return load_workbook_bytes(f"s3://{default_bucket}{data}", default_bucket=default_bucket)

    try:
        workbook_bytes = base64.b64decode(data)
    except (ValueError, TypeError) as exc:
        raise ConversionError("Workbook payload is not valid base64 content") from exc

    if not workbook_bytes:
        raise ConversionError("Workbook content is empty")

    return workbook_bytes, "inline"


def write_temp_workbook(contents: bytes) -> str:
    if not contents:
        raise ConversionError("Workbook content is empty")

    with tempfile.NamedTemporaryFile(prefix="xls2pdf", suffix=".xlsx", delete=False) as handle:
        handle.write(contents)
        temp_path = handle.name
    return temp_path


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    prefix = "s3://"
    if not uri.startswith(prefix):
        raise ConversionError(f"Invalid S3 URI: {uri}")

    remainder = uri[len(prefix) :]
    if "/" not in remainder:
        bucket, key = remainder, ""
    else:
        bucket, key = remainder.split("/", 1)

    if not bucket:
        raise ConversionError(f"Invalid S3 URI: {uri}")
    return bucket, key


def build_object_key(file_hash: str, filename: str) -> str:
    return join_key(file_hash, filename)


def join_key(prefix: str, name: str) -> str:
    prefix = prefix.strip("/")
    if not prefix:
        return name
    return f"{prefix}/{name}"


def prefix_for_key(key: str) -> str:
    parent = str(PurePosixPath(key).parent)
    return "" if parent == "." else parent


def delete_prefix_contents(bucket: str, prefix: str) -> None:
    prefix = prefix.strip("/")
    if not prefix:
        LOGGER.warning("Refusing to delete entire bucket '%s'; prefix is empty", bucket)
        return

    paginator = S3_CLIENT.get_paginator("list_objects_v2")
    full_prefix = f"{prefix}/"

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
            keys = [item["Key"] for item in page.get("Contents", []) if item.get("Key")]
            if not keys:
                continue
            _delete_object_batch(bucket, keys)
    except ClientError as error:
        raise ConversionError(
            f"Unable to prune existing objects under prefix '{full_prefix}' in bucket '{bucket}'"
        ) from error


def _delete_object_batch(bucket: str, keys: Iterable[str]) -> None:
    chunk: List[str] = []
    for key in keys:
        chunk.append(key)
        if len(chunk) == 1000:
            _submit_delete_batch(bucket, chunk)
            chunk = []
    if chunk:
        _submit_delete_batch(bucket, chunk)


def _submit_delete_batch(bucket: str, keys: Iterable[str]) -> None:
    try:
        S3_CLIENT.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in keys]},
        )
    except ClientError as error:
        raise ConversionError(
            f"Unable to delete objects from bucket '{bucket}' while cleaning prefix"
        ) from error


def find_original_object(bucket: str, lookup_key: str) -> Optional[str]:
    prefix = join_key(lookup_key, "")
    paginator = S3_CLIENT.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key:
                continue
            name = PurePosixPath(key).name.lower()
            if name.endswith(f".{SUPPORTED_INPUT_FORMAT}"):
                return key
    return None


def object_exists(bucket: str, key: str) -> bool:
    try:
        S3_CLIENT.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in {"404", "NotFound"}:
            return False
        raise


def read_binary_object(bucket: str, key: str) -> bytes:
    try:
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        raise ConversionError(f"Unable to read object s3://{bucket}/{key}") from error
    return response["Body"].read()


def read_text_object(bucket: str, key: str) -> str:
    return read_binary_object(bucket, key).decode("utf-8")


def extract_api_payload(event: Dict[str, Any], *, optional: bool = False) -> Dict[str, Any]:
    if "body" not in event or event["body"] is None:
        if optional:
            return {}
        raise ConversionError("API event missing body")

    body = event["body"]
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ConversionError("Request body is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise ConversionError("Request body must decode to an object")

    return payload


def json_response(body: Dict[str, Any], *, status_code: int = 200) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
        "headers": {"Content-Type": "application/json"},
    }


def pdf_response(pdf_bytes: bytes, filename: str, *, source_uri: str, target_uri: str) -> Dict[str, Any]:
    encoded_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
    return {
        "statusCode": 200,
        "body": encoded_pdf,
        "isBase64Encoded": True,
        "headers": {
            "Content-Type": PDF_CONTENT_TYPE,
            "Content-Disposition": f"inline; filename=\"{filename}\"",
            "X-Conversion-Source": source_uri,
            "X-Conversion-Target": target_uri,
        },
    }


def is_valid_sha256(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True
