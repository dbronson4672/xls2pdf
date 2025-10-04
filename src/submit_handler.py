"""Lambda handler for the xls2pdf submit API."""

import os
from typing import Any, Dict

from botocore.exceptions import ClientError

from common import (
    LOGGER,
    ConversionError,
    SUPPORTED_INPUT_FORMAT,
    XLSX_CONTENT_TYPE,
    build_object_key,
    delete_prefix_contents,
    calculate_sha256,
    extract_api_payload,
    get_default_bucket,
    json_response,
    load_workbook_bytes,
    sanitize_filename,
    S3_CLIENT,
)


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    payload = extract_api_payload(event)

    filename = sanitize_filename(payload.get("filename"))
    if not filename:
        raise ConversionError("Payload must include a valid 'filename'")

    requested_format = (payload.get("format") or "").lower()
    if requested_format != SUPPORTED_INPUT_FORMAT:
        raise ConversionError(
            f"Unsupported format '{requested_format}', expected '{SUPPORTED_INPUT_FORMAT}'"
        )

    data = payload.get("data")
    if not data:
        raise ConversionError("Payload must include 'data'")

    default_bucket = get_default_bucket()
    workbook_bytes, source_descriptor = load_workbook_bytes(
        data,
        default_bucket=os.getenv("SOURCE_BUCKET"),
    )

    file_hash = calculate_sha256(workbook_bytes)
    object_key = build_object_key(file_hash, filename)
    delete_prefix_contents(default_bucket, file_hash)

    try:
        S3_CLIENT.put_object(
            Bucket=default_bucket,
            Key=object_key,
            Body=workbook_bytes,
            ContentType=XLSX_CONTENT_TYPE,
        )
    except ClientError as error:
        raise ConversionError(
            f"Unable to store workbook at s3://{default_bucket}/{object_key}"
        ) from error

    LOGGER.info(
        "Stored workbook '%s' for hash %s (source=%s)",
        filename,
        file_hash,
        source_descriptor,
    )

    response_body = {
        "filename": filename,
        "status": "submitted",
        "result": file_hash,
    }

    return json_response(response_body, status_code=202)
