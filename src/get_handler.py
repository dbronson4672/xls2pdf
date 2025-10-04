"""Lambda handler for retrieving converted PDFs."""

from pathlib import PurePosixPath
from typing import Any, Dict

from common import (
    LOGGER,
    ConversionError,
    RESULT_OBJECT_NAME,
    extract_api_payload,
    find_original_object,
    get_default_bucket,
    is_valid_sha256,
    json_response,
    join_key,
    object_exists,
    pdf_response,
    read_binary_object,
    read_text_object,
)


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    lookup_key = _extract_result_identifier(event)
    bucket = get_default_bucket()

    original_key = find_original_object(bucket, lookup_key)
    if not original_key:
        return json_response({"error": "Result not found"}, status_code=404)

    result_key = join_key(lookup_key, RESULT_OBJECT_NAME)
    if not object_exists(bucket, result_key):
        LOGGER.info("Result file not present for %s; still in progress", lookup_key)
        original_name = PurePosixPath(original_key).name
        return json_response(
            {
                "filename": original_name,
                "status": "inprogress",
                "result": lookup_key,
            }
        )

    result_filename = read_text_object(bucket, result_key).strip()
    original_name = PurePosixPath(original_key).name
    expected_pdf_name = f"{PurePosixPath(original_name).stem}.pdf"

    pdf_key = join_key(lookup_key, expected_pdf_name)
    if not object_exists(bucket, pdf_key):
        if not result_filename:
            raise ConversionError("Result marker found but no PDF filename was recorded")
        pdf_key = join_key(lookup_key, result_filename)
        if not object_exists(bucket, pdf_key):
            raise ConversionError(
                f"Unable to locate PDF output recorded in result marker for '{lookup_key}'"
            )

    pdf_bytes = read_binary_object(bucket, pdf_key)
    pdf_filename = PurePosixPath(pdf_key).name
    source_uri = f"s3://{bucket}/{original_key}"
    target_uri = f"s3://{bucket}/{pdf_key}"

    return pdf_response(pdf_bytes, pdf_filename, source_uri=source_uri, target_uri=target_uri)


def _extract_result_identifier(event: Dict[str, Any]) -> str:
    params = event.get("queryStringParameters") or {}
    lookup = params.get("result") if params else None

    if not lookup:
        payload = extract_api_payload(event, optional=True)
        lookup = payload.get("result") if payload else None

    if not lookup:
        raise ConversionError("Request must include 'result'")

    lookup = lookup.strip()
    if not is_valid_sha256(lookup):
        raise ConversionError("Result identifier must be a 64 character hexadecimal string")

    return lookup
