import base64
import json
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
from adobe.pdfservices.operation.auth.credentials import Credentials
from adobe.pdfservices.operation.execution_context import ExecutionContext
from adobe.pdfservices.operation.io.file_ref import FileRef
from adobe.pdfservices.operation.pdfops.create_pdf_operation import CreatePDFOperation


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")

PDF_CONTENT_TYPE = "application/pdf"
SUPPORTED_INPUT_FORMAT = "xlsx"
DEFAULT_TEMP_PREFIX = "xls2pdf"


class ConversionError(Exception):
    """Raised when the request payload cannot be converted."""


@dataclass
class S3Location:
    bucket: str
    key: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}" if self.key else f"s3://{self.bucket}"  # pragma: no cover


@dataclass
class ConversionResult:
    metadata: Dict[str, Any]
    pdf_bytes: Optional[bytes] = None


class AdobeWorkbookConverter:
    """Wraps Adobe PDF Services SDK for converting Excel workbooks to PDF."""

    def __init__(self) -> None:
        credentials = self._load_credentials()
        self._execution_context = ExecutionContext.create(credentials)

    def convert(self, workbook_path: str) -> bytes:
        """Convert a workbook at the provided path into PDF bytes."""
        operation = CreatePDFOperation.create_new()
        operation.set_input(FileRef.create_from_local_file(workbook_path))

        LOGGER.info("Running Adobe conversion for workbook '%s'", workbook_path)
        result = operation.execute(self._execution_context)

        tmp_pdf = Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.pdf"
        result.save_as(tmp_pdf)
        try:
            return tmp_pdf.read_bytes()
        finally:
            tmp_pdf.unlink(missing_ok=True)

    @staticmethod
    def _load_credentials() -> Credentials:
        credentials_path = os.getenv("ADOBE_CREDENTIALS_PATH")
        if credentials_path and os.path.exists(credentials_path):
            LOGGER.info("Loading Adobe credentials from %s", credentials_path)
            return (
                Credentials.service_account_credentials_builder()
                .from_file(credentials_path)
                .build()
            )

        raise ConversionError(
            "Unable to initialise Adobe credentials; set ADOBE_CREDENTIALS_PATH to a valid file in the layer."
        )


_converter: Optional[AdobeWorkbookConverter] = None


def lambda_handler(event: Dict[str, Any], context: Any) -> Any:  # pylint: disable=unused-argument
    """AWS Lambda entry point supporting API Gateway and SQS triggers."""
    LOGGER.debug("Received event: %s", json.dumps(event))

    try:
        if _is_sqs_event(event):
            return _handle_sqs_records(event["Records"])
        return _handle_api_event(event)
    except ConversionError as exc:
        LOGGER.error("Conversion failed: %s", exc, exc_info=True)
        if _is_api_event(event):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": str(exc)}),
                "headers": {"Content-Type": "application/json"},
            }
        raise


def _handle_sqs_records(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    results = []
    for record in records:
        body = record.get("body")
        if not body:
            raise ConversionError("SQS message is missing body")
        payload = json.loads(body)
        LOGGER.info("Processing SQS message %s", record.get("messageId"))
        outcome = _process_payload(payload, invocation_source="sqs", return_pdf=False)
        results.append(outcome.metadata)

    return {
        "status": "ok",
        "processed": len(results),
        "results": results,
    }

def _handle_api_event(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = _extract_api_payload(event)
    LOGGER.info("Processing API request for filename '%s'", payload.get("filename"))
    outcome = _process_payload(payload, invocation_source="api", return_pdf=True)
    if not outcome.pdf_bytes:
        raise ConversionError("Conversion produced no PDF content")
    encoded_pdf = base64.b64encode(outcome.pdf_bytes).decode("utf-8")
    return {
        "statusCode": 200,
        "body": encoded_pdf,
        "isBase64Encoded": True,
        "headers": {
            "Content-Type": PDF_CONTENT_TYPE,
            "Content-Disposition": f"inline; filename=\"{outcome.metadata['filename']}\"",
            "X-Conversion-Target": outcome.metadata["target"],
            "X-Conversion-Source": outcome.metadata["source"],
        },
    }


def _extract_api_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" not in event:
        raise ConversionError("API event missing body")

    body = event["body"]
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ConversionError("Request body is not valid JSON") from exc

    return payload


def _process_payload(
    payload: Dict[str, Any], *, invocation_source: str, return_pdf: bool
) -> ConversionResult:
    filename = payload.get("filename")
    if not filename:
        raise ConversionError("Payload must include 'filename'")

    requested_format = payload.get("format", "").lower()
    if requested_format != SUPPORTED_INPUT_FORMAT:
        raise ConversionError(f"Unsupported format '{requested_format}', expected '{SUPPORTED_INPUT_FORMAT}'")

    data = payload.get("data")
    if not data:
        raise ConversionError("Payload must include 'data'")

    workbook_path, source_descriptor = _materialise_workbook(data, default_bucket=os.getenv("SOURCE_BUCKET"))

    converter = _get_converter()
    try:
        pdf_bytes = converter.convert(workbook_path)
    finally:
        Path(workbook_path).unlink(missing_ok=True)

    target_location = _resolve_target(payload.get("target"), filename)
    _write_pdf_to_s3(pdf_bytes, target_location)

    metadata = {
        "source": source_descriptor,
        "target": target_location.uri,
        "filename": Path(target_location.key).name,
        "invocation": invocation_source,
    }

    return ConversionResult(metadata=metadata, pdf_bytes=pdf_bytes if return_pdf else None)


def _materialise_workbook(data: str, *, default_bucket: Optional[str]) -> Tuple[str, str]:
    if data.startswith("s3://"):
        bucket, key = _parse_s3_uri(data)
        LOGGER.info("Downloading workbook from %s", data)
        try:
            response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            raise ConversionError(f"Unable to read source object {data}") from error
        bytes_buffer = response["Body"].read()
        return _write_temp_workbook(bytes_buffer), data

    if data.startswith("/") and default_bucket:
        # Allow keys without scheme by combining with a default bucket.
        return _materialise_workbook(f"s3://{default_bucket}{data}", default_bucket=default_bucket)

    try:
        workbook_bytes = base64.b64decode(data)
    except (ValueError, TypeError) as exc:
        raise ConversionError("Workbook payload is not valid base64 content") from exc

    return _write_temp_workbook(workbook_bytes), "inline"


def _write_temp_workbook(contents: bytes) -> str:
    if not contents:
        raise ConversionError("Workbook content is empty")

    with tempfile.NamedTemporaryFile(prefix=DEFAULT_TEMP_PREFIX, suffix=".xlsx", delete=False) as handle:
        handle.write(contents)
        temp_path = handle.name
    return temp_path


def _resolve_target(target: Optional[str], filename: str) -> S3Location:
    default_bucket = os.getenv("DEFAULT_TARGET_BUCKET")
    pdf_name = f"{Path(filename).stem}.pdf"

    if target and target.startswith("s3://"):
        bucket, key = _parse_s3_uri(target)
        key = _normalise_target_key(key, pdf_name)
        return S3Location(bucket=bucket, key=key)

    if target and not target.startswith("s3://"):
        if not default_bucket:
            raise ConversionError("Target key provided without a default bucket configured")
        key = _normalise_target_key(target.lstrip("/"), pdf_name)
        return S3Location(bucket=default_bucket, key=key)

    if not default_bucket:
        raise ConversionError("No target provided and DEFAULT_TARGET_BUCKET is not configured")

    key = _normalise_target_key(f"converted/{pdf_name}", pdf_name)
    return S3Location(bucket=default_bucket, key=key)


def _normalise_target_key(proposed_key: str, pdf_name: str) -> str:
    proposed_key = proposed_key.strip()
    if not proposed_key:
        return pdf_name
    if proposed_key.endswith("/"):
        return f"{proposed_key.rstrip('/')}/{pdf_name}"
    if proposed_key.lower().endswith(".pdf"):
        return proposed_key
    return f"{proposed_key}.pdf"


def _write_pdf_to_s3(pdf_bytes: bytes, location: S3Location) -> None:
    try:
        S3_CLIENT.put_object(
            Bucket=location.bucket,
            Key=location.key,
            Body=pdf_bytes,
            ContentType=PDF_CONTENT_TYPE,
        )
    except ClientError as error:
        raise ConversionError(f"Unable to store PDF at {location.uri}") from error


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
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


def _get_converter() -> AdobeWorkbookConverter:
    global _converter  # pylint: disable=global-statement
    if _converter is None:
        _converter = AdobeWorkbookConverter()
    return _converter


def _is_sqs_event(event: Dict[str, Any]) -> bool:
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        return False
    return records[0].get("eventSource") == "aws:sqs"


def _is_api_event(event: Dict[str, Any]) -> bool:
    return "httpMethod" in event or "requestContext" in event
