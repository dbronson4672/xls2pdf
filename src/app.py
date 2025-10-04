import base64
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
try:
    from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
    from adobe.pdfservices.operation.exception.exceptions import ServiceApiException, ServiceUsageException, SdkException
    from adobe.pdfservices.operation.io.cloud_asset import CloudAsset
    from adobe.pdfservices.operation.io.stream_asset import StreamAsset
    from adobe.pdfservices.operation.pdf_services import PDFServices
    from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
    from adobe.pdfservices.operation.pdfjobs.jobs.create_pdf_job import CreatePDFJob
    from adobe.pdfservices.operation.pdfjobs.result.create_pdf_result import CreatePDFResult
    
except ImportError as exc:
    raise RuntimeError(
        "pdfservices-sdk v4 is required but not available; ensure it is included in the Lambda layer or deployment package."
    ) from exc


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")
SSM_CLIENT = boto3.client("ssm")

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
        self._pdf_services = self._initialise_client()

    def convert(self, workbook_path: str) -> bytes:
        """Convert a workbook at the provided path into PDF bytes."""
        try:
            with open(workbook_path, "rb") as handle:
                workbook_bytes = handle.read()
        except OSError as exc:  # pragma: no cover - unexpected filesystem faults
            raise ConversionError(f"Unable to read workbook at {workbook_path}") from exc

        try:
            LOGGER.info("Running Adobe conversion for workbook '%s'", workbook_path)
            input_asset = self._pdf_services.upload(
                input_stream=workbook_bytes,
                mime_type=PDFServicesMediaType.XLSX,
            )
            create_pdf_job = CreatePDFJob(input_asset)
            location = self._pdf_services.submit(create_pdf_job)
            job_result = self._pdf_services.get_job_result(location, CreatePDFResult)
            result_asset = job_result.get_result().get_asset()
            stream_asset = self._pdf_services.get_content(result_asset)
            pdf_stream = stream_asset.get_input_stream()
        except Exception as exc:  # pragma: no cover - SDK surface raises many exception types
            raise ConversionError("Adobe PDF Services conversion failed") from exc

        if hasattr(pdf_stream, "read"):
            pdf_bytes = pdf_stream.read()
        else:
            pdf_bytes = pdf_stream

        if not isinstance(pdf_bytes, (bytes, bytearray)):
            raise ConversionError("Adobe PDF Services returned unexpected content type")

        return bytes(pdf_bytes)

    @staticmethod
    def _initialise_client() -> PDFServices:
        client_id = _resolve_secret_value(
            value_env="PDF_SERVICES_CLIENT_ID",
            parameter_env="PDF_SERVICES_CLIENT_ID_PARAMETER",
            secret_label="client identifier",
        )
        client_secret = _resolve_secret_value(
            value_env="PDF_SERVICES_CLIENT_SECRET",
            parameter_env="PDF_SERVICES_CLIENT_SECRET_PARAMETER",
            secret_label="client secret",
        )

        credentials = ServicePrincipalCredentials(client_id=client_id, client_secret=client_secret)

        return PDFServices(credentials=credentials)


def _resolve_secret_value(*, value_env: str, parameter_env: str, secret_label: str) -> str:
    direct_value = os.getenv(value_env)
    if direct_value:
        return direct_value

    parameter_name = os.getenv(parameter_env)
    if not parameter_name:
        raise ConversionError(
            f"PDF Services {secret_label} is not configured; set {value_env} or {parameter_env}."
        )

    try:
        response = SSM_CLIENT.get_parameter(Name=parameter_name, WithDecryption=True)
    except ClientError as exc:
        raise ConversionError(f"Unable to resolve {secret_label} from SSM parameter '{parameter_name}'") from exc

    value = response.get("Parameter", {}).get("Value")
    if not value:
        raise ConversionError(f"Retrieved empty {secret_label} from SSM parameter '{parameter_name}'")

    return value


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

    target_value = payload.get("target")
    pdf_name = f"{Path(filename).stem}.pdf"
    target_location: Optional[S3Location] = None
    if not (return_pdf and _is_blank_string(target_value)):
        target_location = _resolve_target(target_value, filename)
        _write_pdf_to_s3(pdf_bytes, target_location)
        target_descriptor = target_location.uri
        output_filename = Path(target_location.key).name
    else:
        target_descriptor = ""
        output_filename = pdf_name

    metadata = {
        "source": source_descriptor,
        "target": target_descriptor,
        "filename": output_filename,
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


def _is_blank_string(value: Optional[str]) -> bool:
    return isinstance(value, str) and not value.strip()
