"""Lambda function triggered by S3 uploads to generate PDFs."""

import os
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus

try:
    from adobe.pdfservices.operation.auth.service_principal_credentials import ServicePrincipalCredentials
    from adobe.pdfservices.operation.pdf_services import PDFServices
    from adobe.pdfservices.operation.pdf_services_media_type import PDFServicesMediaType
    from adobe.pdfservices.operation.pdfjobs.jobs.create_pdf_job import CreatePDFJob
    from adobe.pdfservices.operation.pdfjobs.result.create_pdf_result import CreatePDFResult
except ImportError as exc:  # pragma: no cover - layer is required in production
    raise RuntimeError(
        "pdfservices-sdk v4 is required but not available; ensure it is included in the Lambda layer or deployment package."
    ) from exc

from botocore.exceptions import ClientError

from common import (
    LOGGER,
    ConversionError,
    PDF_CONTENT_TYPE,
    RESULT_OBJECT_NAME,
    S3_CLIENT,
    SSM_CLIENT,
    join_key,
    prefix_for_key,
    write_temp_workbook,
)

_converter: Optional["AdobeWorkbookConverter"] = None


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    LOGGER.debug("Received S3 event: %s", event)
    records = event.get("Records")
    if not isinstance(records, list):
        raise ConversionError("S3 event missing Records")

    outcomes = []
    for record in records:
        if record.get("eventSource") != "aws:s3":
            continue
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        raw_key = record.get("s3", {}).get("object", {}).get("key")
        if not bucket or not raw_key:
            LOGGER.warning("Skipping S3 record missing bucket or key: %s", record)
            continue
        key = unquote_plus(raw_key)
        if not key.lower().endswith(".xlsx"):
            LOGGER.debug("Skipping non-XLSX object %s", key)
            continue
        LOGGER.info("Processing S3 object %s/%s", bucket, key)
        outcomes.append(_convert_object(bucket, key))

    return {
        "status": "ok",
        "processed": len(outcomes),
        "results": outcomes,
    }


def _convert_object(bucket: str, key: str) -> Dict[str, Any]:
    try:
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        raise ConversionError(f"Unable to read object s3://{bucket}/{key}") from error
    workbook_bytes = response["Body"].read()
    temp_path = write_temp_workbook(workbook_bytes)

    try:
        pdf_bytes = _get_converter().convert(temp_path)
    finally:
        Path(temp_path).unlink(missing_ok=True)

    pdf_filename = f"{PurePosixPath(key).stem}.pdf"
    prefix = prefix_for_key(key)
    pdf_key = join_key(prefix, pdf_filename)
    result_key = join_key(prefix, RESULT_OBJECT_NAME)

    try:
        S3_CLIENT.put_object(
            Bucket=bucket,
            Key=pdf_key,
            Body=pdf_bytes,
            ContentType=PDF_CONTENT_TYPE,
        )
    except ClientError as error:
        raise ConversionError(f"Unable to write PDF to s3://{bucket}/{pdf_key}") from error

    try:
        S3_CLIENT.put_object(
            Bucket=bucket,
            Key=result_key,
            Body=pdf_filename.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
    except ClientError as error:
        raise ConversionError(f"Unable to write result marker to s3://{bucket}/{result_key}") from error

    LOGGER.info(
        "Converted %s/%s to %s and recorded result marker",
        bucket,
        key,
        pdf_key,
    )

    return {
        "source": f"s3://{bucket}/{key}",
        "pdf": f"s3://{bucket}/{pdf_key}",
        "result": f"s3://{bucket}/{result_key}",
    }


class AdobeWorkbookConverter:
    """Wraps Adobe PDF Services SDK for converting Excel workbooks to PDF."""

    def __init__(self) -> None:
        self._pdf_services = self._initialise_client()

    def convert(self, workbook_path: str) -> bytes:
        with open(workbook_path, "rb") as handle:  # noqa: PTH123 - Lambda local FS usage
            workbook_bytes = handle.read()

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

        pdf_bytes = pdf_stream.read() if hasattr(pdf_stream, "read") else pdf_stream
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


def _get_converter() -> "AdobeWorkbookConverter":
    global _converter  # pylint: disable=global-statement
    if _converter is None:
        _converter = AdobeWorkbookConverter()
    return _converter
