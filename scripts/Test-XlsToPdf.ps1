# Harness to encode an XLSX workbook and exercise the xls2pdf Lambda/API.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$InputFile,

    [string]$Target,

    [string]$ApiUrl,

    [string]$OutputPdf,

    [string]$EventOutput,

    [ValidateSet('api', 'sqs')]
    [string]$EventType = 'api'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function New-ApiEvent {
    param(
        [string]$BodyJson
    )

    return @{
        resource        = '/xls-to-pdf'
        path            = '/xls-to-pdf'
        httpMethod      = 'POST'
        headers         = @{ 'Content-Type' = 'application/json' }
        isBase64Encoded = $false
        body            = $BodyJson
    }
}

function New-SqsEvent {
    param(
        [string]$BodyJson
    )

    $record = @{
        messageId         = [guid]::NewGuid().ToString()
        receiptHandle     = 'placeholder-handle'
        body              = $BodyJson
        attributes        = @{}
        messageAttributes = @{}
        md5OfBody         = ''
        eventSource       = 'aws:sqs'
        eventSourceARN    = 'arn:aws:sqs:us-east-1:123456789012:xls2pdf-test'
        awsRegion         = 'us-east-1'
    }

    return @{ Records = @($record) }
}

function Test-IsLikelyBase64Bytes {
    param(
        [byte[]]$Bytes
    )

    if (-not $Bytes -or -not $Bytes.Length) {
        return $false
    }

    foreach ($b in $Bytes) {
        if (($b -ge 48 -and $b -le 57) -or  # 0-9
            ($b -ge 65 -and $b -le 90) -or  # A-Z
            ($b -ge 97 -and $b -le 122) -or # a-z
            $b -eq 43 -or $b -eq 47 -or     # + /
            $b -eq 61 -or                   # =
            $b -eq 10 -or $b -eq 13) {      # new lines
            continue
        }
        return $false
    }

    return $true
}

if (-not (Test-Path -LiteralPath $InputFile -PathType Leaf)) {
    throw "Input file '$InputFile' not found."
}

$resolvedInput = (Resolve-Path -LiteralPath $InputFile).ProviderPath
$fileName = [System.IO.Path]::GetFileName($resolvedInput)

Write-Verbose "Reading workbook $resolvedInput"
$fileBytes = [System.IO.File]::ReadAllBytes($resolvedInput)
if (-not $fileBytes.Length) {
    throw "Input file '$resolvedInput' is empty."
}

$base64Data = [System.Convert]::ToBase64String($fileBytes)

$payload = [ordered]@{
    filename = $fileName
    format   = 'xlsx'
    data     = $base64Data
}

if ($Target) {
    $payload['target'] = $Target
}

$payloadJson = $payload | ConvertTo-Json -Depth 6 -Compress

if ($EventOutput) {
    $event = if ($EventType -eq 'sqs') {
        New-SqsEvent -BodyJson $payloadJson
    }
    else {
        New-ApiEvent -BodyJson $payloadJson
    }

    $eventJson = $event | ConvertTo-Json -Depth 6
    $eventPath = [System.IO.Path]::GetFullPath($EventOutput)
    [System.IO.File]::WriteAllText($eventPath, $eventJson)
    Write-Host "Wrote sample $EventType event to $eventPath"
}

if ($ApiUrl) {
    if (-not $OutputPdf) {
        $folder = [System.IO.Path]::GetDirectoryName($resolvedInput)
        $stem = [System.IO.Path]::GetFileNameWithoutExtension($resolvedInput)
        $OutputPdf = [System.IO.Path]::Combine($folder, "$stem.pdf")
    }

    Write-Host "Invoking API $ApiUrl"
    # $response = Invoke-WebRequest -Verbose -Uri $ApiUrl -Method Post -ContentType 'application/json' -Body $payloadJson
    $response = Invoke-WebRequest -Uri $ApiUrl -Method Post -ContentType 'application/json' -Body $payloadJson
    $pdfBytes = $null
    $contentType = $response.Headers['Content-Type']
    if ($contentType -and $contentType[0] -like 'application/pdf*') {
        $memoryStream = New-Object System.IO.MemoryStream
        $response.RawContentStream.Seek(0, [System.IO.SeekOrigin]::Begin) | Out-Null
        $response.RawContentStream.CopyTo($memoryStream)
        $pdfBytes = $memoryStream.ToArray()
    }
    else {
        $bodyText = $response.Content.Trim()
        if ($bodyText.StartsWith('{')) {
            try {
                $jsonBody = $bodyText | ConvertFrom-Json -ErrorAction Stop
                if ($jsonBody.error) {
                    throw "API returned an error: $($jsonBody.error)"
                }
                throw 'Unexpected JSON response from API; no PDF content located.'
            }
            catch {
                throw $_
            }
        }

        $sanitised = $bodyText.Trim('"').Replace("`n", '').Replace("`r", '')
        try {
            $pdfBytes = [System.Convert]::FromBase64String($sanitised)
        }
        catch {
            throw 'Unable to decode PDF data returned by API.'
        }
    }

    if ($pdfBytes -and (Test-IsLikelyBase64Bytes -Bytes $pdfBytes)) {
        $encodedText = [System.Text.Encoding]::ASCII.GetString($pdfBytes)
        $sanitised = $encodedText.Trim('"').Replace("`n", '').Replace("`r", '')
        try {
            $pdfBytes = [System.Convert]::FromBase64String($sanitised)
        }
        catch {
            throw 'Received base64-like response but decoding failed.'
        }
    }

    if (-not $pdfBytes) {
        throw 'API response did not contain any PDF data.'
    }

    $outputPath = [System.IO.Path]::GetFullPath($OutputPdf)
    [System.IO.File]::WriteAllBytes($outputPath, $pdfBytes)
    Write-Host "Saved PDF to $outputPath"

    $targetHeader = $response.Headers['X-Conversion-Target']
    $sourceHeader = $response.Headers['X-Conversion-Source']
    if ($targetHeader) {
        Write-Host "Target: $targetHeader"
    }
    if ($sourceHeader) {
        Write-Host "Source: $sourceHeader"
    }
}

if (-not $ApiUrl -and -not $EventOutput) {
    Write-Host $payloadJson
}
