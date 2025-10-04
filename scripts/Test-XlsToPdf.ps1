# Harness to encode an XLSX workbook, submit it to the async API, and optionally poll for the PDF.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$InputFile,

    [string]$SubmitUrl,

    [string]$GetUrl,

    [string]$ResultKey,

    [int]$MaxAttempts = 12,

    [int]$DelaySeconds = 5,

    [string]$OutputPdf,

    [string]$EventOutput
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function New-SubmitApiEvent {
    param(
        [string]$BodyJson
    )

    return @{
        resource        = '/submit'
        path            = '/submit'
        httpMethod      = 'POST'
        headers         = @{ 'Content-Type' = 'application/json' }
        isBase64Encoded = $false
        body            = $BodyJson
    }
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

if ($MaxAttempts -lt 1) {
    throw "MaxAttempts must be at least 1."
}

if ($DelaySeconds -lt 0) {
    throw "DelaySeconds cannot be negative."
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

$payloadJson = $payload | ConvertTo-Json -Depth 6 -Compress

if ($EventOutput) {
    $event = New-SubmitApiEvent -BodyJson $payloadJson
    $eventJson = $event | ConvertTo-Json -Depth 6
    $eventPath = [System.IO.Path]::GetFullPath($EventOutput)
    [System.IO.File]::WriteAllText($eventPath, $eventJson)
    Write-Host "Wrote sample submit event to $eventPath"
}

$resultId = $ResultKey

if (-not $resultId -and $SubmitUrl) {
    Write-Host "Submitting workbook to $SubmitUrl"
    $submitResponse = Invoke-WebRequest -Uri $SubmitUrl -Method Post -ContentType 'application/json' -Body $payloadJson
    $submitBody = $submitResponse.Content.Trim()
    if (-not $submitBody.StartsWith('{')) {
        throw 'Submit API returned a non-JSON response.'
    }

    try {
        $submitJson = $submitBody | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        throw "Unable to parse submit response: $($_.Exception.Message)"
    }

    if ($submitJson.error) {
        throw "Submit API returned an error: $($submitJson.error)"
    }

    if ($submitJson.status -ne 'submitted') {
        throw "Submit API returned unexpected status '$($submitJson.status)'"
    }

    if (-not $submitJson.result) {
        throw 'Submit API response did not include a result identifier.'
    }

    $resultId = $submitJson.result
    Write-Host "Result key: $resultId"
}

if (-not $resultId) {
    Write-Host 'No submit call was made and no result identifier supplied; nothing further to do.'
    if (-not $SubmitUrl) {
        Write-Host $payloadJson
    }
    return
}

if (-not $GetUrl) {
    Write-Host "Result identifier: $resultId"
    return
}

if (-not $OutputPdf) {
    $folder = [System.IO.Path]::GetDirectoryName($resolvedInput)
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($resolvedInput)
    $OutputPdf = [System.IO.Path]::Combine($folder, "$stem.pdf")
}

$pollUriBase = if ($GetUrl.Contains('?')) { "$GetUrl&result=" } else { "$GetUrl?result=" }
$attempt = 0
$downloaded = $false

while ($attempt -lt $MaxAttempts -and -not $downloaded) {
    $attempt++
    $pollUri = "$pollUriBase$resultId"
    Write-Host "Polling attempt $attempt: $pollUri"

    try {
        $response = Invoke-WebRequest -Uri $pollUri -Method Get
    }
    catch {
        if ($attempt -ge $MaxAttempts) {
            throw "GET request failed on final attempt: $($_.Exception.Message)"
        }
        Write-Warning "GET request failed: $($_.Exception.Message). Retrying after $DelaySeconds seconds."
        if ($DelaySeconds -gt 0) {
            Start-Sleep -Seconds $DelaySeconds
        }
        continue
    }

    $contentType = $response.Headers['Content-Type']
    $pdfBytes = $null

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
            }
            catch {
                throw "Unable to parse GET response JSON: $($_.Exception.Message)"
            }

            if ($jsonBody.error) {
                throw "GET API returned an error: $($jsonBody.error)"
            }

            if ($jsonBody.status -eq 'inprogress' -or $jsonBody.status -eq 'submitted') {
                Write-Host "Status: $($jsonBody.status). Waiting $DelaySeconds seconds before retrying."
                if ($DelaySeconds -gt 0) {
                    Start-Sleep -Seconds $DelaySeconds
                }
                continue
            }

            throw "Unexpected JSON response from GET API: $($jsonBody | ConvertTo-Json -Compress)"
        }

        if ($bodyText) {
            $sanitised = $bodyText.Trim('"').Replace("`n", '').Replace("`r", '')
            try {
                $pdfBytes = [System.Convert]::FromBase64String($sanitised)
            }
            catch {
                throw 'Unable to decode PDF data returned by GET API.'
            }
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

    if (-not $pdfBytes -or -not $pdfBytes.Length) {
        Write-Warning 'GET API response did not contain PDF content yet.'
        if ($DelaySeconds -gt 0) {
            Start-Sleep -Seconds $DelaySeconds
        }
        continue
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
    $downloaded = $true
}

if (-not $downloaded) {
    throw "Conversion not completed after $MaxAttempts polling attempts. Result key: $resultId"
}

Write-Host "Result key: $resultId"
