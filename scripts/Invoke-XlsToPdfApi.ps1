#!/usr/bin/env pwsh
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SubmitUrl,

    [string]$GetUrl,

    [Parameter(Mandatory = $true)]
    [string]$FilePath,

    [string]$Filename,

    [string]$Format = 'xlsx',

    [string]$Target,

    [string]$OutputPath,

    [hashtable]$Headers,

    [int]$MaxAttempts = 12,

    [int]$DelaySeconds = 5,

    [string]$ResultKey
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
    throw "File '$FilePath' not found."
}

$resolvedPath = (Resolve-Path -LiteralPath $FilePath).ProviderPath
$fileBytes = [System.IO.File]::ReadAllBytes($resolvedPath)
if (-not $fileBytes.Length) {
    throw "File '$resolvedPath' is empty."
}

if ([string]::IsNullOrWhiteSpace($Filename)) {
    $Filename = [System.IO.Path]::GetFileName($resolvedPath)
}

if ([string]::IsNullOrWhiteSpace($Format)) {
    $Format = 'xlsx'
}

$base64Data = [System.Convert]::ToBase64String($fileBytes)

$payload = [ordered]@{
    filename = $Filename
    format   = $Format.ToLowerInvariant()
    data     = $base64Data
}

if ($Target) {
    $payload['target'] = $Target
}

if (-not $ResultKey) {
    $payloadJson = $payload | ConvertTo-Json -Depth 4 -Compress
    Write-Verbose "Payload prepared for '$Filename' (size: $($fileBytes.Length) bytes)"

    $requestParams = @{
        Uri         = $SubmitUrl
        Method      = 'POST'
        Body        = $payloadJson
        ContentType = 'application/json'
    }

    if ($Headers) {
        $requestParams['Headers'] = $Headers
    }

    Write-Host "Invoking submit API $SubmitUrl"
    $submitResponse = Invoke-WebRequest @requestParams
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

    $submitErrorProp = $submitJson.PSObject.Properties['error']
    if ($submitErrorProp -and $submitErrorProp.Value) {
        throw "Submit API returned an error: $($submitErrorProp.Value)"
    }

    $submitStatusProp = $submitJson.PSObject.Properties['status']
    if (-not $submitStatusProp) {
        throw 'Submit API response did not include a status field.'
    }
    $submitStatus = [string]$submitStatusProp.Value
    if ($submitStatus -ne 'submitted') {
        throw "Submit API returned unexpected status '$submitStatus'"
    }

    $submitResultProp = $submitJson.PSObject.Properties['result']
    if (-not $submitResultProp -or -not $submitResultProp.Value) {
        throw 'Submit API response did not include a result identifier.'
    }

    $ResultKey = [string]$submitResultProp.Value
    Write-Host "Result key: $ResultKey"
}

if (-not $GetUrl) {
    Write-Host 'No GET URL provided; returning result identifier only.'
    Write-Output $ResultKey
    return
}

if ($MaxAttempts -lt 1) {
    throw "MaxAttempts must be at least 1."
}

if ($DelaySeconds -lt 0) {
    throw "DelaySeconds cannot be negative."
}

$pollUriBase = if ($GetUrl.Contains('?')) { "${GetUrl}&result=" } else { "${GetUrl}?result=" }
$attempt = 0
$pdfBytes = $null

while ($attempt -lt $MaxAttempts -and -not $pdfBytes) {
    $attempt++
    $pollUri = "$pollUriBase$ResultKey"
    Write-Host ("Polling attempt {0}: {1}" -f $attempt, $pollUri)

    try {
        $getParams = @{ Uri = $pollUri; Method = 'Get' }
        if ($Headers) {
            $getParams['Headers'] = $Headers
        }
        $response = Invoke-WebRequest @getParams
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
    if ($contentType -and $contentType[0] -like 'application/pdf*') {
        $memoryStream = New-Object System.IO.MemoryStream
        $response.RawContentStream.Seek(0, [System.IO.SeekOrigin]::Begin) | Out-Null
        $response.RawContentStream.CopyTo($memoryStream)
        $pdfBytes = $memoryStream.ToArray()
        break
    }

    $bodyText = $response.Content.Trim()
    if ($bodyText.StartsWith('{')) {
        try {
            $jsonBody = $bodyText | ConvertFrom-Json -ErrorAction Stop
        }
        catch {
            throw "Unable to parse GET response JSON: $($_.Exception.Message)"
        }

        $getErrorProp = $jsonBody.PSObject.Properties['error']
        if ($getErrorProp -and $getErrorProp.Value) {
            throw "GET API returned an error: $($getErrorProp.Value)"
        }

        $getStatusProp = $jsonBody.PSObject.Properties['status']
        if ($getStatusProp -and ($getStatusProp.Value -eq 'inprogress' -or $getStatusProp.Value -eq 'submitted')) {
            Write-Host "Status: $($getStatusProp.Value). Waiting $DelaySeconds seconds before retrying."
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

    if (-not $pdfBytes) {
        Write-Warning 'GET API response did not contain PDF content yet.'
        if ($DelaySeconds -gt 0) {
            Start-Sleep -Seconds $DelaySeconds
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
        throw 'Received base64-like PDF response but decoding failed.'
    }
}

if (-not $pdfBytes) {
    throw "Conversion not completed after $MaxAttempts polling attempts. Result key: $ResultKey"
}

if (-not $OutputPath) {
    $folder = [System.IO.Path]::GetDirectoryName($resolvedPath)
    $stem = [System.IO.Path]::GetFileNameWithoutExtension($Filename)
    if (-not $stem) {
        $stem = 'converted'
    }
    $OutputPath = [System.IO.Path]::Combine($folder, "$stem.pdf")
}

$outputFullPath = [System.IO.Path]::GetFullPath($OutputPath)
[System.IO.File]::WriteAllBytes($outputFullPath, $pdfBytes)

Write-Host "Saved PDF to $outputFullPath"

$targetHeader = $response.Headers['X-Conversion-Target']
$sourceHeader = $response.Headers['X-Conversion-Source']
if ($targetHeader) {
    Write-Host "Target: $targetHeader"
}
if ($sourceHeader) {
    Write-Host "Source: $sourceHeader"
}

Write-Output $ResultKey
