[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ApiUrl,

    [Parameter(Mandatory = $true)]
    [string]$FilePath,

    [string]$Filename,

    [string]$Target,

    [string]$Format = 'xlsx',

    [string]$OutputPath,

    [hashtable]$Headers
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

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

$payloadJson = $payload | ConvertTo-Json -Depth 4 -Compress
Write-Verbose "Payload prepared for '$Filename' (size: $($fileBytes.Length) bytes)"

$requestParams = @{
    Uri         = $ApiUrl
    Method      = 'POST'
    Body        = $payloadJson
    ContentType = 'application/json'
}

if ($Headers) {
    $requestParams['Headers'] = $Headers
}

Write-Host "Invoking $ApiUrl"
$response = Invoke-WebRequest @requestParams

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
            throw 'Unexpected JSON payload returned by API; no PDF data located.'
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

if (-not $pdfBytes) {
    throw 'API response did not contain any PDF data.'
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
