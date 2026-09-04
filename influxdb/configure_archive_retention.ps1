param(
    [string]$ProjectRoot = "C:\project\ccms",
    [string]$SourceUrl = "http://localhost:8086",
    [string]$ArchiveUrl = "http://localhost:8087",
    [string]$SourceOrg = "delta",
    [string]$ArchiveOrg = "delta",
    [string]$SourceBucket = "StoreHouseMeasData",
    [string]$ArchiveBucket = "StoreHouseMeasArchive",
    [switch]$ConfirmArchiveVerified
)

$ErrorActionPreference = "Stop"
$Influx = Join-Path $ProjectRoot "influxdb\influx.exe"

function Read-PlainSecret {
    param([string]$Prompt)
    $secureValue = Read-Host $Prompt -AsSecureString
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
    try {
        return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Get-BucketId {
    param(
        [string]$HostUrl,
        [string]$Token,
        [string]$Org,
        [string]$BucketName
    )
    $jsonLines = & $Influx bucket list --host $HostUrl --token $Token --org $Org --json
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to list buckets from $HostUrl"
    }
    $json = $jsonLines -join [Environment]::NewLine
    $matches = @($json | ConvertFrom-Json) | Where-Object { $_.name -eq $BucketName }
    if ($matches.Count -ne 1) {
        throw "Expected one bucket named $BucketName at $HostUrl; found $($matches.Count)."
    }
    return $matches[0].id
}

if (-not $ConfirmArchiveVerified) {
    throw "Verify that 8087 is receiving five-second data, then rerun with -ConfirmArchiveVerified. This prevents old 8086 data from expiring before archive verification."
}
if (-not (Test-Path -LiteralPath $Influx)) {
    throw "Influx CLI was not found: $Influx"
}

$sourceToken = Read-PlainSecret "Enter the 8086 operator/all-access token"
$archiveToken = Read-PlainSecret "Enter the 8087 operator/all-access token"
try {
    $sourceBucketId = Get-BucketId $SourceUrl $sourceToken $SourceOrg $SourceBucket
    $archiveBucketId = Get-BucketId $ArchiveUrl $archiveToken $ArchiveOrg $ArchiveBucket

    & $Influx bucket update --host $ArchiveUrl --token $archiveToken --id $archiveBucketId --retention 17520h
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to set the archive bucket retention. The source was not changed."
    }
    & $Influx bucket update --host $SourceUrl --token $sourceToken --id $sourceBucketId --retention 2160h
    if ($LASTEXITCODE -ne 0) {
        throw "Archive retention was updated, but source retention failed."
    }
} finally {
    $sourceToken = $null
    $archiveToken = $null
}

Write-Host "$ArchiveUrl/$ArchiveBucket retention: 17520h (730 days)"
Write-Host "$SourceUrl/$SourceBucket retention: 2160h (90 days)"
