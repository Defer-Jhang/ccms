[CmdletBinding()]
param(
    [string]$InfluxDbToken = "",
    [string]$InfluxDbUrl = "http://localhost:8086",
    [string]$InfluxDbOrg = "delta",
    [string]$Python = "",
    [string]$Test = "",

    # Both writer programs default to this bucket/measurement. Override the
    # small target when influx_stress_writer_month.py uses a separate bucket.
    [string]$LargeBucket = "StoreHouseMeasData",
    [string]$LargeMeasurement = "meas_data",
    [string]$SmallBucket = "StoreHouseMeasData",
    [string]$SmallMeasurement = "meas_data",

    [string]$LargeDataStart = "2024-09-15T00:00:00+08:00",
    [string]$LargeDataStop = "2026-09-01T00:00:00+08:00",
    [string]$SmallDataStart = "2024-09-15T00:00:00+08:00",
    [string]$SmallDataStop = "2026-09-01T00:00:00+08:00",

    [ValidateSet("fixed", "random", "rolling")]
    [string]$QueryMode = "random",
    [int]$QueryDurationHours = 24,
    [int]$QueryPoints = 10000,
    [string]$QueryFields = "voltage",
    [int]$QueryChunkSeconds = 10,
    [int]$QueryTimeoutSeconds = 120,
    [int]$QueryRandomSeed = 20260917,

    [int]$StressDurationSeconds = 30,
    [int]$StressWriteWorkers = 8,
    [int]$StressQueryWorkers = 8,
    [int]$WritePoints = 50000,
    [int]$WriteBatchSize = 50000,

    [int]$DataSeed = 20250818,
    [ValidateSet("cycle", "servermap")]
    [string]$QrCodeMode = "cycle",
    [switch]$NoGzip,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if ([string]::IsNullOrWhiteSpace($Python)) {
    $Python = Join-Path $repoRoot ".venv\Scripts\python.exe"
}
if ([string]::IsNullOrWhiteSpace($Test)) {
    $Test = Join-Path $repoRoot "core\simulation\influx_performance_test.py"
}

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Python executable not found: $Python"
}
if (-not (Test-Path -LiteralPath $Test)) {
    throw "Performance test not found: $Test"
}

if ([string]::IsNullOrWhiteSpace($InfluxDbToken)) {
    $InfluxDbToken = $env:INFLUXDB_TOKEN
}
if ([string]::IsNullOrWhiteSpace($InfluxDbToken) -and -not $DryRun) {
    throw "Set INFLUXDB_TOKEN or pass -InfluxDbToken. Do not hard-code tokens in this script."
}

$env:INFLUXDB_URL = $InfluxDbUrl
$env:INFLUXDB_ORG = $InfluxDbOrg
if (-not [string]::IsNullOrWhiteSpace($InfluxDbToken)) {
    $env:INFLUXDB_TOKEN = $InfluxDbToken
}

function Invoke-PerformanceCase {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("large", "small")]
        [string]$Target,

        [Parameter(Mandatory = $true)]
        [ValidateSet("write", "query")]
        [string]$Operation
    )

    $arguments = @(
        $Test,
        "--stress-only",
        "--stress-operation", $Operation,
        "--stress-target", $Target,
        "--stress-write-workers", $(if ($Operation -eq "write") { $StressWriteWorkers } else { 0 }),
        "--stress-query-workers", $(if ($Operation -eq "query") { $StressQueryWorkers } else { 0 }),
        "--stress-duration-seconds", $StressDurationSeconds,
        "--write-points", $WritePoints,
        "--write-batch-size", $WriteBatchSize,
        "--large-bucket", $LargeBucket,
        "--large-measurement", $LargeMeasurement,
        "--small-bucket", $SmallBucket,
        "--small-measurement", $SmallMeasurement,
        "--large-query-start", $LargeDataStart,
        "--large-query-stop", $LargeDataStop,
        "--small-query-start", $SmallDataStart,
        "--small-query-stop", $SmallDataStop,
        "--stress-query-mode", $QueryMode,
        "--stress-query-random-seed", $QueryRandomSeed,
        "--query-duration-hours", $QueryDurationHours,
        "--query-fields", $QueryFields,
        "--query-shape", "stream",
        "--query-points", $QueryPoints,
        "--stress-query-timeout", $QueryTimeoutSeconds,
        "--stress-query-chunk-seconds", $QueryChunkSeconds,
        "--schema", "production",
        "--return-code", "OK",
        "--seed", $DataSeed,
        "--qrcode-mode", $QrCodeMode
    )

    if ($Operation -eq "query") {
        $arguments += "--query-count-only"
    }
    if ($NoGzip) {
        $arguments += "--no-gzip"
    }
    if ($DryRun) {
        $arguments += "--dry-run"
    }

    Write-Host "`n===== $Target $Operation =====" -ForegroundColor Cyan
    & $Python @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Target $Operation failed with exit code $LASTEXITCODE"
    }
}

# Invocation with & is synchronous: the next case starts only after the
# previous Python process exits.
Invoke-PerformanceCase -Target large -Operation write
Invoke-PerformanceCase -Target large -Operation query
Invoke-PerformanceCase -Target small -Operation write
Invoke-PerformanceCase -Target small -Operation query

Write-Host "`nAll four performance cases completed." -ForegroundColor Green
