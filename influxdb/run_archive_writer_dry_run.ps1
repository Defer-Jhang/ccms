param(
    [string]$ProjectRoot = "C:\project\ccms",
    [string]$ArchiveRoot = "E:\InfluxDBArchive",
    [string]$StartTime = "",
    [int]$MaxChunks = 1
)

$ErrorActionPreference = "Stop"
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$Writer = Join-Path $ProjectRoot "core\simulation\influx_archive_writer.py"
$SourceTokenFile = Join-Path $ArchiveRoot "secrets\source-read.token"
$Checkpoint = Join-Path $ArchiveRoot "dry-run-checkpoint.json"

if (-not (Test-Path -LiteralPath $Python)) { throw "Python was not found: $Python" }
if (-not (Test-Path -LiteralPath $Writer)) { throw "Writer was not found: $Writer" }
if (-not (Test-Path -LiteralPath $SourceTokenFile)) { throw "Source token was not found: $SourceTokenFile" }

if ([string]::IsNullOrWhiteSpace($StartTime)) {
    $StartTime = [DateTimeOffset]::UtcNow.AddMinutes(-2).ToString("o")
}

& $Python $Writer `
    --source-token-file $SourceTokenFile `
    --checkpoint $Checkpoint `
    --start-time $StartTime `
    --dry-run `
    --once `
    --max-chunks $MaxChunks `
    --verbose
exit $LASTEXITCODE
