param(
    [string]$ServiceName = "InfluxArchiveWriter",
    [string]$ProjectRoot = "C:\project\ccms",
    [string]$ArchiveRoot = "E:\InfluxDBArchive",
    [string]$SourceUrl = "http://localhost:8086",
    [string]$DestinationUrl = "http://localhost:8087",
    [string]$SourceOrg = "delta",
    [string]$DestinationOrg = "delta",
    [string]$SourceBucket = "StoreHouseMeasData",
    [string]$DestinationBucket = "StoreHouseMeasArchive",
    [string]$Measurement = "meas_data",
    [int]$IntervalSeconds = 5,
    [int]$ChunkSeconds = 60,
    [int]$OverlapSeconds = 10,
    [int]$SafetyDelaySeconds = 10,
    [string]$InitialStart = "now",
    [switch]$RefreshTokens,
    [switch]$DoNotStart
)

$ErrorActionPreference = "Stop"
$Nssm = Join-Path $ProjectRoot "nssm.exe"
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$Writer = Join-Path $ProjectRoot "core\simulation\influx_archive_writer.py"
$SecretDirectory = Join-Path $ArchiveRoot "secrets"
$SourceTokenFile = Join-Path $SecretDirectory "source-read.token"
$DestinationTokenFile = Join-Path $SecretDirectory "archive-write.token"
$Checkpoint = Join-Path $ArchiveRoot "archive_writer_checkpoint.json"
$LogDirectory = Join-Path $ArchiveRoot "log"
$StdOut = Join-Path $LogDirectory "archive-writer.out.log"
$StdErr = Join-Path $LogDirectory "archive-writer.err.log"

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script from an elevated PowerShell window."
    }
}

function Write-SecretFile {
    param([string]$Path, [string]$Prompt)
    $secureValue = Read-Host $Prompt -AsSecureString
    $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
    try {
        $plainValue = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer)
        if ([string]::IsNullOrWhiteSpace($plainValue)) {
            throw "Token must not be empty."
        }
        [IO.File]::WriteAllText($Path, $plainValue.Trim(), [Text.UTF8Encoding]::new($false))
    } finally {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    }
}

function Quote-Argument {
    param([string]$Value)
    return '"' + $Value.Replace('"', '\"') + '"'
}

function Set-NssmValue {
    param(
        [string]$Name,
        [string]$Parameter,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Values
    )
    & $Nssm set $Name $Parameter @Values | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "nssm failed to set $Parameter for $Name"
    }
}

Assert-Administrator
if ($IntervalSeconds -le 0 -or $ChunkSeconds -le 0) {
    throw "IntervalSeconds and ChunkSeconds must be greater than zero."
}
if (($ChunkSeconds % $IntervalSeconds) -ne 0) {
    throw "ChunkSeconds must be divisible by IntervalSeconds."
}
if ($OverlapSeconds -lt 0 -or ($OverlapSeconds % $IntervalSeconds) -ne 0) {
    throw "OverlapSeconds must be zero or divisible by IntervalSeconds."
}
if ($SafetyDelaySeconds -lt 0) {
    throw "SafetyDelaySeconds must not be negative."
}
foreach ($requiredPath in ($Nssm, $Python, $Writer)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Required file was not found: $requiredPath"
    }
}
New-Item -ItemType Directory -Force -Path $SecretDirectory, $LogDirectory | Out-Null

if ($RefreshTokens -or -not (Test-Path -LiteralPath $SourceTokenFile)) {
    Write-SecretFile $SourceTokenFile "Enter the 8086 source bucket READ token"
}
if ($RefreshTokens -or -not (Test-Path -LiteralPath $DestinationTokenFile)) {
    Write-SecretFile $DestinationTokenFile "Enter the 8087 archive bucket WRITE token"
}

# LocalSystem runs the service. Limit token files to LocalSystem and administrators.
& icacls.exe $SecretDirectory /inheritance:r | Out-Null
& icacls.exe $SecretDirectory /grant:r "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to protect token directory ACL: $SecretDirectory"
}
& icacls.exe (Join-Path $SecretDirectory "*") /inheritance:r /grant:r "*S-1-5-18:F" "*S-1-5-32-544:F" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to protect token file ACLs: $SecretDirectory"
}

$startArguments = if ($InitialStart -eq "now") {
    @("--start-now")
} else {
    try {
        [DateTimeOffset]::Parse($InitialStart) | Out-Null
    } catch {
        throw "InitialStart must be 'now' or an ISO-8601 timestamp."
    }
    @("--start-time", (Quote-Argument $InitialStart))
}

$writerArguments = @(
    (Quote-Argument $Writer)
    "--source-url", (Quote-Argument $SourceUrl)
    "--source-token-file", (Quote-Argument $SourceTokenFile)
    "--source-org", (Quote-Argument $SourceOrg)
    "--source-bucket", (Quote-Argument $SourceBucket)
    "--destination-url", (Quote-Argument $DestinationUrl)
    "--destination-token-file", (Quote-Argument $DestinationTokenFile)
    "--destination-org", (Quote-Argument $DestinationOrg)
    "--destination-bucket", (Quote-Argument $DestinationBucket)
    "--measurement", (Quote-Argument $Measurement)
    "--interval-seconds", $IntervalSeconds
    "--chunk-seconds", $ChunkSeconds
    "--overlap-seconds", $OverlapSeconds
    "--safety-delay-seconds", $SafetyDelaySeconds
    "--checkpoint", (Quote-Argument $Checkpoint)
) + $startArguments
$appParameters = $writerArguments -join " "

$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($null -eq $service) {
    & $Nssm install $ServiceName $Python | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to install service $ServiceName"
    }
} else {
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
}

Set-NssmValue $ServiceName "Application" $Python
Set-NssmValue $ServiceName "AppDirectory" $ProjectRoot
Set-NssmValue $ServiceName "AppParameters" $appParameters
Set-NssmValue $ServiceName "AppStdout" $StdOut
Set-NssmValue $ServiceName "AppStderr" $StdErr
Set-NssmValue $ServiceName "AppNoConsole" "1"
Set-NssmValue $ServiceName "AppExit" "Default" "Restart"
Set-NssmValue $ServiceName "AppRestartDelay" "10000"
Set-NssmValue $ServiceName "AppRotateFiles" "1"
Set-NssmValue $ServiceName "AppRotateOnline" "1"
Set-NssmValue $ServiceName "AppRotateSeconds" "86400"
Set-NssmValue $ServiceName "AppRotateBytes" "10485760"
Set-NssmValue $ServiceName "DependOnService" "InfluxDB" "InfluxDBArchive"

& sc.exe failure $ServiceName reset= 86400 actions= restart/10000/restart/30000/restart/60000 | Out-Null
if ($DoNotStart) {
    & sc.exe config $ServiceName start= demand | Out-Null
    Write-Host "$ServiceName is configured but was not started."
} else {
    & sc.exe config $ServiceName start= auto | Out-Null
    Start-Service -Name $ServiceName
    Start-Sleep -Seconds 3
    $state = Get-Service -Name $ServiceName
    if ($state.Status -ne "Running") {
        throw "$ServiceName did not remain running. Check $StdErr"
    }
    Write-Host "$ServiceName is running."
}

Write-Host "Checkpoint: $Checkpoint"
Write-Host "Logs: $StdOut and $StdErr"
Write-Host "Tokens are stored under $SecretDirectory with restricted ACLs."
