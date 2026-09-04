param(
    [string]$ServiceName = "InfluxDBArchive",
    [string]$ArchiveRoot = "E:\InfluxDBArchive",
    [string]$BindAddress = "127.0.0.1:8087",
    [string]$ProjectRoot = "C:\project\ccms"
)

$ErrorActionPreference = "Stop"
$Nssm = Join-Path $ProjectRoot "nssm.exe"
$Influxd = Join-Path $ProjectRoot "influxdb\influxd.exe"
$EnginePath = Join-Path $ArchiveRoot "engine"
$BoltPath = Join-Path $ArchiveRoot "influxd.bolt"
$SqlitePath = Join-Path $ArchiveRoot "influxd.sqlite"
$LogPath = Join-Path $ArchiveRoot "log"
$StdOut = Join-Path $LogPath "influxdb-archive.out.log"
$StdErr = Join-Path $LogPath "influxdb-archive.err.log"

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "Run this script from an elevated PowerShell window."
    }
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
if (-not (Test-Path -LiteralPath $Nssm)) {
    throw "NSSM was not found: $Nssm"
}
if (-not (Test-Path -LiteralPath $Influxd)) {
    throw "influxd.exe was not found: $Influxd"
}

New-Item -ItemType Directory -Force -Path $EnginePath, $LogPath | Out-Null

$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($null -eq $service) {
    & $Nssm install $ServiceName $Influxd | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to install service $ServiceName"
    }
} else {
    Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
}

$arguments = @(
    "--http-bind-address=$BindAddress"
    "--engine-path=$EnginePath"
    "--bolt-path=$BoltPath"
    "--sqlite-path=$SqlitePath"
) -join " "

Set-NssmValue $ServiceName "Application" $Influxd
Set-NssmValue $ServiceName "AppDirectory" (Split-Path -Parent $Influxd)
Set-NssmValue $ServiceName "AppParameters" $arguments
Set-NssmValue $ServiceName "AppStdout" $StdOut
Set-NssmValue $ServiceName "AppStderr" $StdErr
Set-NssmValue $ServiceName "AppNoConsole" "1"
Set-NssmValue $ServiceName "AppExit" "Default" "Restart"
Set-NssmValue $ServiceName "AppRestartDelay" "5000"
Set-NssmValue $ServiceName "AppRotateFiles" "1"
Set-NssmValue $ServiceName "AppRotateOnline" "1"
Set-NssmValue $ServiceName "AppRotateSeconds" "86400"
Set-NssmValue $ServiceName "AppRotateBytes" "10485760"

& sc.exe config $ServiceName start= auto | Out-Null
& sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/15000/restart/60000 | Out-Null

Start-Service -Name $ServiceName
$healthy = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try {
        $health = Invoke-RestMethod -Uri "http://$BindAddress/health" -TimeoutSec 2
        if ($health.status -eq "pass") {
            $healthy = $true
            break
        }
    } catch {
        Start-Sleep -Seconds 1
    }
}
if (-not $healthy) {
    throw "$ServiceName started but did not become healthy. Check $StdErr"
}

Write-Host "$ServiceName is healthy at http://$BindAddress"
Write-Host "Engine: $EnginePath"
Write-Host "Metadata: $BoltPath"
Write-Host "Next: open http://$BindAddress and initialize org/bucket before installing the archive writer."

