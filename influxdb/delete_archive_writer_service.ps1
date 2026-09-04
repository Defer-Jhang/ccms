param(
    [string]$ServiceName = "InfluxArchiveWriter",
    [string]$ProjectRoot = "C:\project\ccms"
)

$ErrorActionPreference = "Stop"
$Nssm = Join-Path $ProjectRoot "nssm.exe"
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Run this script from an elevated PowerShell window."
}
if (-not (Test-Path -LiteralPath $Nssm)) {
    throw "NSSM was not found: $Nssm"
}

$service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($null -eq $service) {
    Write-Host "$ServiceName is not installed."
    exit 0
}
Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
& $Nssm remove $ServiceName confirm | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Unable to remove service $ServiceName"
}
Write-Host "$ServiceName was removed. Checkpoint, token files, and archive data were not deleted."
