# ========= Settings =========
$ServiceName = "InfluxDB"
$NSSM    = "C:\project\ccms\nssm.exe"

$Username = ".\defer"
$Password = "5;5p4wu/6"

$BaseDir = "C:\project\ccms\influxdb"
$BatFile = Join-Path $BaseDir "run.bat"

$LogDir  = Join-Path $BaseDir "logs"
$StdOut  = Join-Path $LogDir  "influxdb.out.log"
$StdErr  = Join-Path $LogDir  "influxdb.err.log"

# ========= Admin check =========
$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  Write-Error "Please use administrator to execute script"; exit 1
}

# ========= Validate paths =========
if (-not (Test-Path $NSSM))    { Write-Error "Not found NSSM：$NSSM"; exit 1 }
if (-not (Test-Path $BaseDir)) { Write-Error "Not found project dictionary：$BaseDir"; exit 1 }
if (-not (Test-Path $BatFile)) { Write-Error "Not found bat file：$BatFile"; exit 1 }
if (-not (Test-Path $LogDir))  { New-Item -Type Directory -Path $LogDir | Out-Null }

# ========= helper：set =========
function Set-NssmSafe {
    param(
        [string]$ServiceName,
        [string]$Param,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Values
    )
    try {
        # 組成: nssm set <svc> <param> <value1> <value2> ...
        & $NSSM set $ServiceName $Param @Values | Out-Null
        Write-Host "nssm set $ServiceName $Param $($Values -join ' ')"
    } catch {
        Write-Host "$Param not support for NSSM"
    }
}

# ========= Install or update =========
$exists = sc.exe query $ServiceName 2>$null | Select-String -Pattern "SERVICE_NAME" -Quiet
if (-not $exists) {
  Write-Host "Install Service $ServiceName ..."
  & $NSSM install $ServiceName "$env:ComSpec" "/c `"$BatFile`""
} else {
  Write-Host "Service $ServiceName exist. Update setting ..."
}

# ---- NSSM settings ----
Set-NssmSafe $ServiceName "AppDirectory"  $BaseDir
Set-NssmSafe $ServiceName "AppStdout"     $StdOut
Set-NssmSafe $ServiceName "AppStderr"     $StdErr
Set-NssmSafe $ServiceName "AppNoConsole"  "1"

# AppExit
Set-NssmSafe $ServiceName "AppExit" "Default" "Restart"

# Restart Delay
Set-NssmSafe $ServiceName "AppRestartDelay" "1000"

# Stop Setting
Set-NssmSafe $ServiceName "AppStopMethodSkip" "0"
Set-NssmSafe $ServiceName "AppStopMethodConsole" "15000"
Set-NssmSafe $ServiceName "AppStopMethodWindow"  "15000"
Set-NssmSafe $ServiceName "AppStopMethodThreads" "15000"

# ---- SCM settings ----
sc.exe config $ServiceName obj= $Username password= $Password start= auto | Out-Null
sc.exe failure $ServiceName reset= 0 actions= restart/5000/restart/5000/restart/5000 | Out-Null

# ========= Smoke test =========
Write-Host "Service Restart ..."
sc.exe stop  $ServiceName | Out-Null
Start-Sleep -Seconds 2
sc.exe start $ServiceName | Out-Null
Start-Sleep -Seconds 2
sc.exe query $ServiceName

Write-Host "`Create InfluxDB Service Done"