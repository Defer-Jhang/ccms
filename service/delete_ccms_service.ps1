$svc = Get-WmiObject -Class Win32_Service -Filter "Name='CCMS'"
if ($svc) { $svc.StopService() | Out-Null; Start-Sleep 2; $svc.Delete() }