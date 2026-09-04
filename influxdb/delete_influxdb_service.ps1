$svc = Get-WmiObject -Class Win32_Service -Filter "Name='InfluxDB'"
if ($svc) { $svc.StopService() | Out-Null; Start-Sleep 2; $svc.Delete() }