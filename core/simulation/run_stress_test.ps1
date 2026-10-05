$StartTime = "2025-06-17T19:00:00.000"
$EndTime = "2026-09-09T01:00:00.000"
$QRCodeOrigin = "2024-09-07T07:00:00.000"

python .\influx_stress_writer.py `
  --start-time $StartTime `
  --end-time $EndTime `
  --end-extension-days 0 `
  --qrcode-start-time $QRCodeOrigin `
  --qrcode-mode cycle `
  --confirm-large-run