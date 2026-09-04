$GrafanaUrl = "http://localhost:3000"

$GrafanaUser = "admin"
$GrafanaPassword = "11111111"

$OutputDir = "C:\GrafanaExport"

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null


$pair = "${GrafanaUser}:${GrafanaPassword}"

$auth = [Convert]::ToBase64String(
    [Text.Encoding]::ASCII.GetBytes($pair)
)

$headers = @{
    Authorization = "Basic $auth"
}


# Get all dashboards
$dashboards = Invoke-RestMethod `
    -Method Get `
    -Uri "$GrafanaUrl/api/search?type=dash-db" `
    -Headers $headers


foreach ($item in $dashboards) {

    Write-Host "Exporting: $($item.title)"

    $result = Invoke-RestMethod `
        -Method Get `
        -Uri "$GrafanaUrl/api/dashboards/uid/$($item.uid)" `
        -Headers $headers


    # Get latest dashboard JSON
    $dashboard = $result.dashboard


    # Remove internal database ID
    $dashboard.id = $null


    # Create safe filename
    $fileName = $item.title

    foreach ($char in [IO.Path]::GetInvalidFileNameChars()) {
        $fileName = $fileName.Replace($char, "_")
    }


    $filePath = Join-Path `
        $OutputDir `
        "$fileName.json"


    $dashboard |
        ConvertTo-Json -Depth 100 |
        Set-Content `
            -Path $filePath `
            -Encoding UTF8


    Write-Host "Saved: $filePath"
}


Write-Host ""
Write-Host "Export completed."