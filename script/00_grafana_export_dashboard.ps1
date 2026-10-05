param (
    # Use root for dashboards directly under Grafana's root folder.
    # A named folder exports only dashboards directly in that folder;
    # dashboards in child folders are excluded.
    [string]$FolderName = "Root"
)

$ErrorActionPreference = "Stop"

# ============================================================
# Grafana configuration
# ============================================================

$GrafanaUrl = "http://localhost:3000"

$GrafanaUser = "admin"
$GrafanaPassword = "11111111"

$OutputDir = ".\dashboard"


# ============================================================
# Create output directory
# ============================================================

New-Item `
    -ItemType Directory `
    -Path $OutputDir `
    -Force | Out-Null


# ============================================================
# Create Basic Authentication header
# ============================================================

$pair = "${GrafanaUser}:${GrafanaPassword}"

$auth = [Convert]::ToBase64String(
    [Text.Encoding]::ASCII.GetBytes($pair)
)

$headers = @{
    Authorization = "Basic $auth"
}


# ============================================================
# Function:
# Create a safe Grafana input variable name.
#
# Example:
#   InfluxDB        -> DS_INFLUXDB
#   MSSQL Production -> DS_MSSQL_PRODUCTION
# ============================================================

function Get-DatasourceInputName {

    param (
        [Parameter(Mandatory = $true)]
        [string]$DatasourceName
    )

    $safeName = $DatasourceName `
        -replace '[^A-Za-z0-9]', '_'

    $safeName = $safeName.Trim('_').ToUpper()

    if ([string]::IsNullOrWhiteSpace($safeName)) {
        $safeName = "DATASOURCE"
    }

    return "DS_$safeName"
}


# ============================================================
# Function:
# Select dashboards from one folder level only.
# ============================================================

function Get-DashboardsForFolder {

    param (
        [Parameter(Mandatory = $true)]
        [object[]]$Dashboards,

        [Parameter(Mandatory = $true)]
        [string]$FolderName,

        [Parameter(Mandatory = $true)]
        [string]$GrafanaUrl,

        [Parameter(Mandatory = $true)]
        [hashtable]$Headers
    )

    $FolderName = $FolderName.Trim()

    if ([string]::IsNullOrWhiteSpace($FolderName)) {
        $FolderName = "root"
    }

    if ($FolderName -ieq "root") {
        Write-Host "Folder filter: root (direct root dashboards only; subfolders skipped)." -ForegroundColor Cyan

        return @(
            $Dashboards | Where-Object {
                $FolderUid = [string]$_.folderUid
                $FolderTitle = [string]$_.folderTitle
                $FolderId = $null
                $FolderIdProperty = $_.PSObject.Properties["folderId"]

                if (
                    $null -ne $FolderIdProperty -and
                    -not [string]::IsNullOrWhiteSpace([string]$FolderIdProperty.Value)
                ) {
                    $FolderId = [int]$FolderIdProperty.Value
                }

                $IsRoot = [string]::IsNullOrWhiteSpace($FolderUid)

                if ($null -ne $FolderId -and $FolderId -ne 0) {
                    $IsRoot = $false
                }

                # Grafana commonly reports the root folder as General.
                if (
                    -not [string]::IsNullOrWhiteSpace($FolderTitle) -and
                    $FolderTitle -ine "General"
                ) {
                    $IsRoot = $false
                }

                $IsRoot
            }
        )
    }

    # The dashboard search response already contains folderTitle on most
    # Grafana versions. Prefer it so a folder endpoint is not required.
    $DashboardsWithMatchingTitle = @(
        $Dashboards | Where-Object {
            [string]$_.folderTitle -ieq $FolderName
        }
    )

    if ($DashboardsWithMatchingTitle.Count -gt 0) {
        Write-Host (
            "Folder filter: {0} (direct dashboards only; subfolders skipped)." -f
            $FolderName
        ) -ForegroundColor Cyan

        return $DashboardsWithMatchingTitle
    }

    # Some Grafana versions omit folder fields from /api/search?type=dash-db.
    # Read the dashboard metadata as a fallback so folders still work.
    $DashboardsFromMetadata = New-Object System.Collections.ArrayList

    foreach ($Dashboard in $Dashboards) {
        $DashboardUid = [string]$Dashboard.uid

        if ([string]::IsNullOrWhiteSpace($DashboardUid)) {
            continue
        }

        try {
            $EncodedDashboardUid = [System.Uri]::EscapeDataString($DashboardUid)
            $DashboardDetail = Invoke-RestMethod `
                -Method Get `
                -Uri "$GrafanaUrl/api/dashboards/uid/$EncodedDashboardUid" `
                -Headers $Headers

            if (
                $null -ne $DashboardDetail.meta -and
                [string]$DashboardDetail.meta.folderTitle -ieq $FolderName
            ) {
                [void]$DashboardsFromMetadata.Add($Dashboard)
            }
        }
        catch {
            Write-Host (
                "Could not read folder metadata for dashboard '{0}': {1}" -f
                $Dashboard.title,
                $_.Exception.Message
            ) -ForegroundColor Yellow
        }
    }

    if ($DashboardsFromMetadata.Count -gt 0) {
        Write-Host (
            "Folder filter: {0} (direct dashboards only; subfolders skipped)." -f
            $FolderName
        ) -ForegroundColor Cyan

        return @($DashboardsFromMetadata)
    }

    Write-Host (
        "No dashboards were found directly in folder '{0}'. The folder may be empty, or FolderName is not an exact folder title." -f
        $FolderName
    ) -ForegroundColor Yellow

    return @()
}


# ============================================================
# Function:
# Register a datasource as a Grafana import input.
#
# Return example:
#   ${DS_INFLUXDB}
# ============================================================

function Register-DatasourceInput {

    param (
        [Parameter(Mandatory = $true)]
        $Datasource,

        [Parameter(Mandatory = $true)]
        [hashtable]$UsedInputs,

        [Parameter(Mandatory = $true)]
        [hashtable]$InputNameByUid
    )

    $inputName = $InputNameByUid[$Datasource.uid]

    if (-not $UsedInputs.ContainsKey($inputName)) {

        $pluginName = $Datasource.type

        if (
            $Datasource.PSObject.Properties.Name -contains "typeName" -and
            -not [string]::IsNullOrWhiteSpace($Datasource.typeName)
        ) {
            $pluginName = $Datasource.typeName
        }

        $UsedInputs[$inputName] = [PSCustomObject][ordered]@{
            name        = $inputName
            label       = $Datasource.name
            description = ""
            type        = "datasource"
            pluginId    = $Datasource.type
            pluginName  = $pluginName
        }
    }

    # Create literal:
    #
    # ${DS_INFLUXDB}

    return ('${' + $inputName + '}')
}


# ============================================================
# Function:
# Recursively search the dashboard JSON and replace
# datasource UID/name with Grafana import placeholders.
#
# Modern Grafana example:
#
# Before:
# "datasource": {
#     "type": "influxdb",
#     "uid": "P123456789"
# }
#
# After:
# "datasource": {
#     "type": "influxdb",
#     "uid": "${DS_INFLUXDB}"
# }
#
# Legacy Grafana:
#
# "datasource": "InfluxDB"
#
# becomes:
#
# "datasource": "${DS_INFLUXDB}"
# ============================================================

function Convert-DatasourceReferences {

    param (
        [Parameter(Mandatory = $false)]
        $Node,

        [Parameter(Mandatory = $true)]
        [hashtable]$DatasourceByUid,

        [Parameter(Mandatory = $true)]
        [hashtable]$DatasourceByName,

        [Parameter(Mandatory = $true)]
        [hashtable]$UsedInputs,

        [Parameter(Mandatory = $true)]
        [hashtable]$InputNameByUid
    )

    if ($null -eq $Node) {
        return
    }


    # --------------------------------------------------------
    # Handle arrays
    # --------------------------------------------------------

    if (
        $Node -is [System.Array] -or
        $Node -is [System.Collections.ArrayList]
    ) {

        foreach ($item in $Node) {

            Convert-DatasourceReferences `
                -Node $item `
                -DatasourceByUid $DatasourceByUid `
                -DatasourceByName $DatasourceByName `
                -UsedInputs $UsedInputs `
                -InputNameByUid $InputNameByUid
        }

        return
    }


    # --------------------------------------------------------
    # Skip simple values
    # --------------------------------------------------------

    if (
        $Node -is [string] -or
        $Node -is [ValueType]
    ) {
        return
    }


    # --------------------------------------------------------
    # Handle PSCustomObject
    # --------------------------------------------------------

    if ($Node -is [PSCustomObject]) {

        $properties = @($Node.PSObject.Properties)

        foreach ($property in $properties) {

            # =================================================
            # Found a datasource property
            # =================================================

            if ($property.Name -eq "datasource") {

                $datasourceValue = $property.Value


                # ------------------------------------------------
                # Legacy datasource format:
                #
                # "datasource": "InfluxDB"
                # ------------------------------------------------

                if ($datasourceValue -is [string]) {

                    $datasource = $null

                    if (
                        $DatasourceByUid.ContainsKey(
                            $datasourceValue
                        )
                    ) {

                        $datasource =
                            $DatasourceByUid[$datasourceValue]
                    }
                    elseif (
                        $DatasourceByName.ContainsKey(
                            $datasourceValue
                        )
                    ) {

                        $datasource =
                            $DatasourceByName[$datasourceValue]
                    }


                    if ($null -ne $datasource) {

                        $placeholder =
                            Register-DatasourceInput `
                                -Datasource $datasource `
                                -UsedInputs $UsedInputs `
                                -InputNameByUid $InputNameByUid

                        $property.Value = $placeholder
                    }
                }


                # ------------------------------------------------
                # Modern datasource format:
                #
                # "datasource": {
                #     "type": "influxdb",
                #     "uid": "xxxx"
                # }
                # ------------------------------------------------

                elseif (
                    $null -ne $datasourceValue -and
                    $datasourceValue -is [PSCustomObject]
                ) {

                    $uidProperty =
                        $datasourceValue.PSObject.Properties["uid"]

                    if ($null -ne $uidProperty) {

                        $uid = [string]$uidProperty.Value

                        if (
                            $DatasourceByUid.ContainsKey($uid)
                        ) {

                            $datasource =
                                $DatasourceByUid[$uid]

                            $placeholder =
                                Register-DatasourceInput `
                                    -Datasource $datasource `
                                    -UsedInputs $UsedInputs `
                                    -InputNameByUid $InputNameByUid

                            $uidProperty.Value = $placeholder
                        }
                    }
                }
            }


            # =================================================
            # Continue recursive scan
            # =================================================

            Convert-DatasourceReferences `
                -Node $property.Value `
                -DatasourceByUid $DatasourceByUid `
                -DatasourceByName $DatasourceByName `
                -UsedInputs $UsedInputs `
                -InputNameByUid $InputNameByUid
        }
    }
}


# ============================================================
# Main
# ============================================================

try {

    Write-Host ""
    Write-Host "Connecting to Grafana..."
    Write-Host "URL: $GrafanaUrl"
    Write-Host ""


    # ========================================================
    # Get all Grafana datasources
    # ========================================================

    Write-Host "Reading datasources..."

    $datasources = Invoke-RestMethod `
        -Method Get `
        -Uri "$GrafanaUrl/api/datasources" `
        -Headers $headers


    # ========================================================
    # Build datasource lookup tables
    # ========================================================

    $DatasourceByUid = @{}
    $DatasourceByName = @{}

    $InputNameByUid = @{}

    $reservedInputNames = @{}


    foreach ($ds in $datasources) {

        if (
            -not [string]::IsNullOrWhiteSpace($ds.uid)
        ) {
            $DatasourceByUid[$ds.uid] = $ds
        }

        if (
            -not [string]::IsNullOrWhiteSpace($ds.name)
        ) {
            $DatasourceByName[$ds.name] = $ds
        }


        # ----------------------------------------------------
        # Generate unique DS_xxx variable name
        # ----------------------------------------------------

        $baseName =
            Get-DatasourceInputName `
                -DatasourceName $ds.name

        $inputName = $baseName

        $counter = 2

        while (
            $reservedInputNames.ContainsKey($inputName)
        ) {

            $inputName =
                "${baseName}_${counter}"

            $counter++
        }

        $reservedInputNames[$inputName] = $true

        $InputNameByUid[$ds.uid] = $inputName


        Write-Host (
            "Datasource: {0} | UID: {1} | Input: {2}" `
            -f $ds.name, $ds.uid, $inputName
        )
    }


    Write-Host ""


    # ========================================================
    # Get all dashboards
    # ========================================================

    Write-Host "Reading dashboards..."

    $allDashboards = Invoke-RestMethod `
        -Method Get `
        -Uri "$GrafanaUrl/api/search?type=dash-db" `
        -Headers $headers

    $dashboards = @(
        Get-DashboardsForFolder `
            -Dashboards $allDashboards `
            -FolderName $FolderName `
            -GrafanaUrl $GrafanaUrl `
            -Headers $headers
    )


    Write-Host (
        "Found {0} dashboard(s) in '{1}'." `
        -f $dashboards.Count, $FolderName
    )

    Write-Host ""


    # ========================================================
    # Export dashboards
    # ========================================================

    foreach ($item in $dashboards) {

        try {

            Write-Host "----------------------------------------"
            Write-Host "Exporting: $($item.title)"


            # ------------------------------------------------
            # Get dashboard JSON
            # ------------------------------------------------

            $result = Invoke-RestMethod `
                -Method Get `
                -Uri "$GrafanaUrl/api/dashboards/uid/$($item.uid)" `
                -Headers $headers


            $dashboard = $result.dashboard


            # ------------------------------------------------
            # Remove Grafana internal database ID
            # ------------------------------------------------

            $dashboard.id = $null


            # ------------------------------------------------
            # Preserve existing non-datasource inputs
            #
            # Example:
            # constant variables from externally shared
            # dashboards will remain.
            # ------------------------------------------------

            $preservedInputs = @()

            $existingInputsProperty =
                $dashboard.PSObject.Properties["__inputs"]

            if ($null -ne $existingInputsProperty) {

                foreach (
                    $input in @($existingInputsProperty.Value)
                ) {

                    if (
                        $input.type -ne "datasource"
                    ) {
                        $preservedInputs += $input
                    }
                }
            }


            # ------------------------------------------------
            # Track datasources used by this dashboard
            # ------------------------------------------------

            $UsedInputs = @{}


            # ------------------------------------------------
            # Convert datasource UID to ${DS_xxx}
            # ------------------------------------------------

            Convert-DatasourceReferences `
                -Node $dashboard `
                -DatasourceByUid $DatasourceByUid `
                -DatasourceByName $DatasourceByName `
                -UsedInputs $UsedInputs `
                -InputNameByUid $InputNameByUid


            # ------------------------------------------------
            # Generate Grafana __inputs
            # ------------------------------------------------

            $generatedInputs = @(
                $UsedInputs.Values |
                Sort-Object label
            )


            $allInputs = @()

            $allInputs += $preservedInputs
            $allInputs += $generatedInputs


            # ------------------------------------------------
            # Add or replace __inputs
            # ------------------------------------------------

            if (
                $dashboard.PSObject.Properties.Name `
                    -contains "__inputs"
            ) {

                $dashboard.__inputs = $allInputs
            }
            else {

                $dashboard |
                    Add-Member `
                        -NotePropertyName "__inputs" `
                        -NotePropertyValue $allInputs
            }


            # ------------------------------------------------
            # Show detected datasource
            # ------------------------------------------------

            if ($generatedInputs.Count -gt 0) {

                Write-Host "Datasource input(s):"

                foreach ($input in $generatedInputs) {

                    Write-Host (
                        "  {0} -> {1}" `
                        -f $input.name, $input.label
                    )
                }
            }
            else {

                Write-Host "No fixed datasource references found."
            }


            # ------------------------------------------------
            # Create safe filename
            # ------------------------------------------------

            $fileName = $item.title

            foreach (
                $char in [IO.Path]::GetInvalidFileNameChars()
            ) {
                $fileName =
                    $fileName.Replace($char, "_")
            }


            $filePath = Join-Path `
                $OutputDir `
                "$fileName.json"


            # ------------------------------------------------
            # Save JSON
            # ------------------------------------------------

            $dashboard |
                ConvertTo-Json -Depth 100 |
                Set-Content `
                    -Path $filePath `
                    -Encoding UTF8


            Write-Host "Saved: $filePath"
            Write-Host ""
        }
        catch {

            Write-Host ""
            Write-Host (
                "ERROR exporting dashboard: {0}" `
                -f $item.title
            )

            Write-Host $_.Exception.Message
            Write-Host ""
        }
    }


    Write-Host "========================================"
    Write-Host "Export completed."
    Write-Host "Output directory:"
    Write-Host $OutputDir
    Write-Host "========================================"
}
catch {

    Write-Host ""
    Write-Host "Export failed."
    Write-Host $_.Exception.Message
    Write-Host ""
}
