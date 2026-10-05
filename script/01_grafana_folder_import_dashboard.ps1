[CmdletBinding()]
param (
    # Folder containing the dashboard JSON files to import.
    [string]$DashboardFolder = "H:\1_CCMS\20260924\20260930",

    # Grafana folder name. The UID is resolved from Grafana at runtime.
    [string]$FolderName = "Main"
)

$ErrorActionPreference = "Stop"


# ============================================================
# Tool Configuration
# ============================================================

# Fixed jq executable path.
$script:JqExe = "C:\project\ccms\script\jq.exe"


# ============================================================
# Grafana Configuration
# ============================================================

$GrafanaUrl = "http://localhost:3000"
$GrafanaUser = "admin"
$GrafanaPassword = "11111111"

# These names must match the Grafana datasource names.
$InfluxDatasourceName = "InfluxDB"
$MssqlDatasourceName = "MSSQL"


# ============================================================
# Helper Functions
# ============================================================

<#
.SYNOPSIS
Generates a random alphanumeric Grafana Dashboard UID.

.PARAMETER Length
Length of the generated UID.

.RETURNS
A random UID string.
#>
function New-RandomDashboardUid {

    param (
        [int]$Length = 10
    )

    $Alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    $AcceptableByteLimit =
        256 - (256 % $Alphabet.Length)

    $RandomBytes =
        New-Object byte[] 1

    $Builder =
        New-Object System.Text.StringBuilder

    $RandomNumberGenerator =
        [System.Security.Cryptography.RandomNumberGenerator]::Create()

    try {

        while ($Builder.Length -lt $Length) {

            $RandomNumberGenerator.GetBytes(
                $RandomBytes
            )

            $RandomValue =
                [int]$RandomBytes[0]


            # Rejection sampling avoids modulo bias.
            if ($RandomValue -lt $AcceptableByteLimit) {

                [void]$Builder.Append(
                    $Alphabet[
                        $RandomValue % $Alphabet.Length
                    ]
                )
            }
        }

        return $Builder.ToString()
    }
    finally {

        $RandomNumberGenerator.Dispose()
    }
}


<#
.SYNOPSIS
Validates a JSON file using jq.

.PARAMETER Path
Path to the JSON file.

.PARAMETER ErrorMessage
Error message thrown if jq validation fails.
#>
function Test-JqJsonFile {

    param (
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$ErrorMessage
    )

    $PreviousErrorActionPreference =
        $ErrorActionPreference

    try {

        $ErrorActionPreference =
            "Continue"

        & $script:JqExe empty "$Path"

        $JqExitCode =
            $LASTEXITCODE
    }
    finally {

        $ErrorActionPreference =
            $PreviousErrorActionPreference
    }


    if ($JqExitCode -ne 0) {

        throw $ErrorMessage
    }
}


# ============================================================
# Temporary Files
# ============================================================

$TempDir =
    Join-Path $env:TEMP "grafana_import"


if (
    -not (
        Test-Path `
            -LiteralPath $TempDir `
            -PathType Container
    )
) {

    New-Item `
        -ItemType Directory `
        -Path $TempDir `
        -Force |
        Out-Null
}


$PreparedFile =
    Join-Path $TempDir "dashboard_prepared.json"

$RequestFile =
    Join-Path $TempDir "dashboard_request.json"

$ResponseFile =
    Join-Path $TempDir "dashboard_response.json"

$PrepareFilterFile =
    Join-Path $TempDir "prepare_dashboard.jq"

$RequestFilterFile =
    Join-Path $TempDir "build_request.jq"

$DashboardUidMapFile =
    Join-Path $TempDir "dashboard_uid_map.json"


# ============================================================
# UTF-8 Configuration
# ============================================================

# UTF-8 without BOM for Windows PowerShell 5.1.
$Utf8NoBom =
    New-Object System.Text.UTF8Encoding($false)


# jq.exe outputs UTF-8.
# Preserve the current console encoding and restore it later.
$PreviousConsoleOutputEncoding =
    [Console]::OutputEncoding


try {

    [Console]::OutputEncoding =
        $Utf8NoBom
}
catch {

    Write-Warning (
        "Unable to change Console.OutputEncoding. " +
        "Continue with current encoding."
    )
}


# ============================================================
# Main
# ============================================================

try {

    Write-Host ""
    Write-Host "========================================"
    Write-Host "Grafana Dashboard Import"
    Write-Host "========================================"
    Write-Host ""


    # ========================================================
    # STEP 1 - Check input folder, files and commands
    # ========================================================

    if (
        -not (
            Test-Path `
                -LiteralPath $DashboardFolder `
                -PathType Container
        )
    ) {

        throw (
            "Dashboard folder not found: " +
            $DashboardFolder
        )
    }


    $DashboardFiles = @(

        Get-ChildItem `
            -LiteralPath $DashboardFolder `
            -File |

        Where-Object {
            $_.Extension -ieq ".json"
        } |

        Sort-Object Name
    )


    if ($DashboardFiles.Count -eq 0) {

        throw (
            "No dashboard JSON files found in: " +
            $DashboardFolder
        )
    }


    # --------------------------------------------------------
    # Check jq.exe fixed path
    # --------------------------------------------------------

    if (
        -not (
            Test-Path `
                -LiteralPath $script:JqExe `
                -PathType Leaf
        )
    ) {

        throw (
            "jq.exe was not found: " +
            $script:JqExe
        )
    }


    # --------------------------------------------------------
    # Test jq.exe
    # --------------------------------------------------------

    $PreviousErrorActionPreference =
        $ErrorActionPreference

    try {

        $ErrorActionPreference =
            "Continue"

        $JqVersion =
            & $script:JqExe --version

        $JqExitCode =
            $LASTEXITCODE
    }
    finally {

        $ErrorActionPreference =
            $PreviousErrorActionPreference
    }


    if ($JqExitCode -ne 0) {

        throw (
            "Unable to execute jq.exe: " +
            $script:JqExe
        )
    }


    # --------------------------------------------------------
    # Check curl.exe
    # --------------------------------------------------------

    if (
        $null -eq (
            Get-Command `
                curl.exe `
                -ErrorAction SilentlyContinue
        )
    ) {

        throw "curl.exe was not found."
    }


    Write-Host "Dashboard folder:"
    Write-Host "  $DashboardFolder"

    Write-Host ""

    Write-Host "Dashboard files:"
    Write-Host (
        "  {0} JSON file(s)" -f
        $DashboardFiles.Count
    )

    Write-Host ""

    Write-Host "jq:"
    Write-Host (
        "  Path    : {0}" -f
        $script:JqExe
    )

    Write-Host (
        "  Version : {0}" -f
        (($JqVersion -join "").Trim())
    ) -ForegroundColor Green

    Write-Host ""


    # ========================================================
    # STEP 2 - Create Grafana authentication header
    # ========================================================

    $Pair =
        "${GrafanaUser}:${GrafanaPassword}"


    $Auth =
        [Convert]::ToBase64String(
            [Text.Encoding]::ASCII.GetBytes(
                $Pair
            )
        )


    $Headers = @{

        Authorization =
            "Basic $Auth"
    }


    # ========================================================
    # STEP 3 - Check Grafana connection
    # ========================================================

    $Health =
        Invoke-RestMethod `
            -Method Get `
            -Uri "$GrafanaUrl/api/health" `
            -Headers $Headers


    Write-Host "Grafana:"

    Write-Host (
        "  Version : {0}" -f
        $Health.version
    ) -ForegroundColor Green

    Write-Host ""


    # ========================================================
    # STEP 4 - Ensure folder exists and resolve folder UID
    # ========================================================

    if (
        [string]::IsNullOrWhiteSpace(
            $FolderName
        )
    ) {

        throw "FolderName cannot be empty."
    }


    $FolderInfo =
        $null

    $FolderWasCreated =
        $false


    try {

        $FolderInfo =
            Invoke-RestMethod `
                -Method Get `
                -Uri (
                    "$GrafanaUrl/api/folders/name/" +
                    [Uri]::EscapeDataString(
                        $FolderName
                    )
                ) `
                -Headers $Headers
    }
    catch {

        $FolderLookupStatusCode =
            $null


        if (
            $null -ne $_.Exception.Response
        ) {

            $FolderLookupStatusCode =
                [int]$_.Exception.Response.StatusCode
        }


        if (
            $FolderLookupStatusCode -ne 404
        ) {

            throw (
                "Cannot look up Grafana folder '{0}'. {1}" -f
                $FolderName,
                $_.Exception.Message
            )
        }


        $FolderRequestBody = @{

            title =
                $FolderName

        } | ConvertTo-Json -Compress


        try {

            $FolderInfo =
                Invoke-RestMethod `
                    -Method Post `
                    -Uri "$GrafanaUrl/api/folders" `
                    -Headers $Headers `
                    -ContentType "application/json" `
                    -Body $FolderRequestBody


            $FolderWasCreated =
                $true
        }
        catch {

            throw (
                "Cannot create Grafana folder '{0}'. {1}" -f
                $FolderName,
                $_.Exception.Message
            )
        }
    }


    $FolderUid =
        [string]$FolderInfo.uid


    if (
        [string]::IsNullOrWhiteSpace(
            $FolderUid
        )
    ) {

        throw (
            "Grafana folder '$FolderName' did not return a UID."
        )
    }


    Write-Host "Target Grafana folder:"

    Write-Host (
        "  Name: {0}" -f
        $FolderName
    )

    Write-Host (
        "  UID : {0}" -f
        $FolderUid
    ) -ForegroundColor Green


    if ($FolderWasCreated) {

        Write-Host (
            "  Status: Created"
        ) -ForegroundColor Green
    }


    Write-Host ""


    # ========================================================
    # STEP 5 - Read current datasources
    # ========================================================

    try {

        $MssqlDatasource =
            Invoke-RestMethod `
                -Method Get `
                -Uri (
                    "$GrafanaUrl/api/datasources/name/" +
                    [Uri]::EscapeDataString(
                        $MssqlDatasourceName
                    )
                ) `
                -Headers $Headers
    }
    catch {

        throw (
            "Cannot find Grafana datasource: " +
            $MssqlDatasourceName
        )
    }


    $MssqlDatasourceUid =
        [string]$MssqlDatasource.uid


    try {

        $InfluxDatasource =
            Invoke-RestMethod `
                -Method Get `
                -Uri (
                    "$GrafanaUrl/api/datasources/name/" +
                    [Uri]::EscapeDataString(
                        $InfluxDatasourceName
                    )
                ) `
                -Headers $Headers
    }
    catch {

        throw (
            "Cannot find Grafana datasource: " +
            $InfluxDatasourceName
        )
    }


    $InfluxDatasourceUid =
        [string]$InfluxDatasource.uid


    Write-Host "Datasource Mapping:"

    Write-Host "  `${DS_MSSQL}"

    Write-Host (
        "       -> Name : {0}" -f
        $MssqlDatasource.name
    )

    Write-Host (
        "       -> UID  : {0}" -f
        $MssqlDatasourceUid
    ) -ForegroundColor Green


    Write-Host ""

    Write-Host "  `${DS_INFLUXDB}"

    Write-Host (
        "       -> Name : {0}" -f
        $InfluxDatasource.name
    )

    Write-Host (
        "       -> UID  : {0}" -f
        $InfluxDatasourceUid
    ) -ForegroundColor Green

    Write-Host ""


    # ========================================================
    # STEP 6 - Build old UID -> new UID mapping
    # ========================================================

    # Generate all new UIDs before importing any dashboard.
    # This allows links inside one dashboard to point to another
    # dashboard imported in the same run.

    $DashboardUidMap =
        @{}

    $DashboardPlans =
        @()

    $UsedUids =
        @()


    Write-Host "Dashboard UID Mapping:"


    foreach (
        $DashboardItem in
        $DashboardFiles
    ) {

        $DashboardFile =
            $DashboardItem.FullName


        # ----------------------------------------------------
        # Validate original JSON
        # ----------------------------------------------------

        Test-JqJsonFile `
            -Path $DashboardFile `
            -ErrorMessage (
                "Original Dashboard JSON is invalid: " +
                $DashboardFile
            )


        # ----------------------------------------------------
        # Read original Dashboard JSON
        # ----------------------------------------------------

        try {

            $SourceJsonText =
                [IO.File]::ReadAllText(
                    $DashboardFile,
                    [Text.Encoding]::UTF8
                )


            $SourceJson =
                $SourceJsonText |
                ConvertFrom-Json
        }
        catch {

            throw (
                "Cannot read Dashboard JSON '{0}'. {1}" -f
                $DashboardFile,
                $_.Exception.Message
            )
        }


        $SourceDashboard =
            $SourceJson


        if (
            $null -ne $SourceJson.dashboard
        ) {

            $SourceDashboard =
                $SourceJson.dashboard
        }


        $OldUid =
            [string]$SourceDashboard.uid


        if (
            [string]::IsNullOrWhiteSpace(
                $OldUid
            )
        ) {

            throw (
                (
                    "Dashboard '{0}' does not contain an original UID. " +
                    "The UID is required to update dashboard links."
                ) -f
                $DashboardFile
            )
        }


        if (
            $DashboardUidMap.ContainsKey(
                $OldUid
            )
        ) {

            throw (
                "Duplicate original Dashboard UID '{0}' found in '{1}'." -f
                $OldUid,
                $DashboardFile
            )
        }


        # ----------------------------------------------------
        # Generate unique 10-character UID
        # ----------------------------------------------------

        do {

            $NewUid =
                New-RandomDashboardUid `
                    -Length 10
        }
        while (
            $UsedUids -contains
            $NewUid
        )


        $UsedUids +=
            $NewUid


        $DashboardUidMap[$OldUid] =
            $NewUid


        $DashboardPlans +=
            [PSCustomObject]@{

                Item   = $DashboardItem
                File   = $DashboardFile
                OldUid = $OldUid
                NewUid = $NewUid
            }


        Write-Host (
            "  {0}: {1} -> {2}" -f
            $DashboardItem.Name,
            $OldUid,
            $NewUid
        )
    }


    # --------------------------------------------------------
    # Save Dashboard UID mapping JSON
    # --------------------------------------------------------

    $DashboardUidMapJson =
        $DashboardUidMap |
        ConvertTo-Json -Compress


    [IO.File]::WriteAllText(
        $DashboardUidMapFile,
        $DashboardUidMapJson,
        $Utf8NoBom
    )


    Test-JqJsonFile `
        -Path $DashboardUidMapFile `
        -ErrorMessage (
            "Generated Dashboard UID mapping JSON is invalid."
        )


    Write-Host ""


    # ========================================================
    # STEP 7 - Create jq filters
    # ========================================================

    $PrepareFilter = @'
def rewrite_dashboard_links($uidMap):
    reduce ($uidMap | to_entries[]) as $item
    (.;
         gsub("/d/" + $item.key + "/";
              "/d/" + $item.value + "/")
         |
         gsub("/d-solo/" + $item.key + "/";
              "/d-solo/" + $item.value + "/")
    );

(
    if has("dashboard") then
        .dashboard
    else
        .
    end
)
|
walk(
    if type == "string" then
        if . == "${DS_MSSQL}" then
            $mssqlUid
        elif . == "${DS_INFLUXDB}" then
            $influxUid
        else
            rewrite_dashboard_links($uidMap[0])
        end
    else
        .
    end
)
|
.id = null
|
.uid = $uid
'@


    [IO.File]::WriteAllText(
        $PrepareFilterFile,
        $PrepareFilter,
        $Utf8NoBom
    )


    $RequestFilter = @'
{
    dashboard: .,
    folderUid: $folderUid,
    overwrite: false
}
'@


    [IO.File]::WriteAllText(
        $RequestFilterFile,
        $RequestFilter,
        $Utf8NoBom
    )


    # ========================================================
    # STEP 8 - Import every dashboard
    # ========================================================

    $SuccessCount =
        0

    $FailedCount =
        0

    $FailedDashboards =
        @()

    $DashboardIndex =
        0


    foreach (
        $DashboardPlan in
        $DashboardPlans
    ) {

        $DashboardIndex++

        $DashboardItem =
            $DashboardPlan.Item

        $DashboardFile =
            $DashboardPlan.File

        $OldUid =
            [string]$DashboardPlan.OldUid

        $NewUid =
            [string]$DashboardPlan.NewUid


        try {

            Write-Host ""

            Write-Host "----------------------------------------"

            Write-Host (
                "[{0}/{1}] Importing: {2}" -f
                $DashboardIndex,
                $DashboardFiles.Count,
                $DashboardItem.Name
            )


            Write-Host (
                "  New UID: {0}" -f
                $NewUid
            )


            # ------------------------------------------------
            # Validate original JSON
            # ------------------------------------------------

            Test-JqJsonFile `
                -Path $DashboardFile `
                -ErrorMessage (
                    "Original Dashboard JSON is invalid: " +
                    $DashboardFile
                )


            # ------------------------------------------------
            # Prepare Dashboard JSON
            # ------------------------------------------------

            $PreviousErrorActionPreference =
                $ErrorActionPreference


            try {

                $ErrorActionPreference =
                    "Continue"


                $PreparedOutput =
                    & $script:JqExe `
                        --slurpfile uidMap "$DashboardUidMapFile" `
                        --arg uid "$NewUid" `
                        --arg mssqlUid "$MssqlDatasourceUid" `
                        --arg influxUid "$InfluxDatasourceUid" `
                        -f "$PrepareFilterFile" `
                        "$DashboardFile"


                $JqExitCode =
                    $LASTEXITCODE
            }
            finally {

                $ErrorActionPreference =
                    $PreviousErrorActionPreference
            }


            if ($JqExitCode -ne 0) {

                throw (
                    "Failed to prepare Dashboard JSON: " +
                    $DashboardFile
                )
            }


            [IO.File]::WriteAllText(
                $PreparedFile,
                ($PreparedOutput -join "`n"),
                $Utf8NoBom
            )


            # ------------------------------------------------
            # Validate prepared JSON
            # ------------------------------------------------

            Test-JqJsonFile `
                -Path $PreparedFile `
                -ErrorMessage (
                    "Prepared Dashboard JSON is invalid: " +
                    $DashboardFile
                )


            # ------------------------------------------------
            # Verify generated UID
            # ------------------------------------------------

            $CheckUidOutput =
                & $script:JqExe `
                    -r `
                    '.uid' `
                    "$PreparedFile"


            $JqExitCode =
                $LASTEXITCODE


            $CheckUid =
                (
                    (
                        $CheckUidOutput -join ""
                    ).Trim()
                )


            if ($JqExitCode -ne 0) {

                throw (
                    "Unable to verify Dashboard UID: " +
                    $DashboardFile
                )
            }


            if ($CheckUid -ne $NewUid) {

                throw (
                    "Dashboard UID replacement failed. " +
                    "Expected={0}, Actual={1}" -f
                    $NewUid,
                    $CheckUid
                )
            }


            if (
                $NewUid -notmatch
                '^[A-Za-z0-9]{10}$'
            ) {

                throw (
                    "Generated Dashboard UID has an invalid format: " +
                    $NewUid
                )
            }


            # ------------------------------------------------
            # Verify datasource replacement
            # ------------------------------------------------

            $PreparedRaw =
                [IO.File]::ReadAllText(
                    $PreparedFile,
                    [Text.Encoding]::UTF8
                )


            if (
                $PreparedRaw.Contains(
                    '${DS_MSSQL}'
                )
            ) {

                throw (
                    'Datasource placeholder remains: ${DS_MSSQL}'
                )
            }


            if (
                $PreparedRaw.Contains(
                    '${DS_INFLUXDB}'
                )
            ) {

                throw (
                    'Datasource placeholder remains: ${DS_INFLUXDB}'
                )
            }


            # ------------------------------------------------
            # Build Grafana API request JSON
            # ------------------------------------------------

            $PreviousErrorActionPreference =
                $ErrorActionPreference


            try {

                $ErrorActionPreference =
                    "Continue"


                $RequestOutput =
                    & $script:JqExe `
                        --arg folderUid "$FolderUid" `
                        -f "$RequestFilterFile" `
                        "$PreparedFile"


                $JqExitCode =
                    $LASTEXITCODE
            }
            finally {

                $ErrorActionPreference =
                    $PreviousErrorActionPreference
            }


            if ($JqExitCode -ne 0) {

                throw (
                    "Failed to create Grafana API request: " +
                    $DashboardFile
                )
            }


            [IO.File]::WriteAllText(
                $RequestFile,
                ($RequestOutput -join "`n"),
                $Utf8NoBom
            )


            # ------------------------------------------------
            # Validate request JSON
            # ------------------------------------------------

            Test-JqJsonFile `
                -Path $RequestFile `
                -ErrorMessage (
                    "Grafana request JSON is invalid: " +
                    $DashboardFile
                )


            # ------------------------------------------------
            # Remove old response file
            # ------------------------------------------------

            if (
                Test-Path `
                    -LiteralPath $ResponseFile
            ) {

                Remove-Item `
                    -LiteralPath $ResponseFile `
                    -Force
            }


            # ------------------------------------------------
            # Import Dashboard through Grafana API
            # ------------------------------------------------

            $PreviousErrorActionPreference =
                $ErrorActionPreference


            try {

                $ErrorActionPreference =
                    "Continue"


                $HttpStatusOutput =
                    & curl.exe `
                        --silent `
                        --show-error `
                        --output "$ResponseFile" `
                        --write-out "%{http_code}" `
                        -u "${GrafanaUser}:${GrafanaPassword}" `
                        -H "Content-Type: application/json" `
                        -H "Accept: application/json" `
                        --data-binary "@$RequestFile" `
                        "$GrafanaUrl/api/dashboards/db"


                $CurlExitCode =
                    $LASTEXITCODE
            }
            finally {

                $ErrorActionPreference =
                    $PreviousErrorActionPreference
            }


            $HttpStatus =
                (
                    (
                        $HttpStatusOutput -join ""
                    ).Trim()
                )


            $ResponseText =
                ""


            if (
                Test-Path `
                    -LiteralPath $ResponseFile
            ) {

                $ResponseText =
                    [IO.File]::ReadAllText(
                        $ResponseFile,
                        [Text.Encoding]::UTF8
                    )
            }


            if ($CurlExitCode -ne 0) {

                throw (
                    "curl.exe failed for {0}. ExitCode={1}" -f
                    $DashboardFile,
                    $CurlExitCode
                )
            }


            $HttpStatusCode =
                [int]$HttpStatus


            if (
                $HttpStatusCode -lt 200 -or
                $HttpStatusCode -ge 300
            ) {

                Write-Host (
                    "Grafana Response:"
                ) -ForegroundColor Yellow


                Write-Host (
                    $ResponseText
                ) -ForegroundColor Yellow


                throw (
                    "Grafana import failed for {0}. HTTP Status={1}" -f
                    $DashboardFile,
                    $HttpStatus
                )
            }


            # ------------------------------------------------
            # Success
            # ------------------------------------------------

            $SuccessCount++


            Write-Host (
                "IMPORT SUCCESSFUL"
            ) -ForegroundColor Green


            Write-Host (
                "  HTTP Status: {0}" -f
                $HttpStatus
            )


            Write-Host (
                "  Folder UID : {0}" -f
                $FolderUid
            )


            Write-Host (
                "  New UID    : {0}" -f
                $NewUid
            )


            Write-Host "Grafana Response:"
            Write-Host $ResponseText
        }
        catch {

            $FailedCount++


            $FailedDashboards +=
                $DashboardFile


            Write-Host (
                "IMPORT FAILED"
            ) -ForegroundColor Red


            Write-Host (
                "  File: {0}" -f
                $DashboardFile
            ) -ForegroundColor Red


            Write-Host (
                "  Error: {0}" -f
                $_.Exception.Message
            ) -ForegroundColor Red
        }
    }


    # ========================================================
    # STEP 9 - Summary
    # ========================================================

    Write-Host ""
    Write-Host "========================================"
    Write-Host "IMPORT SUMMARY"
    Write-Host "========================================"


    Write-Host (
        "Total   : {0}" -f
        $DashboardFiles.Count
    )


    Write-Host (
        "Success : {0}" -f
        $SuccessCount
    ) -ForegroundColor Green


    if ($FailedCount -eq 0) {

        Write-Host (
            "Failed  : 0"
        ) -ForegroundColor Green
    }
    else {

        Write-Host (
            "Failed  : {0}" -f
            $FailedCount
        ) -ForegroundColor Red
    }


    if ($FailedCount -gt 0) {

        Write-Host ""

        Write-Host (
            "Failed dashboard files:"
        ) -ForegroundColor Yellow


        foreach (
            $FailedDashboard in
            $FailedDashboards
        ) {

            Write-Host (
                "  {0}" -f
                $FailedDashboard
            ) -ForegroundColor Yellow
        }
    }


    Write-Host ""

    Write-Host "Temporary Files:"

    Write-Host (
        "  Prepared : {0}" -f
        $PreparedFile
    )

    Write-Host (
        "  Request  : {0}" -f
        $RequestFile
    )

    Write-Host (
        "  Response : {0}" -f
        $ResponseFile
    )

    Write-Host ""
}
catch {

    Write-Host ""
    Write-Host "========================================"

    Write-Host (
        "IMPORT PROCESS FAILED"
    ) -ForegroundColor Red

    Write-Host "========================================"
    Write-Host ""

    Write-Host (
        $_.Exception.Message
    ) -ForegroundColor Red

    Write-Host ""

    exit 1
}
finally {

    # Restore console encoding after script execution.
    try {

        [Console]::OutputEncoding =
            $PreviousConsoleOutputEncoding
    }
    catch {

        # Ignore encoding restore errors.
    }
}