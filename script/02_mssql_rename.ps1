# 請修改這三個變數
$ServerInstance  = "localhost"
$OldDatabaseName = "BALPS"
$NewDatabaseName = "BALPS_Ver1"

$ErrorActionPreference = "Stop"

function Quote-SqlIdentifier {
    param([string]$Name)
    "[" + $Name.Replace("]", "]]") + "]"
}

function Quote-SqlLiteral {
    param([string]$Value)
    $Value.Replace("'", "''")
}

$oldId = Quote-SqlIdentifier $OldDatabaseName
$newId = Quote-SqlIdentifier $NewDatabaseName

$oldLiteral = Quote-SqlLiteral $OldDatabaseName
$newLiteral = Quote-SqlLiteral $NewDatabaseName

# Windows Authentication
$connectionString = `
    "Server=$ServerInstance;Database=master;Integrated Security=True;TrustServerCertificate=True;Connection Timeout=30;"

$conn = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$cmd = $null
$singleUserSet = $false
$renamed = $false

try {
    $conn.Open()
    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 60

    # 檢查舊資料庫是否存在
    $cmd.CommandText = "SELECT DB_ID(N'$oldLiteral');"
    $oldDbId = $cmd.ExecuteScalar()

    if ($null -eq $oldDbId -or $oldDbId -is [DBNull]) {
        throw "找不到舊資料庫：$OldDatabaseName"
    }

    # 檢查新名稱是否已存在
    $cmd.CommandText = "SELECT DB_ID(N'$newLiteral');"
    $newDbId = $cmd.ExecuteScalar()

    if ($null -ne $newDbId -and $newDbId -isnot [DBNull]) {
        throw "新資料庫名稱已存在：$NewDatabaseName"
    }

    Write-Host "切換為 SINGLE_USER..." -ForegroundColor Yellow
    $cmd.CommandText = `
        "ALTER DATABASE $oldId SET SINGLE_USER WITH ROLLBACK IMMEDIATE;"
    [void]$cmd.ExecuteNonQuery()
    $singleUserSet = $true

    Write-Host "重新命名資料庫..." -ForegroundColor Yellow
    $cmd.CommandText = `
        "ALTER DATABASE $oldId MODIFY NAME = $newId;"
    [void]$cmd.ExecuteNonQuery()
    $renamed = $true

    Write-Host "切換回 MULTI_USER..." -ForegroundColor Yellow
    $cmd.CommandText = `
        "ALTER DATABASE $newId SET MULTI_USER;"
    [void]$cmd.ExecuteNonQuery()

    $singleUserSet = $false

    Write-Host "完成：$OldDatabaseName -> $NewDatabaseName" `
        -ForegroundColor Green
}
catch {
    if ($singleUserSet -and $null -ne $cmd) {
        try {
            $restoreId = if ($renamed) { $newId } else { $oldId }
            $cmd.CommandText = "ALTER DATABASE $restoreId SET MULTI_USER;"
            [void]$cmd.ExecuteNonQuery()
        }
        catch {
            Write-Warning "無法自動恢復 MULTI_USER，請手動檢查資料庫狀態。"
        }
    }

    throw
}
finally {
    if ($null -ne $cmd) {
        $cmd.Dispose()
    }

    if ($null -ne $conn) {
        $conn.Close()
        $conn.Dispose()
    }
}