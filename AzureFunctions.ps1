Using namespace Microsoft.WindowsAzure.Commands.Storage
Using module Az.Storage

class BlobReference {
    [string]$name
    [string]$bktype
    [string]$database
    [string]$server
    [string]$extension
    [DateTime]$bkdate

    [string] ToString() {
        return $this.name
    }
}

function Get-BlobsForServer {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$servername,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Storage.AzureStorageContext]$Context
    )

    return Get-AzStorageBlob -Context $context -Container $ContainerName -Prefix $servername
}

function Get-BlobsForDatabase {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Storage.AzureStorageContext]$Context
    )
    return Get-AzStorageBlob -Context $context -Container $ContainerName -Blob "*/$databasename/*"
}

function Get-BlobReferences {
    param (
        [parameter(ValueFromPipeline)]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs
    )
    
    $dateRegex = '(?<ServerName>[\x21-\x2e,\x30-\x7E]{1,254})\/(?<DatabaseName>[\x21-\x2e,\x30-\x7E]{1,254})\/(?<BackupType>FULL|DIFF|FULL_COPY_ONLY|LOG|LOG_COPY_ONLY)\/(?<filenamestart>\k<ServerName>_\k<DatabaseName>_\k<BackupType>)_(?<bkdate>[\d]{8}_[\d]{6})\.(?<FileExtension>bak|trn)'
    $regExCompiled = New-Object Regex $dateRegex, 'Compiled'

    [BlobReference[]]$blobCollection = @()
    
    foreach ($blob in $blobs) {
        $mymatch = $regExCompiled.Match($blob.Name)
        if ($mymatch.Success) {
            $objBlob = [BlobReference]@{
                name      = $blob.Name
                bktype    = $mymatch.Groups['BackupType'].Value
                database  = $mymatch.Groups['DatabaseName'].Value
                server    = $mymatch.Groups['ServerName'].Value
                extension = $mymatch.Groups['FileExtension'].Value
                #filenamestart = $mymatch.Groups['filenamestart'].Value
                bkdate    = [DateTime]::ParseExact($mymatch.Groups['bkdate'].Value, 'yyyyMMdd_HHmmss', [System.Globalization.CultureInfo]::InvariantCulture)
            }
            $blobCollection += $objBlob
        }
        else {
            Write-Warning "Non-conformant blob name: $($blob.Name)"
        }
    }
    
    return $blobCollection
}

function Restore-FullDiffFile {
    param (
        [string]$mostRecentFullFile,
        [string]$mostRecentDiffFile,
        [parameter(Mandatory = $true)]
        [string]$DestinationServer,
        [parameter(Mandatory = $true)]
        [string]$StorageAccountName
    )
	
    $DateStamp = (Get-Date).ToString('yyyyMMdd')
    $DestinationServerDefaultPaths = Get-DbaDefaultPath -SqlInstance $DestinationServer
    
    try {
        $HeaderInfo = Read-DbaBackupHeader -SqlInstance $DestinationServer -Path $mostRecentFullFile -AzureCredential $StorageAccountName
        $FileMapping = @{ }
        $loopCount = 0
        $DestinationDBName = $HeaderInfo.DatabaseName
            
        Foreach ($dataFile in $HeaderInfo.FileList) {
            if ($dataFile.Type -eq "D") {
                if ($loopCount -eq 0) {
                    $newFilename = $DestinationDBName + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                else {
                    $newFilename = $DestinationDBName + "_Data" + $loopCount.tostring("00") + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                
                $NewFullFilePath = [System.IO.Path]::combine($DestinationServerDefaultPaths.Data, $newFilename)
            }
            else {
                # Type eq "L"
                if ($loopCount -eq 0) {
                    $newFilename = $DestinationDBName + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                else {
                    $newFilename = $DestinationDBName + "_Log" + $loopCount.tostring("00") + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                
                $NewFullFilePath = [System.IO.Path]::combine($DestinationServerDefaultPaths.Log, $newFilename)
            }
            
            $FileMapping[$dataFile.LogicalName] = $NewFullFilePath
            $loopCount++
        }
    }
    catch {
        Write-Error $_.Exception.ToString()
        exit
    }

    try {
        $restoreFiles = @{ }
        $restoreFiles += $mostRecentFullFile

        if (-Not [string]::IsNullOrEmpty($mostRecentDiffFile)) {
            $restoreFiles += $mostRecentDiffFile
        }

        Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $DestinationDBName -Path $restoreFiles -FileMapping $FileMapping -AzureCredential $StorageAccountName -WithReplace  -NoRecovery
    }
    catch {
        Write-Error $_.Exception.ToString()
        exit
    }
}

function Restore-TRNLogs {
    param (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)]
        [string]$DestinationServer,
        [parameter(Mandatory = $true)]
        [string]$StorageAccountName,
        [string[]]$trnfiles,
        [bool]$NoRecovery = $false
    )

    foreach ($file in $trnfiles) {
        $sqlRestore = "Restore DATABASE [$databasename] FROM URL = '$file'  WITH  CREDENTIAL ='$StorageAccountName', REPLACE, NoRecovery, BLOCKSIZE = 512"
        Write-Host $sqlRestore
        Invoke-Sqlcmd -ServerInstance $DestinationServer -Database 'master' -Query $sqlRestore -Verbose
        #Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $databasename -Path $file -AzureCredential $StorageAccountName -WithReplace -BlockSize 512 -NoRecovery -Continue # -Verbose
    }

    if ($NoRecovery -ne $true) {
        Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $databasename -Recover
    }
}

function Get-BackupFinishDate {
    param (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)]
        [string]$DestinationServer
    )
    $theResult = Invoke-Sqlcmd -ServerInstance $DestinationServer -Database 'msdb' -query "WITH LastRestores AS (SELECT r.backup_set_id, RowNum = ROW_NUMBER() OVER (PARTITION BY d.Name ORDER BY r.[restore_date] DESC)
FROM master.sys.databases d INNER JOIN msdb.dbo.[restorehistory] r ON r.[destination_database_name] = d.Name
WHERE r.destination_database_name= '$databasename' )
SELECT bs.backup_finish_date FROM [LastRestores] lr INNER JOIN msdb.dbo.backupset bs ON lr.backup_set_id = bs.backup_set_id WHERE [RowNum] = 1"

    [DateTime]$backup_finish_date = $theResult[0]
    return $backup_finish_date
}

function Get-MostRecentCopyOnlyFile {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string[]]$serverList, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(ParameterSetName = 'Token')][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Blobs')]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'BlobCollection')]
        [BlobReference[]]$blobCollection,
        [switch]$AsURL
    )

    if (-not [string]::IsNullOrEmpty($SasToken) ) {
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
        $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename
    }

    if ($null -ne $blobs) {
        $blobCollection = Get-BlobReferences -blobs $blobs
    }

    $mostRecentCopy = $blobCollection | Where-Object { $_.bktype -eq 'FULL_COPY_ONLY' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
    
    if ([string]::IsNullOrEmpty($mostRecentCopy)) {
        Write-Error "Unable to find file for `$databasename: $databasename`r`n`$serverList: $serverList"
    }
	
    if ($AsURL) {
        $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
        $mostRecentCopyFile = "$($azureURL)$($mostRecentCopy.Name)"
    }
    else {
        $mostRecentCopyFile = $mostRecentCopy.Name
    }
	
    return $mostRecentCopyFile
}

function Get-MostRecentFullDiffFile {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string[]]$serverList, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(ParameterSetName = 'Token')][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Blobs')]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'BlobCollection')]
        [BlobReference[]]$blobCollection,
        [switch]$AsURL
    )
    
    if (-not [string]::IsNullOrEmpty($SasToken) ) {
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
        $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename 
    }
    
    if ($null -ne $blobs) {
        $blobCollection = Get-BlobReferences -blobs $blobs
    }
    
    $mostRecentFull = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1

    if ([string]::IsNullOrEmpty($mostRecentFull.Name)) {
        Write-Error "Could not find full backup for $database"
    }
    else {
        $mostRecentDiff = $blobCollection | Where-Object { $_.bktype -eq 'DIFF' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-object { $_.bkdate } -Descending | Select-Object -First 1
    }

    if ($AsURL) {
        $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
        $mostRecentFullFile = "$($azureURL)$($mostRecentFull.Name)"
        if ([string]::IsNullOrEmpty($mostRecentDiff.Name)) {
            $mostRecentDiffFile = [string]::Empty
        }
        else {
            $mostRecentDiffFile = "$($azureURL)$($mostRecentDiff.Name)"
        }
            
        Write-Verbose "$($mostRecentFullFile): $mostRecentFullFile`r`n$($mostRecentDiffFile): $mostRecentDiffFile"
        [Tuple[string, string]]$retvalue = New-Object "tuple[string, string]" $mostRecentFullFile, $mostRecentDiffFile
    }
    else {
        Write-Verbose "mostRecentFullFile: $($mostRecentFull.Name)`r`nmostRecentDiffFile: $($mostRecentDiff.Name)"
        [Tuple[string, string]]$retvalue = New-Object "tuple[string, string]" $mostRecentFull.Name, $mostRecentDiff.Name
    }

    return $retvalue
}

function Get-MostRecentCopyOnlyForServer {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$servername, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [switch]$AsURL
    )

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobs = Get-BlobsForServer -ContainerName $ContainerName -Context $Context -servername $servername
    $blobCollection = Get-BlobReferences -blobs $blobs
    $grouped = $blobCollection | Where-Object { $_.bktype -eq 'FULL_COPY_ONLY' } | Group-Object -Property server, database, bktype | Sort-Object { $_.bkdate } -Descending
    $mostRecentCopys = $grouped | ForEach-Object { $_.Group | Select-Object -First 1 }

    if ($AsURL) {
        $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
        $mostRecentCopyFiles = $mostRecentCopys | ForEach-Object { "$($azureURL)$($_.Name)" }
    }
    else {
        $mostRecentCopyFiles = $mostRecentCopys | ForEach-Object { $_.Name }
    }

    return $mostRecentCopyFiles
}

function Get-MostRecentFullDiffFilesForServer {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$servername, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [switch]$AsURL
    )

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobs = Get-BlobsForServer -ContainerName $ContainerName -Context $Context -servername $servername
    $blobCollection = Get-BlobReferences -blobs $blobs
    $grouped = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -or $_.bktype -eq 'DIFF' } | Group-Object -Property database | Sort-Object { $_.bkdate } -Descending

    if ($AsURL) {
        $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
        $retval = $grouped | ForEach-Object {
            $mostRecentFull = $_.Group | Where-Object { $_.bktype -eq 'FULL' } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
            $mostRecentDiff = $_.Group | Where-Object { $_.bktype -eq 'DIFF' -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
            New-Object "tuple[string, string]" "$($azureURL)$($mostRecentFull.Name)", "$($azureURL)$($mostRecentDiff.Name)"
        }
    }
    else {
        $retval = $grouped | ForEach-Object {
            $mostRecentFull = $_.Group | Where-Object { $_.bktype -eq 'FULL' } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
            $mostRecentDiff = $_.Group | Where-Object { $_.bktype -eq 'DIFF' -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
            New-Object "tuple[string, string]" $mostRecentFull.Name, $mostRecentDiff.Name
        }
    }

    return $retval
}

function Restore-LatestDatabase {
    param
    (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string[]]$serverList, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)]
        [string]$DestinationServer,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [string]$TargetDatabaseName = "",
        [switch]$UseCopyOnly#,
        #[switch]$NoRecovery
    )

    $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename
    $blobCollection = Get-BlobReferences -blobs $blobs
    Write-Verbose "blob count: $($blobCollection.Count())`r`n `$databasename: $databasename`r`n`$serverList: $serverList"
    
    if ($UseCopyOnly) {
        $mostRecentCopy = $blobCollection | Where-Object { $_.bktype -eq 'FULL_COPY_ONLY' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
        if ([string]::IsNullOrEmpty($mostRecentCopy)) {
            Write-Error "Unable to find file for `$databasename: $databasename`r`n`$serverList: $serverList"
        }

        $mostRecentCopyFile = "$($azureURL)$($mostRecentCopy.Name)"
        Write-Verbose "$($mostRecentCopyFile): $mostRecentCopyFile"
        Restore-FullDiffFile -mostRecentFullFile $mostRecentCopyFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName
    }
    else {
        $mostRecentFull = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
        $mostRecentDiff = $blobCollection | Where-Object { $_.bktype -eq 'DIFF' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-object { $_.bkdate } -Descending | Select-Object -First 1
        $mostRecentFullFile = "$($azureURL)$($mostRecentFull.Name)"
        $mostRecentDiffFile = "$($azureURL)$($mostRecentDiff.Name)"

        Write-Verbose "$($mostRecentFullFile): $mostRecentFullFile`r`n$($mostRecentDiffFile): $mostRecentDiffFile"
        Restore-FullDiffFile -mostRecentFullFile $mostRecentFullFile -mostRecentDiffFile $mostRecentDiffFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName
    }

    $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename
    $blobCollection = Get-BlobReferences -blobs $blobs
    $StartDateTime = Get-BackupFinishDate -databasename $databasename -DestinationServer $DestinationServer
    $trnFiles = $blobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $StartDateTime } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name }
    Restore-TRNLogs -databasename $databasename -DestinationServer $DestinationServer -trnfiles $trnfiles -StorageAccountName $StorageAccountName
}

function Remove-OldBlobs {
    param
    (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [int]$keepMinimumCount = 4, # keep at least this many backups
        [int]$keepMinimumDays = 30 # only delete older than this number of days
    )

    $deleteOlderThan = (Get-Date).AddDays( - $keepMinimumDays)
    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobs = Get-AzStorageBlob -Context $context -Container $ContainerName
    $blobCollection = Get-BlobReferences -blobs $blobs
    $grouped = $blobCollection | Group-Object -Property server, database, bktype | Where-Object { $_.count -gt $keepMinimumCount } 
    $blobsToDelete = $grouped | ForEach-Object { $_.Group | Select-Object -First ($_.Count - $keepMinimumCount) | Where-Object { $_.bkdate -lt $deleteOlderThan } }
    $blobsToDelete | ForEach-Object { Remove-AzStorageBlob -Blob $_.Name -Container $ContainerName -Context $context -WhatIf }
}