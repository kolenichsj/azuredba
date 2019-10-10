Using namespace Microsoft.WindowsAzure.Commands.Storage
Using module Az.Storage

class BlobReference {
    [string]$name
    [string]$bktype
    [string]$database
    [string]$server
    [string]$extension
    [DateTime]$bkdate

    [string] ToString()
    {
        return $this.name
    }
}

function Get-BlobsForServer {
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$serverename,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Storage.AzureStorageContext]$Context
    )

   return Get-AzStorageBlob -Context $context -Container $ContainerName -Prefix $serverename
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
            name     = $blob.Name
            bktype   = $mymatch.Groups['BackupType'].Value
            database = $mymatch.Groups['DatabaseName'].Value
            server   = $mymatch.Groups['ServerName'].Value
            extension = $mymatch.Groups['FileExtension'].Value
            #filenamestart = $mymatch.Groups['filenamestart'].Value
            bkdate   = [DateTime]::ParseExact($mymatch.Groups['bkdate'].Value, 'yyyyMMdd_HHmmss', [System.Globalization.CultureInfo]::InvariantCulture)
        }
        $blobCollection += $objBlob
	}
    else {
        Write-Warning "Non-conformant blob name: $($blob.Name)"}
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
    
    try
    {
        $HeaderInfo = Read-DbaBackupHeader -SqlInstance $DestinationServer -Path $mostRecentFullFile -AzureCredential $StorageAccountName
        $FileMapping = @{}
        $loopCount = 0
        $DestinationDBName = $HeaderInfo.DatabaseName
            
        Foreach ($dataFile in $HeaderInfo.FileList) 
        {
            if ($dataFile.Type -eq "D")
            {
                if ($loopCount-eq 0)
                {
                    $newFilename = $DestinationDBName + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                else
                {
                    $newFilename = $DestinationDBName + "_Data" + $loopCount.tostring("00") + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                
                $NewFullFilePath = [System.IO.Path]::combine($DestinationServerDefaultPaths.Data, $newFilename)
            }
            else # Type eq "L"
            {
                if ($loopCount-eq 0)
                {
                    $newFilename = $DestinationDBName + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                else
                {
                    $newFilename = $DestinationDBName + "_Log" + $loopCount.tostring("00") + "_" + $DateStamp + [System.IO.Path]::GetExtension($dataFile.PhysicalName)
                }
                
                $NewFullFilePath = [System.IO.Path]::combine($DestinationServerDefaultPaths.Log, $newFilename)
            }
            
            $FileMapping[$dataFile.LogicalName]=$NewFullFilePath
            $loopCount++
        }
    }
    catch 
    {
        Write-Error $_.Exception.ToString()
        exit
    }

    try
    {
        Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $DestinationDBName -Path $mostRecentFullFile -FileMapping $FileMapping -AzureCredential $StorageAccountName -WithReplace  -NoRecovery
        if ('' -ne $mostRecentDiffFile) {
		    Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $DestinationDBName -Path $mostRecentDiffFile -FileMapping $FileMapping -AzureCredential $StorageAccountName
		}
    }
    catch 
    {
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

    if ($NoRecovery -ne $true){
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

function Restore-LatestDatabase{
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
        [string]$TargetDatabaseName="",
        [switch]$UseCopyOnly#,
        #[switch]$NoRecovery
    )

    $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobCollection = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename | Get-BlobReferences

    if ($UseCopyOnly){
        $mostRecentCopy = $blobCollection | Where-Object {$_.bktype -eq 'FULL_COPY_ONLY' -and $_.database -eq $databasename -and $serverList.Contains($_.server)} | Sort-Object {$_.bkdate} -Descending | Select-Object -First 1
        $mostRecentCopyFile = "$($azureURL)$($mostRecentCopy.Name)"
        Restore-FullDiffFile -mostRecentFullFile $mostRecentCopyFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName
    }
    else{
        $mostRecentFull = $blobCollection | Where-Object {$_.bktype -eq 'FULL' -and $_.database -eq $databasename -and $serverList.Contains($_.server)} | Sort-Object {$_.bkdate} -Descending | Select-Object -First 1
        $mostRecentDiff = $blobCollection | Where-Object {$_.bktype -eq 'DIFF' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $mostRecentFull.bkdate} | Sort-object {$_.bkdate} -Descending | Select-Object -First 1
        $mostRecentFullFile = "$($azureURL)$($mostRecentFull.Name)"
        $mostRecentDiffFile = "$($azureURL)$($mostRecentDiff.Name)"

        Restore-FullDiffFile -mostRecentFullFile $mostRecentFullFile -mostRecentDiffFile $mostRecentDiffFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName
    }

    $blobCollection = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename | Get-BlobReferences
    $StartDateTime = Get-BackupFinishDate -databasename $databasename -DestinationServer $DestinationServer
    $trnFiles = $blobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $StartDateTime } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name}
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
    $blobCollection = Get-AzStorageBlob -Context $context -Container $ContainerName | Get-BlobReferences
    $grouped = $blobCollection | Group-Object -Property server,database,bktype | Where-Object {$_.count -gt $keepMinimumCount } 
    $blobsToDelete = $grouped | ForEach-Object {$_.Group | Select-Object -First ($_.Count - $keepMinimumCount) | Where-Object {$_.bkdate -lt $deleteOlderThan } }
    $blobsToDelete | ForEach-Object{Remove-AzStorageBlob -Blob $_.Name -Container $ContainerName -Context $context -WhatIf}
}
