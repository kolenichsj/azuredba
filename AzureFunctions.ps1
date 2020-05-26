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
        [Microsoft.Azure.Commands.Common.Authentication.Abstractions.IStorageContext]$Context
    )
    return Get-AzStorageBlob -Context $context -Container $ContainerName -Blob "*/$databasename/*"
}

function Get-BlobReferences {
    param (
        [parameter(ValueFromPipeline)]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs
    )
    
    #$dateRegex = '(?<ServerName>[\x21-\x2e,\x30-\x7E]{1,254})\/(?<DatabaseName>[\x21-\x2e,\x30-\x7E]{1,254})\/(?<BackupType>LOG|FULL|DIFF|FULL_COPY_ONLY|LOG_COPY_ONLY)\/(?:\k<ServerName>_\k<DatabaseName>_\k<BackupType>)_(?<bkdate>[\d]{8}_[\d]{6})\.(?<FileExtension>bak|trn)'
    $dateRegex = '^(?<ServerName>[^\/]{1,254})\/(?<DatabaseName>[^\/]{1,254})\/(?<BackupType>LOG|DIFF|FULL|FULL_COPY_ONLY|LOG_COPY_ONLY)\/(?:\k<ServerName>_\k<DatabaseName>_\k<BackupType>)_(?<bkdate>[\d]{8}_[\d]{6})\.(?<FileExtension>trn|bak)$'
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

function Get-BlobReferences2 {
    param (
        [parameter(ValueFromPipeline)]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs
    )
    
    [BlobReference[]]$blobCollection = @()
    
    foreach ($blob in $blobs) {
        $myFilePath = $blob.Name.Split('/')
        
        $objBlob = [BlobReference]@{
            name      = $blob.Name
            server    = $myFilePath[0]
            database  = $myFilePath[1]
            bktype    = $myFilePath[2]
            bkdate    = [DateTime]::ParseExact($myFilePath[3].Substring($myFilePath[3].Length - 19, 15)  , 'yyyyMMdd_HHmmss', [System.Globalization.CultureInfo]::InvariantCulture)
            extension = $myFilePath[3].Substring($myFilePath[3].Length - 3)
        }
        
        $blobCollection += $objBlob
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
        [string]$StorageAccountName,
        [string]$TargetDatabaseName
    )
	
    $DateStamp = (Get-Date).ToString('yyyyMMdd')
    $DestinationServerDefaultPaths = Get-DbaDefaultPath -SqlInstance $DestinationServer
    
    try {
        Write-Verbose "reading header info from $mostRecentFullFile"
        $HeaderInfo = Read-DbaBackupHeader -SqlInstance $DestinationServer -Path $mostRecentFullFile -AzureCredential $StorageAccountName
        $FileMapping = @{ }
        $loopCount = 0

        if ([string]::IsNullOrEmpty($TargetDatabaseName)) {
            $DestinationDBName = $HeaderInfo.DatabaseName
        }
        else {
            $DestinationDBName = $TargetDatabaseName
        }

        Write-Verbose "using target DB $DestinationDBName"
            
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
        $restoreFiles = @( $mostRecentFullFile)

        if (-Not [string]::IsNullOrEmpty($mostRecentDiffFile)) {
            $restoreFiles += $mostRecentDiffFile
        }

        Write-Verbose "Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $DestinationDBName -Path $restoreFiles -FileMapping $FileMapping -AzureCredential $StorageAccountName -WithReplace -NoRecovery"

        Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $DestinationDBName -Path $restoreFiles -FileMapping $FileMapping -AzureCredential $StorageAccountName -WithReplace -NoRecovery
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
        [datetime]$stopAtDate,
        [string[]]$trnfiles,
        [bool]$PrintOnly = $false,
        [bool]$NoRecovery = $false
    )

    foreach ($file in $trnfiles) {
        $sqlRestore = "Restore DATABASE [$databasename] FROM URL = '$file'  WITH  CREDENTIAL ='$StorageAccountName', REPLACE, NoRecovery, BLOCKSIZE = 512 " # BLOCKSIZE = 4096
        if ($null -ne $stopAtDate) {
            $sqlRestore += ", STOPAT = '$stopAtDate'"
        }
        
        Write-Host $sqlRestore
        
        if (-not $PrintOnly) {
            Invoke-Sqlcmd -ServerInstance $DestinationServer -Database 'master' -Query $sqlRestore -Verbose -QueryTimeout 65535
        }

        #Restore-DbaDatabase -SqlInstance $DestinationServer -DatabaseName $databasename -Path $file -AzureCredential $StorageAccountName -WithReplace -BlockSize 512 -NoRecovery -Continue # -Verbose
    }

    if ($NoRecovery -ne $true -and -not $PrintOnly) {
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
    [OutputType([string])]
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
        [parameter(ParameterSetName = 'Blobs')][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'BlobCollection')][ValidateNotNull()]
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
    [CmdletBinding()]
    [OutputType([Tuple[string, string]])]
    param(
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string[]]$serverList, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [datetime]$PriorToDate,
        [parameter(ParameterSetName = 'Token')][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Blobs')][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'Context', Mandatory = $true)][ValidateNotNull()]
        [Microsoft.Azure.Commands.Common.Authentication.Abstractions.IStorageContext]$Context,
        [parameter(ParameterSetName = 'BlobCollection')][ValidateNotNull()]
        [BlobReference[]]$blobCollection,
        [switch]$AsURL
    )

    if ($null -eq $Context) {
        Write-Verbose 'Getting context'
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    }

    if ($null -eq $blobs) {
        $blobList = New-Object System.Collections.ArrayList
        foreach ($server in $serverList) {
            Write-Verbose "Getting blobs: $server/$databasename/*/*.bak" 
            $response = Get-AzStorageBlob -Context $strg.Context -Container $ContainerName -Blob "$server/$databasename/*/*.bak" 
            $response | ForEach-Object { $blobList.Add($_) }
        }

        $blobs = $blobList.ToArray()
    }

    if ($null -ne $blobs) {
        $blobCollection = Get-BlobReferences -blobs $blobs
    }

    if ($null -eq $PriorToDate) {
        $PriorToDate = Get-Date
    }   
    
    $mostRecentFull = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -le $PriorToDate } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1

    if ([string]::IsNullOrEmpty($mostRecentFull.Name)) {
        Write-Error "Could not find full backup for $database"
    }
    else {
        $mostRecentDiff = $blobCollection | Where-Object { $_.bktype -eq 'DIFF' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -le $PriorToDate -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-object { $_.bkdate } -Descending | Select-Object -First 1
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
        [datetime]$PriorToDate,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [switch]$AsURL
    )

    $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    $blobs = Get-BlobsForServer -ContainerName $ContainerName -Context $Context -servername $servername
    $blobCollection = Get-BlobReferences -blobs $blobs
    $grouped = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -or $_.bktype -eq 'DIFF' } | Group-Object -Property database | Sort-Object { $_.bkdate } -Descending
    if ($null -eq $PriorToDate) {
        $PriorToDate = Get-Date
    }

    $grpfd = $grouped | ForEach-Object {
        $mostRecentFull = $_.Group | Where-Object { $_.bktype -eq 'FULL' -and $_.bkdate -le $PriorToDate } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
        $mostRecentDiff = $_.Group | Where-Object { $_.bktype -eq 'DIFF' -and $_.bkdate -ge $mostRecentFull.bkdate -and $_.bkdate -lt $PriorToDate } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
        New-Object "tuple[string, string]" $mostRecentFull.Name, $mostRecentDiff.Name
    }

    if ($AsURL) {
        $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
        $retval = $grpfd | ForEach-Object {
            New-Object "tuple[string, string]" "$($azureURL)$($grpfd.Item1)", "$($azureURL)$($grpfd.Item2)"
        }
    }
    else {
        $retval = $grpfd
    }

    return $retval
}

function Get-TRNFiles {
    param
    (        
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$servername, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [datetime]$StartDateTime,
        [datetime]$PriorToDate, 
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


    if ($null -eq $PriorToDate) {
        $PriorToDate = Get-Date
    }

    $trnFiles = $blobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $StartDateTime -and $_.bkdate -lt $PriorToDate } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name }
    $trnFiles += $blobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $StartDateTime -and $_.bkdate -ge $PriorToDate } | Sort-object { $_.bkdate } | Select-Object -First 1 { $azureURL + $_.name }

    return $trnFiles
}

function Restore-BlobDatabase {
    [CmdletBinding()]
    param (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string[]]$serverList, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$databasename, 
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [datetime]$PriorToDate = [System.DateTime]::Now,
        [parameter(ParameterSetName = 'Token')][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Blobs')][ValidateNotNull()]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'Context', Mandatory = $true)][ValidateNotNull()]
        [Microsoft.Azure.Commands.Common.Authentication.Abstractions.IStorageContext]$Context,
        [parameter(ParameterSetName = 'BlobCollection')][ValidateNotNull()]
        [BlobReference[]]$blobCollection,
        [Parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$DestinationServer,
        [string]$TargetDatabaseName = $databasename,
        [switch]$NoRecovery
    )

    $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"

    if ($null -eq $Context) {
        Write-Verbose 'Getting context'
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    }

    if ($null -eq $blobs) {
        $blobList = New-Object System.Collections.ArrayList
        foreach ($server in $serverList) {
            Write-Verbose "Getting blobs: $server/$databasename/*/*.bak" 
            $response = Get-AzStorageBlob -Context $strg.Context -Container $ContainerName -Blob "$server/$databasename/*/*.bak" 
            $response | ForEach-Object { $blobList.Add($_) }
        }

        $blobs = $blobList.ToArray()
    }

    if ($null -ne $blobs) {
        $blobCollection = Get-BlobReferences -blobs $blobs
    }

    $fullDiffFileParams = @{
        serverList         = $serverList
        databasename       = $databasename
        ContainerName      = $ContainerName
        StorageAccountName = $StorageAccountName
        PriorToDate        = $PriorToDate
        blobCollection     = $blobCollection
        AsURL              = $true
    }

    foreach ($key in $fullDiffFileParams.Keys) {
        Write-Verbose "$key - > $($fullDiffFileParams[$key])"
    }

    [Tuple[string, string]]$fullDiffFile = Get-MostRecentFullDiffFile @fullDiffFileParams
    
    $restoreParams = @{
        StorageAccountName = $StorageAccountName
        DestinationServer  = $DestinationServer
        mostRecentFullFile = $fullDiffFile.Item1
        mostRecentDiffFile = $fullDiffFile.Item2
        TargetDatabaseName = $TargetDatabaseName
    }

    foreach ($key in $restoreParams.Keys) {
        Write-Verbose "$key - > $($restoreParams[$key])"
    }

    Restore-FullDiffFile @restoreParams

    $StartDateTime = Get-BackupFinishDate -databasename $TargetDatabaseName -DestinationServer $DestinationServer
    $trnFiles = $blobCollection | Where-Object { $_.bktype -eq 'LOG' `
            -and $_.database -eq $databasename `
            -and $_.server -eq $servername `
            -and $_.bkdate -gt $StartDateTime `
            -and $_.bkdate -le $PriorToDate } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name }
    
    $TRNParams = @{
        databasename       = $TargetDatabaseName
        DestinationServer  = $DestinationServer
        StorageAccountName = $StorageAccountName
        stopAtDate         = $PriorToDate
        trnFiles           = $trnFiles
        NoRecovery         = $NoRecovery
    }

    Restore-TRNLogs @TRNParams
}

# function Restore-LatestDatabase {
#     param
#     (
#         [parameter(Mandatory = $true)][ValidateNotNull()]
#         [string[]]$serverList, 
#         [parameter(Mandatory = $true)][ValidateNotNull()]
#         [string]$databasename, 
#         [parameter(Mandatory = $true)]
#         [string]$DestinationServer,
#         [parameter(Mandatory = $true)][ValidateNotNull()]
#         [string]$StorageAccountName,
#         [parameter(Mandatory = $true)][ValidateNotNull()]
#         [string]$ContainerName,
#         [parameter(Mandatory = $true)][ValidateNotNull()]
#         [string]$SasToken,
#         [string]$TargetDatabaseName = "",
#         [datetime]$PriorToDate = $null,
#         [switch]$UseCopyOnly,
#         [switch]$NoRecovery
#     )

#     $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"
#     $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
#     $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename
#     $blobCollection = Get-BlobReferences -blobs $blobs
#     Write-Verbose "blob count: $($blobCollection.Count())`r`n `$databasename: $databasename`r`n`$serverList: $serverList"
    
#     if ($UseCopyOnly) {
#         $mostRecentCopy = $blobCollection | Where-Object { $_.bktype -eq 'FULL_COPY_ONLY' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
#         if ([string]::IsNullOrEmpty($mostRecentCopy)) {
#             Write-Error "Unable to find file for `$databasename: $databasename`r`n`$serverList: $serverList"
#         }

#         $mostRecentCopyFile = "$($azureURL)$($mostRecentCopy.Name)"
#         Write-Verbose "$($mostRecentCopyFile): $mostRecentCopyFile"
#         Restore-FullDiffFile -mostRecentFullFile $mostRecentCopyFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName -TargetDatabaseName $TargetDatabaseName
#     }
#     else {

#         $response = Get-MostRecentFullDiffFile #-serverList $serverList -databasename $databasename -ContainerName $ContainerName -StorageAccountName $StorageAccountName 
#         # PriorToDate
#         # AsURL

#         $mostRecentFull = $blobCollection | Where-Object { $_.bktype -eq 'FULL' -and $_.database -eq $databasename -and $serverList.Contains($_.server) } | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1
#         $mostRecentDiff = $blobCollection | Where-Object { $_.bktype -eq 'DIFF' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $mostRecentFull.bkdate } | Sort-object { $_.bkdate } -Descending | Select-Object -First 1
#         $mostRecentFullFile = "$($azureURL)$($mostRecentFull.Name)"
#         $mostRecentDiffFile = "$($azureURL)$($mostRecentDiff.Name)"

#         Write-Verbose "$($mostRecentFullFile): $mostRecentFullFile`r`n$($mostRecentDiffFile): $mostRecentDiffFile"
#         Restore-FullDiffFile -mostRecentFullFile $mostRecentFullFile -mostRecentDiffFile $mostRecentDiffFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName -TargetDatabaseName $TargetDatabaseName
#     }

#     $blobs = Get-BlobsForDatabase -ContainerName $ContainerName -Context $Context -databasename $databasename
#     $blobCollection = Get-BlobReferences -blobs $blobs
#     $StartDateTime = Get-BackupFinishDate -databasename $databasename -DestinationServer $DestinationServer
#     $trnFiles = $blobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -gt $StartDateTime } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name }
#     Restore-TRNLogs -databasename $databasename -DestinationServer $DestinationServer -trnfiles $trnfiles -StorageAccountName $StorageAccountName -NoRecovery:$NoRecovery
# }


function Restore-AGDatabase {
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
        [string]$TargetDatabaseName = '',
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$AvailabilityGroupName = '',        
        [parameter(ParameterSetName = 'Token')][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Blobs')]
        [Microsoft.WindowsAzure.Commands.Common.Storage.ResourceModel.AzureStorageBlob[]]$blobs,
        [parameter(ParameterSetName = 'Context', Mandatory = $true)][ValidateNotNull()]
        [Microsoft.Azure.Commands.Common.Authentication.Abstractions.IStorageContext]$Context,
        [parameter(ParameterSetName = 'BlobCollection')]
        [BlobReference[]]$blobCollection,
        [parameter(ParameterSetName = 'Token')]
        [parameter(ParameterSetName = 'Blobs')]
        [parameter(ParameterSetName = 'BlobCollection')]
        [parameter(ParameterSetName = 'Context')]
        [switch]$UseCopyOnly,
        [parameter(ParameterSetName = 'Token')]
        [parameter(ParameterSetName = 'TrnOnly')]
        [parameter(ParameterSetName = 'BlobCollection')]
        [parameter(ParameterSetName = 'Context')]
        [switch]$UseTrnOnly
    )

    $azureURL = "https://$StorageAccountName.blob.core.windows.net/$ContainerName/"

    if (-not $UseTrnOnly) {
        if ($null -eq $Context) {
            $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
        }

        if ($null -eq $blobs) {
            $blobList = New-Object System.Collections.ArrayList
            foreach ($server in $serverList) {
                $response = Get-AzStorageBlob -Context $strg.Context -Container $ContainerName -Blob "$server/$databasename/*/*.bak" 
                $response | ForEach-Object { $blobList.Add($_) }
            }

            $blobs = $blobList.ToArray()
        }

        if ($null -ne $blobs) {
            $blobCollection = Get-BlobReferences -blobs $blobs
        }

        Write-Verbose "blob count: $($blobCollection.Count)`r`n `$databasename: $databasename`r`n`$serverList: $serverList"
        
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
            if ([string]::IsNullOrEmpty($mostRecentDiff)) {
                $mostRecentDiffFile = $null
            }
            else {
                $mostRecentDiffFile = "$($azureURL)$($mostRecentDiff.Name)"
            }

            Write-Verbose "`$mostRecentFullFile: $mostRecentFullFile`r`n`$mostRecentDiffFile: $mostRecentDiffFile"
            Restore-FullDiffFile -mostRecentFullFile $mostRecentFullFile -mostRecentDiffFile $mostRecentDiffFile -DestinationServer $DestinationServer -StorageAccountName $StorageAccountName
        }
    }

    if ($null -eq $context) {
        $trnBlobCollection = $blobCollection
    }
    else {
        $trnblobList = New-Object System.Collections.ArrayList
        foreach ($server in $serverList) {
            $response = Get-AzStorageBlob -Context $strg.Context -Container $ContainerName -Blob "$server/$databasename/LOG/*.trn" 
            $response | ForEach-Object { $trnblobList.Add($_) }
        }

        $trnblobs = $trnblobList.ToArray()
        $trnBlobCollection = Get-BlobReferences -blobs $trnBlobs
    }

    $StartDateTime = Get-BackupFinishDate -databasename $databasename -DestinationServer $DestinationServer
    $trnFiles = $trnBlobCollection | Where-Object { $_.bktype -eq 'LOG' -and $_.database -eq $databasename -and $serverList.Contains($_.server) -and $_.bkdate -ge $StartDateTime } | Sort-object { $_.bkdate } | ForEach-Object { $azureURL + $_.name }
    Restore-TRNLogs -databasename $databasename -DestinationServer $DestinationServer -trnfiles $trnfiles -StorageAccountName $StorageAccountName -NoRecovery $true
    Invoke-Sqlcmd -ServerInstance $DestinationServer -Query "ALTER DATABASE [$databasename] SET HADR AVAILABILITY GROUP = [$AvailabilityGroupName]"
}

function Remove-OldBlobs {
    param
    (
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$ContainerName,
        [parameter(Mandatory = $true)][ValidateNotNull()]
        [string]$StorageAccountName,
        [parameter(ParameterSetName = 'Token', Mandatory = $true)][ValidateNotNull()]
        [string]$SasToken,
        [parameter(ParameterSetName = 'Context', Mandatory = $true)][ValidateNotNull()]
        [Microsoft.Azure.Commands.Common.Authentication.Abstractions.IStorageContext]$Context,
        [int]$keepMinimumDays = 35, # only delete older than this number of days
        [switch]$WhatIf
    )

    $maxRestorePoint = (Get-Date).AddDays( - $keepMinimumDays)
    
    if ($null -eq $Context ) {
        $Context = New-AzStorageContext -StorageAccountName $StorageAccountName -SasToken $SasToken
    }
    $blobs = Get-AzStorageBlob -Context $Context -Container $ContainerName
    $oldBlobs = $blobs | Where-Object { $_.LastModified -lt $maxRestorePoint } # we want to keep everything before the max restore point, so don't even process it
    $blobCollection = Get-BlobReferences -blobs $oldBlobs
    $serverGroups = $blobCollection | Group-Object -Property server
    $blobsToKeepSet = New-Object 'System.Collections.Generic.HashSet[string]'

    foreach ($server in $serverGroups) {
        $databaseGroups = $server.Group | Group-Object -Property database
        Write-Debug "Processing $($server.Name)"

        foreach ($database in $databaseGroups) {
            #need most recent full
            #need most recent diff after most recent full (if one exists)
            #need all logs between maxRestorePoint and Last Diff or full (whichever is later)
            Write-Debug "Processing $($database.Name)"
            $groupedFiles = $database.Group | Group-Object -Property bktype -AsHashTable
            $mostRecentFull = $groupedFiles['FULL'] | Sort-Object { $_.bkdate } -Descending | Select-Object -First 1

            if ([string]::IsNullOrEmpty($mostRecentFull.Name)) {
                Write-Warning "Could not find full backup for $($database.Name) on  $($server.Name)"
            }
            else {
                $mostRecentDiff = $groupedFiles['DIFF'] | Where-Object { $_.bkdate -gt $mostRecentFull.bkdate } | Sort-object { $_.bkdate } -Descending | Select-Object -First 1
            }

            if ([string]::IsNullOrEmpty($mostRecentDiff.Name)) {
                $groupedFiles['LOG'] | Where-Object { $_.bkdate -ge $mostRecentFull.bkdate } | ForEach-Object { $blobsToKeepSet.Add($_.Name) | out-null }
            }
            else {
                $groupedFiles['LOG'] | Where-Object { $_.bkdate -ge $mostRecentDiff.bkdate } | ForEach-Object { $blobsToKeepSet.Add($_.Name) | out-null }
            }
            
            if (-not ($blobsToKeepSet.Count -eq 0 -or [string]::IsNullOrEmpty($mostRecentDiff.Name))) {
                $blobsToKeepSet.Add($mostRecentDiff.Name) | out-null
            }

            if ($blobsToKeepSet.Count -gt 0) {
                $blobsToKeepSet.Add($mostRecentFull.Name) | out-null
            }
        }
    }
    
    foreach ($blob in $oldBlobs) {
        if (-not $blobsToKeepSet.Contains($blob.Name)) {
            Remove-AzStorageBlob -Blob $blob.Name -Container $ContainerName -Context $context -WhatIf:$WhatIf
        }
    }
}