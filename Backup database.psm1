[String]$DatabaseServer = '.'
[String]$DatabaseBackupPath = ''

function Backup-Database {
<#
.Synopsis
    Creates SQL Server database backup
.Description
    Creates SQL Server database backup, transaction log backup, checks database and backup for integrity. Generates filenames depending on current date and time.
.Example
    Backup-Database 'DB1','DB2' -Server 'DBServer' -Destination '\\FileServer\Backup' -CheckBackup -BackupTransactionLog

.Example
    $DatabaseServer = 'DBServer'
    $DatabaseBackupPath = '\\FileServer\Backup'
    Backup-Database 'DB1','DB2' -CheckBackup -BackupTransactionLog

.Example
    Backup-Database 'DB1',@{Database='DB2'; BackupTransactionLog=$true; CheckDatabase=$true} -Server 'DBServer' -Destination '\\FileServer\Backup'

.Parameter Database
    Database name
.Parameter Server
    Database server name
.Parameter Destination
    Backup path
.Parameter UseSubfolder
    If true, creates backup into <Database name> subfolder for each database
.Parameter CheckDatabase
    If true, invokes DBCC CHECKDB before backing up
.Parameter CheckBackup
    If true, checks backup integrity after backing up
.Parameter CopyOnly
    Set to create backup independent of the sequence of conventional SQL Server backups
.Parameter BackupTransactionLog
    Set to create transaction log backup
.Parameter BackupDatabase
    Set to create database backup
.Parameter Differential
    Set to create differential database backup (otherwise full backup)
    This flag can be used with non-copyonly database backups
.Parameter Compression
    Whether to use compression
.Parameter RetainDays
    Sets 'Retain days' property to backup
.Parameter ParentProgressId
    If specified, all progress uses this parent progress id to write progress
#>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [PSObject[]]$Database,
        [String]$Server = $DatabaseServer,
        [Alias('Path')]
        [String]$Destination = $DatabaseBackupPath,
        [Switch]$UseSubfolder,
        [Switch]$CheckDatabase,
        [Switch]$CheckBackup,
        [Switch]$CopyOnly,
        [Switch]$BackupTransactionLog,
        [Switch]$BackupDatabase = $true,
        [Switch]$Differential,
        [Switch]$Compression = $true,
        [int]$RetainDays = 0,
        [int]$ParentProgressId
    )
    
    begin {
        Push-Location
        Import-Module SQLPS -Verbose:$false -WarningAction SilentlyContinue
        $SmoServerType = (Get-Command Backup-SqlDatabase).Parameters.InputObject.ParameterType.GetElementType()
        $ParameterNames = @{DatabaseName='Database'},'Server',@{Destination='Path'},'Destination','CheckDatabase','CheckBackup','BackupDatabase','BackupTransactionLog','Differential','Compression','RetainDays'
        $ProgressId = Get-Random
    }
    process {
        try {
            $NumberProcessed = 0
            foreach ($DB in $Database) {
                # Determining parameters passed through Hashtables in Database array
                [String]$DatabaseName = ''
                if ($DB -is [String]) {
                    $DatabaseName = $DB
                }
                elseif ($DB -is [Hashtable]) {
                    foreach ($ParameterName in $ParameterNames) {
                        if ($ParameterName -is [HashTable]) {
                            $VarName = $ParameterName.Keys | select -First 1
                            $ParName = $ParameterName[$VarName]
                        }
                        else {
                            $ParName = $ParameterName
                            $VarName = $ParameterName
                        }
                        if ($DB.Contains($ParName)) {
                            Set-Variable $VarName $DB[$ParName]
                        }
                    }
                }
                else {
                    Write-Error "Database contains $($DB.GetType())" -Category InvalidArgument -RecommendedAction "Provide String or Hashtable in Database"
                    $NumberProcessed++
                    continue
                }
                
                if ($Compression) { $CompressionOption = 'On' } else { $CompressionOption = 'Off' }
                $DatabaseTitle = [Char]::ToUpperInvariant($DatabaseName[0]) + $DatabaseName.Substring(1)

                $Progress = @{}
                if ($Database.Count -eq 1) {
                    $Progress.Activity = "Backing up $DatabaseTitle database"
                }
                else {
                    $Progress.Activity = "Backing up $($Database.Count) databases"
                    $Progress.Status = "Backing up $DatabaseTitle database"
                    #$Progress.PercentComplete = $NumberProcessed * 100 / $Database.Count
                }
                if ($PSBoundParameters.ContainsKey('ParentProgressId')) {
                    $Progress.ParentId = $ParentProgressId
                }
                $Progress.Id = $ProgressId

                $Steps = 0
                if ($BackupDatabase) { $Steps++ }
                if ($BackupTransactionLog) { $Steps++ }
                if ($CheckBackup) { $Steps *= 2 }
                if ($CheckDatabase) { $Steps++ }
                $StepN = 0
                
                #Connecting to database server
                $ServerInstance = New-Object $SmoServerType $Server
                $ServerInstance.ConnectionContext.StatementTimeout = 65535

                #Checking database integrity
                if ($CheckDatabase) {
                    Write-Progress @Progress -CurrentOperation 'Checking database integrity' -PercentComplete (($NumberProcessed + ($StepN++) / $Steps) * 100 / $Database.Count)
                    Write-Verbose "Checking $DatabaseTitle database integrity"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $DatabaseName -Query 'DBCC CHECKDB($(dbname))' -Variable "dbname='$DatabaseName'" -QueryTimeout 65535
                }

                #Backing up transaction log
                if ($BackupTransactionLog) {
                    Write-Progress @Progress -PercentComplete (($NumberProcessed + ($StepN++) / $Steps) * 100 / $Database.Count) #Child Write-Progress will show current operation
                    $FileName = $Destination
                    if ($UseSubfolder) {
                        $FileName = Join-Path $FileName $DatabaseTitle
                        if (-not (Test-Path $FileName)) {
                            New-Item $FileName -ItemType Container -Force | Out-Null
                        }
                    }
                    $FileName = Join-Path $FileName "$($DatabaseTitle)_$(Get-Date -format 'yyyy-MM-dd_HH-mm')_log.trn"

                    Write-Verbose "Backing up $DatabaseTitle transaction log to $FileName"

                    #Creating backup in job to control Write-Progress
                    $JobPercentComplete = 0
                    Write-Progress -Activity "Creating transaction log backup" -ParentId $Progress.Id -PercentComplete $JobPercentComplete -Status "$JobPercentComplete% complete"
                    $Job = Start-Job -InitializationScript { Import-Module SQLPS -Verbose:$false -WarningAction SilentlyContinue } -ScriptBlock {
                        param($Server,$Parameters)
                        $SmoServerType = (Get-Command Backup-SqlDatabase).Parameters.InputObject.ParameterType.GetElementType()
                        $ServerInstance = New-Object $SmoServerType $Server
                        $ServerInstance.ConnectionContext.StatementTimeout = 65535
                        Backup-SqlDatabase @Parameters -InputObject $ServerInstance -BackupAction Log -Checksum
                    } -ArgumentList $Server,@{Database=$DatabaseName; BackupFile=$FileName; CompressionOption=$CompressionOption; CopyOnly=$CopyOnly; RetainDays=$RetainDays}
                    do {
                        Start-Sleep 2
                        $JobProgress = $Job.ChildJobs[0].Progress[-1]
                        if ($JobProgress -ne $null -and $JobProgress.PercentComplete -ne $JobPercentComplete) {
                            $JobPercentComplete = $JobProgress.PercentComplete
                            Write-Progress @Progress -PercentComplete (($NumberProcessed + ($StepN - 1 + $JobPercentComplete / 100) / $Steps) * 100 / $Database.Count)
                            Write-Progress -Activity "Creating transaction log backup" -PercentComplete $JobPercentComplete -ParentId $Progress.Id -Status "$JobPercentComplete% complete"
                        }
                    } while ($Job.State -eq 'Running')
                    Remove-Job $Job
                    Write-Progress -Activity "Creating transaction log backup" -Completed -ParentId $Progress.Id

                    if ($CheckBackup) {
                        Write-Progress @Progress -CurrentOperation 'Checking transaction log backup integrity' -PercentComplete (($NumberProcessed + ($StepN++) / $Steps) * 100 / $Database.Count)
                        #$FileNumber = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select position from msdb..backupset where database_name=$(dbname) and backup_set_id=(select max(backup_set_id) from msdb..backupset where database_name=$(dbname))' -Variable "dbname='$DatabaseName'" | select -ExpandProperty position
                        $BackupInfo = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select top 1 backup_set_id,position,backup_size,compressed_backup_size from msdb..backupset where database_name=$(dbname) order by backup_set_id desc' -Variable "dbname='$DatabaseName'"
                        $FileNumber = $BackupInfo.position
                        $Position = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE HEADERONLY FROM DISK=$(filename)' -Variable "filename='$FileName'" | select -Last 1 -ExpandProperty Position
                        if ($FileNumber -ne $null -and $FileNumber -eq $Position) {
                            Write-Verbose "Verifying backup file '$FileName', position $FileNumber"
                            Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE VERIFYONLY FROM DISK=$(filename) WITH FILE=$(position)' -Variable "filename='$FileName'","position=$FileNumber" -QueryTimeout 65535
                        }
                        else {
                            Write-Error "Transaction log verify failed. Backup information for database $DatabaseName not found or incorrect (query position $FileNumber, file position $Position)" -Category InvalidResult
                        }
                    }
                }

                #Backing up database
                if ($BackupDatabase) {
                    Write-Progress @Progress -PercentComplete (($NumberProcessed + ($StepN++) / $Steps) * 100 / $Database.Count) # Child Write-Progress will show current operation
                    $FileName = $Destination
                    if ($UseSubfolder) {
                        $FileName = Join-Path $FileName $DatabaseTitle
                        if (-not (Test-Path $FileName)) {
                            New-Item $FileName -ItemType Container -Force | Out-Null
                        }
                    }
                    if ($Differential) {
                        $part = 'diff'
                    }
                    else {
                        $part = 'full'
                    }
                    $FileName = Join-Path $FileName "$($DatabaseTitle)_$(Get-Date -format 'yyyy-MM-dd_HH-mm')_$part.bak"
                    
                    Write-Verbose "Backing up $DatabaseTitle database to $FileName"
                    
                    #Creating backup in job to control Write-Progress
                    #Standard cmdlet writes progress "Backing up (Database: 'accounting' ; Server: 'Serv1C' ; Action = 'Database') ."
                    # and DOESN'T write Write-Progress -Complete
                    $JobPercentComplete = 0
                    Write-Progress -Activity "Creating database backup" -ParentId $Progress.Id -PercentComplete $JobPercentComplete -Status "$JobPercentComplete% complete"
                    $Job = Start-Job -InitializationScript { Import-Module SQLPS -Verbose:$false -WarningAction SilentlyContinue } -ScriptBlock {
                        param($Server,$Parameters)
                        $SmoServerType = (Get-Command Backup-SqlDatabase).Parameters.InputObject.ParameterType.GetElementType()
                        $ServerInstance = New-Object $SmoServerType $Server
                        $ServerInstance.ConnectionContext.StatementTimeout = 65535
                        Backup-SqlDatabase @Parameters -InputObject $ServerInstance -BackupAction Database -Checksum
                    } -ArgumentList $Server,@{Database=$DatabaseName; BackupFile=$FileName; CompressionOption=$CompressionOption; Incremental=$Differential; CopyOnly=$CopyOnly; RetainDays=$RetainDays}
                    do {
                        Start-Sleep 2
                        $JobProgress = $Job.ChildJobs[0].Progress[-1]
                        if ($JobProgress -ne $null -and $JobProgress.PercentComplete -ne $JobPercentComplete) {
                            $JobPercentComplete = $JobProgress.PercentComplete
                            Write-Progress @Progress -PercentComplete (($NumberProcessed + ($StepN - 1 + $JobPercentComplete / 100) / $Steps) * 100 / $Database.Count)
                            Write-Progress -Activity "Creating database backup" -PercentComplete $JobPercentComplete -ParentId $Progress.Id -Status "$JobPercentComplete% complete"
                        }
                    } while ($Job.State -eq 'Running')
                    Remove-Job $Job
                    Write-Progress -Activity "Creating database backup" -Completed -ParentId $Progress.Id
                    
                    if ($CheckBackup) {
                        Write-Progress @Progress -CurrentOperation 'Checking database backup integrity' -PercentComplete (($NumberProcessed + ($StepN++) / $Steps) * 100 / $Database.Count)
                        #$FileNumber = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select position from msdb..backupset where database_name=$(dbname) and backup_set_id=(select max(backup_set_id) from msdb..backupset where database_name=$(dbname))' -Variable "dbname='$DB'" -Verbose:$Verbose | select -ExpandProperty position
                        $BackupInfo = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select top 1 backup_set_id,position,backup_size,compressed_backup_size from msdb..backupset where database_name=$(dbname) order by backup_set_id desc' -Variable "dbname='$DatabaseName'"
                        $FileNumber = $BackupInfo.position
                        $Position = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE HEADERONLY FROM DISK=$(filename)' -Variable "filename='$FileName'" | select -Last 1 -ExpandProperty Position
                        if ($FileNumber -ne $null -and $FileNumber -eq $Position) {
                            Write-Verbose "Verifying backup file '$FileName', position $FileNumber"
                            Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE VERIFYONLY FROM DISK=$(filename) WITH FILE=$(position)' -Variable "filename='$FileName'","position=$FileNumber" -QueryTimeout 65535
                        }
                        else {
                            Write-Error "Database backup verify failed. Backup information for database $DatabaseName not found or incorrect (query position $FileNumber, file position $Position)" -Category InvalidResult
                        }
                    }
                }
                $NumberProcessed++
            }
            Write-Progress @Progress -Completed
        }
        finally {
            Pop-Location
        }
    }
}

Export-ModuleMember -Variable 'DatabaseServer','DatabaseBackupPath'
Export-ModuleMember -Function Backup-Database