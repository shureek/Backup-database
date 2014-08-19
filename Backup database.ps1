$LogCommandHealthEvent = $true
$LogCommandLifecycleEvent = $true
$ProgressPreference = 'Continue'

function Backup-Database {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        $Server,
        [Parameter(Mandatory=$true, ValueFromPipeline=$true, ValueFromPipelineByPropertyName=$true)]
        [String]$Database,
        [Parameter(Mandatory=$true)]
        [String]$Path,
        [Switch]$CreateSubfolder,
        [Switch]$Check,
        [Switch]$CopyOnly,
        [Switch]$BackupDatabase = $true,
        [Parameter(ValueFromPipelineByPropertyName=$true)]
        [Switch]$BackupTransactionLog,
        [Switch]$NoInnerVerbose,
        [Parameter(ValueFromPipelineByPropertyName=$true)]
        [Switch]$Differential,
        [Switch]$ShowProgress = $true
    )

    begin {
        if (-not $Path.EndsWith('\')) {
            $Path = $Path + '\'
        }
        if ($NoInnerVerbose) {
            $Verbose = $false
        }
        else {
            $Verbose = ($VerbosePreference -eq 'Continue')
        }

        Import-Module SQLPS -Verbose:$false -WarningAction SilentlyContinue
        $Activity = "Backing up databases"
        
        # В некоторых командах нельзя указать таймаут, поэтому будем использовать этот сервер
        Write-Verbose "Connecting to database server $Server"
        if ($ShowProgress) {
            Write-Progress -Activity $Activity -CurrentOperation "Connecting to database server $Server"
        }
        $ServerInstance = New-Object Microsoft.SqlServer.Management.Smo.Server $Server
        $ServerInstance.ConnectionContext.StatementTimeout = 0
    }
    process {
        Push-Location
        try
        {
            $DefaultProgressPreference = $ProgressPreference
            $RetainDays = 0
            if ($Differential) {
                $RetainDays = 31
            }
            
            $DB = $Database
            $DBTitle = [Char]::ToUpperInvariant($DB[0]) + $DB.Substring(1)
            $Operation = "Backing up $DBTitle database"
            
            if ($Check) {
                if ($ShowProgress) {
                    Write-Progress -Activity $Activity -CurrentOperation $Operation -Status 'Checking database integrity' #-PercentComplete $TotalProgress
                }
                Write-Verbose "Checking $DB database integrity"
                Invoke-Sqlcmd -ServerInstance $ServerInstance -Database $DB -Query 'DBCC CHECKDB($(dbname))' -Variable "dbname='$DB'" -Verbose:$Verbose -QueryTimeout 65535
            }

            if ($BackupTransactionLog) {
                if ($ShowProgress) {
                    Write-Progress -Activity $Activity -CurrentOperation $Operation -Status 'Backing up transaction log' #-PercentComplete $TotalProgress
                }
                $FileName = $Path
                if ($CreateSubfolder) {
                    $FileName = Join-Path $FileName $DBTitle
                    if (-not (Test-Path $FileName -PathType Container)) {
                        New-Item $FileName -ItemType Container -Force | Out-Null
                    }
                }
                $FileName = Join-Path $FileName "$($DBTitle)_$(Get-Date -format 'yyyy-MM-dd_HH-mm')_log.trn"

                Write-Verbose "Backing up transaction log to $FileName"
                #Backup-SqlDatabase отображает прогресс, но не закрывает его при завершении, поэтому не будем его вообще показывать
                $ProgressPreference = 'SilentlyContinue'
                Backup-SqlDatabase -Database $DB -BackupFile $FileName -InputObject $ServerInstance -BackupAction Log -Checksum:$Check -CompressionOption On -CopyOnly:$CopyOnly -RetainDays $RetainDays -LogTruncationType Truncate -Verbose:$Verbose
                $ProgressPreference = $DefaultProgressPreference
                Get-Item $FileName | %{ Write-Verbose "File size: $($_.Length) bytes" }

                if ($Check) {
                    #Write-Progress -Activity $Activity -CurrentOperation $Operation -Status 'Checking transaction log backup integrity' #-PercentComplete $TotalProgress
                    $FileNumber = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select position from msdb..backupset where database_name=$(dbname) and backup_set_id=(select max(backup_set_id) from msdb..backupset where database_name=$(dbname))' -Variable "dbname='$DB'" -Verbose:$Verbose | select -ExpandProperty position
                    $Position = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE HEADERONLY FROM DISK=$(filename)' -Variable "filename='$FileName'" -Verbose:$Verbose | select -Last 1 -ExpandProperty Position
                    if ($FileNumber -ne $null -and $FileNumber -eq $Position) {
                        Write-Verbose "Verifying backup file '$FileName', position $FileNumber"
                        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE VERIFYONLY FROM DISK=$(filename) WITH FILE=$(position)' -Variable "filename='$FileName'","position=$FileNumber" -QueryTimeout 65535 -Verbose:$Verbose
                    }
                    else {
                        Write-Error "Transaction log verify failed. Backup information not found or incorrect (query position $FileNumber, file position $Position)" -TargetObject "$Server\$DB" -Category InvalidResult
                    }
                }
            }

            if ($BackupDatabase) {
                #Write-Progress -Activity $Activity -CurrentOperation $Operation -Status 'Backing up database' #-PercentComplete $TotalProgress
                $FileName = $Path
                if ($CreateSubfolder) {
                    $FileName = Join-Path $FileName $DBTitle
                    if (-not (Test-Path $FileName -PathType Container)) {
                        New-Item $FileName -ItemType Container -Force | Out-Null
                    }
                }
                if ($Differential) {
                    $part = 'diff'
                }
                else {
                    $part = 'full'
                }
                $FileName = Join-Path $FileName "$($DBTitle)_$(Get-Date -format 'yyyy-MM-dd_HH-mm')_$part.bak"
            
                Write-Verbose "Backing up $DB database to $FileName"
                $ProgressPreference = 'SilentlyContinue'
                Backup-SqlDatabase -Database $DB -BackupFile $FileName -InputObject $ServerInstance -BackupAction Database -Checksum:$Check -CompressionOption On -Incremental:$Differential -CopyOnly:$CopyOnly -RetainDays $RetainDays -Verbose:$Verbose
                $ProgressPreference = $DefaultProgressPreference
                Get-Item $FileName | %{ Write-Verbose "File size: $($_.Length) bytes" }
            
                if ($Check) {
                    #Write-Progress -Activity $Activity -CurrentOperation $Operation -Status 'Checking database backup integrity' #-PercentComplete $TotalProgress
                    $FileNumber = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'select position from msdb..backupset where database_name=$(dbname) and backup_set_id=(select max(backup_set_id) from msdb..backupset where database_name=$(dbname))' -Variable "dbname='$DB'" -Verbose:$Verbose | select -ExpandProperty position
                    $Position = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE HEADERONLY FROM DISK=$(filename)' -Variable "filename='$FileName'" -Verbose:$Verbose | select -Last 1 -ExpandProperty Position
                    if ($FileNumber -ne $null -and $FileNumber -eq $Position) {
                        Write-Verbose "Verifying backup file '$FileName', position $FileNumber"
                        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query 'RESTORE VERIFYONLY FROM DISK=$(filename) WITH FILE=$(position)' -Variable "filename='$FileName'","position=$FileNumber" -QueryTimeout 65535 -Verbose:$Verbose
                    }
                    else {
                        Write-Error "Database backup verify failed. Backup information not found or incorrect (query position $FileNumber, file position $Position)" -TargetObject "$Server\$DB" -Category InvalidResult
                    }
                }
            }
        }
        finally {
            #Write-Progress -Activity $Activity -Completed
            Pop-Location
        }
    }
}

$Server = 'Serv1C'
$DatabaseInfos = @(
    @{Database='accounting3';       Differential=$false; BackupTransactionLog=$true},
    @{Database='pult';              Differential=$false; BackupTransactionLog=$true},
    @{Database='zup';               Differential=$false; BackupTransactionLog=$true},
    @{Database='accounting';        Differential=$true; BackupTransactionLog=$true},
    @{Database='accounting_ip';     Differential=$true; BackupTransactionLog=$true},
    @{Database='accounting_u';      Differential=$true; BackupTransactionLog=$true}
)
$Databases = $DatabaseInfos | %{ New-Object PSObject -Property $_ } | Out-GridView -PassThru
if ($Databases -ne $null) {
    $Check = $false
    $BackupPath = '\\MAIN\Backup\SQL'
    $Databases | Backup-Database -Server $Server -Path $BackupPath -CreateSubfolder -Verbose -NoInnerVerbose -Check:$Check
    #$Backup = Backup-SqlDatabase -ServerInstance $Server -Database $Databases[0] -BackupFile '\\MAIN\Backup\SQL\Service\service2.bak' -BackupAction Database -BackupSetName 'Service' -CompressionOption On -PassThru
}