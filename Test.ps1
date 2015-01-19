Set-Location $PSScriptRoot
Import-Module '.\Backup database.psm1'

#$DatabaseServer = 'Serv1C'
#$DatabaseBackupPath = '\\VLG-HV-S1\Backup\SQL'
#$DBList = 'ZUP','ZUP_copy'
Backup-Database 'ZUP','ZUP_copy' -CopyOnly -CheckDatabase -CheckBackup -Server 'Serv1C' -Destination '\\VLG-HV-S1\Backup\SQL'