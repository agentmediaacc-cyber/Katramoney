$date = Get-Date -Format "yyyy-MM-dd"
$backupName = "katramoney_backup_$date.db"
if (Test-Path "katramoney.db") {
    Copy-Item "katramoney.db" -Destination "$backupName"
    Write-Host "✅ Backup Created: $backupName" -ForegroundColor Cyan
} else {
    Write-Host "❌ Error: katramoney.db not found!" -ForegroundColor Red
}
