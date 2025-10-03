$exclude = @("venv", "botPython.zip", "#material", "json", "downloads", "Logs", "Registros", ".json", ".xlsx")
$files = Get-ChildItem -Path . -Exclude $exclude
Compress-Archive -Path $files -DestinationPath "botPython.zip" -Force