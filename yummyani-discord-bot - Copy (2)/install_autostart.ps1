# Adds Startup shortcut: pythonw.exe launch.pyw (no console window).
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Pyw = Join-Path $Root "launch.pyw"
if (-not (Test-Path $Pyw)) {
    Write-Error "launch.pyw not found: $Pyw"
}

$pythonw = $null
try {
    $pythonw = (Get-Command pythonw.exe -ErrorAction Stop).Source
} catch {
    $py = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($py) {
        $dir = Split-Path $py.Source
        $candidate = Join-Path $dir "pythonw.exe"
        if (Test-Path $candidate) { $pythonw = $candidate }
    }
}
if (-not $pythonw) {
    Write-Error "pythonw.exe not found. Install Python and enable Add to PATH."
}

$Startup = [Environment]::GetFolderPath("Startup")
if (-not $Startup) {
    $Startup = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
}
$LinkPath = Join-Path $Startup "YummyAnime Discord Bot.lnk"
$Shell = New-Object -ComObject WScript.Shell
$Sc = $Shell.CreateShortcut($LinkPath)
$Sc.TargetPath = $pythonw
$Sc.Arguments = "`"$Pyw`""
$Sc.WorkingDirectory = $Root
$Sc.WindowStyle = 7
$Sc.Description = "YummyAnime Discord Bot"
$Sc.Save()

Write-Host "Shortcut created: $LinkPath"
