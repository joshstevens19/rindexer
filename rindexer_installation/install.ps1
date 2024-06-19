# Define base directories and default download URL
$BaseDir = $env:XDG_CONFIG_HOME -ne $null ? $env:XDG_CONFIG_HOME : $env:HOME
$RindexerDir = $env:RINDEXER_DIR -ne $null ? $env:RINDEXER_DIR : "$BaseDir\.rindexer"
$RindexerBinDir = "$RindexerDir\bin"
$RindexerUpPath = "$RindexerBinDir\rindexerup.ps1"
$RindexerDownPath = "$RindexerBinDir\rindexerdown.ps1"
$BinPath = "$RindexerBinDir\rindexer.exe"
$BinUrl = "https://rindexer.io/download/rindexer-latest"
$LocalBinPath = "C:\Users\joshstevens\code\rindexer\target\debug\rindexer_cli.exe"

# Ensure the bin directory exists
if (-Not (Test-Path -Path $RindexerBinDir)) {
    New-Item -ItemType Directory -Path $RindexerBinDir | Out-Null
}

# Install or uninstall based on the command line option
param (
    [string]$Action = ""
)

switch ($Action) {
    "--local" {
        Write-Output "Using local binary from $LocalBinPath..."
        Copy-Item -Path $LocalBinPath -Destination $BinPath
    }
    "--uninstall" {
        Write-Output "Uninstalling rindexer..."
        Remove-Item -Path $BinPath, $RindexerUpPath -Force
        Remove-Item -Path $RindexerBinDir -Recurse -Force
        $ProfilePath = $PROFILE
        (Get-Content $ProfilePath) -notmatch 'rindexerup|rindexer' | Set-Content $ProfilePath
        Write-Output "Uninstallation complete! Please restart your shell or source your profile to complete the process."
        exit
    }
    default {
        Write-Output "Downloading binary from $BinUrl..."
        Invoke-WebRequest -Uri $BinUrl -OutFile $BinPath
    }
}

# Make the binary executable
& icacls $BinPath /grant Everyone:(RX)

# Update PATH in user's profile
$ProfilePath = $PROFILE
$PathEntry = "export PATH=`"`$PATH;$RindexerBinDir`""
if (-Not (Get-Content $ProfilePath | Select-String -Pattern [regex]::Escape($RindexerBinDir))) {
    Add-Content -Path $ProfilePath -Value $PathEntry
    Write-Output "PATH updated in $ProfilePath. Please log out and back in or source the profile file."
}

# Add the rindexerup and rindexerdown commands to the profile
Add-Content -Path $ProfilePath -Value "`n# Adding rindexerup and rindexerdown commands"
Add-Content -Path $ProfilePath -Value "`nalias rindexerup=`"`powershell -File $RindexerUpPath`"`"
Add-Content -Path $ProfilePath -Value "`nalias rindexerdown=`"`powershell -File $RindexerDownPath`"`"

# Create or update the rindexerup script to check for updates
@"
#!/usr/bin/env pwsh
Set-StrictMode -Version Latest

Write-Output "Updating rindexer..."
param (
    [string]`$UpdateOption = ""
)

if (`$UpdateOption -eq "--local") {
    Write-Output "Using local binary for update..."
    Copy-Item -Path "$LocalBinPath" -Destination "$BinPath"
} else {
    Write-Output "Downloading the latest binary from $BinUrl..."
    Invoke-WebRequest -Uri "$BinUrl" -OutFile "$BinPath"
}
& icacls "$BinPath" /grant Everyone:(RX)
Write-Output "rindexer has been updated to the latest version."
"@ | Set-Content -Path $RindexerUpPath

# Create the rindexerdown script
@"
#!/usr/bin/env pwsh
Set-StrictMode -Version Latest

Write-Output "Uninstalling rindexer..."
Remove-Item -Path "$BinPath", "$RindexerUpPath" -Force
Remove-Item -Path "$RindexerBinDir" -Recurse -Force
`$ProfilePath = `$PROFILE
(Get-Content `$ProfilePath) -notmatch 'rindexerup|rindexer' | Set-Content `$ProfilePath
Write-Output "Uninstallation complete! Please restart your shell or source your profile to complete the process."
"@ | Set-Content -Path $RindexerDownPath

& icacls $RindexerUpPath /grant Everyone:(RX)
& icacls $RindexerDownPath /grant Everyone:(RX)

Write-Output "Installation complete! Please run 'source $ProfilePath' or start a new terminal session to use rindexer."
Write-Output "You can update rindexer anytime by typing 'rindexerup --local' or just 'rindexerup'."
Write-Output "To uninstall rindexer, type 'rindexerdown'."
