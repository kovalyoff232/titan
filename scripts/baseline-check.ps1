param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot\..")

$cargo = (Get-Command cargo -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty Source)
if (-not $cargo) {
    $fallback = Join-Path $HOME ".cargo\bin\cargo.exe"
    if (Test-Path $fallback) {
        $cargo = $fallback
    } else {
        throw "cargo not found in PATH or $fallback"
    }
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Command,
        [int]$TimeoutSec = 600
    )

    Write-Host "==> $Name"
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Command))
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "powershell.exe"
    $psi.Arguments = "-NoProfile -EncodedCommand $encoded"
    $psi.WorkingDirectory = (Get-Location).Path
    $psi.UseShellExecute = $false

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    [void]$proc.Start()

    if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
        try {
            $proc.Kill()
        } catch {
        }
        throw "Step '$Name' timed out after ${TimeoutSec}s"
    }

    if ($proc.ExitCode -ne 0) {
        throw "Step '$Name' failed with exit code $($proc.ExitCode)"
    }
}

$fmtCmd = "& '$cargo' fmt --all -- --check"
$buildCmd = "& '$cargo' test --workspace --no-run"
$testCmd = "& '$cargo' test --workspace"

Invoke-Step -Name "format-check" -Command $fmtCmd -TimeoutSec 120
Invoke-Step -Name "build-check" -Command $buildCmd -TimeoutSec 900
if (-not $SkipTests) {
    Invoke-Step -Name "test-run" -Command $testCmd -TimeoutSec 900
}

Write-Host "Baseline checks completed."
