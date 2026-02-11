param(
    [int]$Rows = 1000,
    [int]$Iterations = 40,
    [int]$Warmup = 5,
    [ValidateSet("titan", "postgres", "both")]
    [string]$Target = "both",
    [string]$PostgresDsn = "",
    [string]$OutJson = "",
    [int]$BuildTimeoutSec = 600,
    [int]$RunTimeoutSec = 900
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

function Quote-Arg {
    param([Parameter(Mandatory = $true)][string]$Arg)
    return "'" + ($Arg -replace "'", "''") + "'"
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

$buildCmd = "& '$cargo' build --bin titan_bin --bin baseline_runner"
Invoke-Step -Name "build-benchmark-binaries" -Command $buildCmd -TimeoutSec $BuildTimeoutSec

$args = @(
    "--rows", $Rows,
    "--iterations", $Iterations,
    "--warmup", $Warmup,
    "--target", $Target
)

if ($PostgresDsn) {
    $args += @("--postgres-dsn", (Quote-Arg $PostgresDsn))
}
if ($OutJson) {
    $args += @("--out-json", (Quote-Arg $OutJson))
}

$runCmd = "& '$cargo' run --quiet -p titan_bin --bin baseline_runner -- " + ($args -join " ")
Invoke-Step -Name "run-baseline-benchmark" -Command $runCmd -TimeoutSec $RunTimeoutSec

Write-Host "Baseline benchmark completed."
