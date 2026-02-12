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

function Stop-TitanServerProcesses {
    Get-Process titan_bin -ErrorAction SilentlyContinue |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 120
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Command,
        [int]$TimeoutSec = 600
    )

    Write-Host "==> $Name"
    Stop-TitanServerProcesses

    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($Command))
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "powershell.exe"
    $psi.Arguments = "-NoProfile -EncodedCommand $encoded"
    $psi.WorkingDirectory = (Get-Location).Path
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi

    try {
        [void]$proc.Start()
        $start = Get-Date

        $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
        $stderrTask = $proc.StandardError.ReadToEndAsync()

        if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
            Write-Warning "Step '$Name' timed out after ${TimeoutSec}s. Killing process tree."
            try {
                $proc.Kill($true)
            } catch {
            }

            $stdout = $stdoutTask.Result
            $stderr = $stderrTask.Result
            if ($stdout) {
                Write-Host "---- stdout (tail) ----"
                $stdout.Split([Environment]::NewLine) | Select-Object -Last 80 | ForEach-Object { Write-Host $_ }
            }
            if ($stderr) {
                Write-Host "---- stderr (tail) ----"
                $stderr.Split([Environment]::NewLine) | Select-Object -Last 80 | ForEach-Object { Write-Host $_ }
            }

            throw "Step '$Name' timed out after ${TimeoutSec}s"
        }

        $proc.WaitForExit()
        $stdout = $stdoutTask.Result
        $stderr = $stderrTask.Result

        if ($proc.ExitCode -ne 0) {
            if ($stdout) {
                Write-Host "---- stdout (tail) ----"
                $stdout.Split([Environment]::NewLine) | Select-Object -Last 120 | ForEach-Object { Write-Host $_ }
            }
            if ($stderr) {
                Write-Host "---- stderr (tail) ----"
                $stderr.Split([Environment]::NewLine) | Select-Object -Last 120 | ForEach-Object { Write-Host $_ }
            }
            throw "Step '$Name' failed with exit code $($proc.ExitCode)"
        }

        $elapsedSec = [Math]::Round(((Get-Date) - $start).TotalSeconds, 2)
        Write-Host "Step '$Name' completed in ${elapsedSec}s"
    } finally {
        Stop-TitanServerProcesses
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
