param(
    [int]$TimeoutSec = 300
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-CargoExe {
    $cargoCmd = Get-Command cargo -ErrorAction SilentlyContinue
    if ($cargoCmd) {
        return $cargoCmd.Source
    }

    $fallback = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
    if (Test-Path $fallback) {
        return $fallback
    }

    throw "cargo executable not found in PATH or $fallback"
}

function Invoke-CargoWithTimeout {
    param(
        [string]$CargoExe,
        [string]$Label,
        [string[]]$CargoArgs,
        [int]$TimeoutSeconds
    )

    Write-Host ""
    Write-Host "=== $Label ==="
    Write-Host "$CargoExe $($CargoArgs -join ' ')"

    $start = Get-Date
    $escapedArgs = $CargoArgs | ForEach-Object {
        if ($_ -match '\s') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ }
    }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $CargoExe
    $psi.Arguments = ($escapedArgs -join ' ')
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi
    [void]$proc.Start()

    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()

    if (-not $proc.WaitForExit($TimeoutSeconds * 1000)) {
        Write-Warning "Timeout exceeded (${TimeoutSeconds}s). Killing process $($proc.Id)."
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
        throw "Timed out: cargo $($CargoArgs -join ' ')"
    }

    $proc.WaitForExit()
    $stdout = $stdoutTask.Result
    $stderr = $stderrTask.Result

    $exitCode = $proc.ExitCode
    if ($exitCode -ne 0) {
        if ($stdout) {
            Write-Host "---- stdout (tail) ----"
            $stdout.Split([Environment]::NewLine) | Select-Object -Last 120 | ForEach-Object { Write-Host $_ }
        }
        if ($stderr) {
            Write-Host "---- stderr (tail) ----"
            $stderr.Split([Environment]::NewLine) | Select-Object -Last 120 | ForEach-Object { Write-Host $_ }
        }
        throw "Failed with exit code ${exitCode}: cargo $($CargoArgs -join ' ')"
    }

    $elapsed = [Math]::Round(((Get-Date) - $start).TotalSeconds, 2)
    Write-Host "Completed in ${elapsed}s"
}

$cargoExe = Resolve-CargoExe

$testTargets = @(
    [pscustomobject]@{
        Label = "bedrock lib tests"
        Args = @("test", "-p", "bedrock", "--lib", "--verbose")
    },
    [pscustomobject]@{
        Label = "titan_bin lib tests"
        Args = @("test", "-p", "titan_bin", "--lib", "--verbose")
    }
)

$integrationTargets = Get-ChildItem -Path "titan_bin/tests" -File -Filter "*.rs" |
    Sort-Object Name |
    ForEach-Object { $_.BaseName }

foreach ($name in $integrationTargets) {
    $testTargets += [pscustomobject]@{
        Label = "titan_bin integration test: $name"
        Args = @("test", "-p", "titan_bin", "--test", $name, "--verbose")
    }
}

foreach ($target in $testTargets) {
    Invoke-CargoWithTimeout -CargoExe $cargoExe -Label $target.Label -CargoArgs $target.Args -TimeoutSeconds $TimeoutSec
}

Write-Host ""
Write-Host "All test targets completed successfully with timeout ${TimeoutSec}s."
