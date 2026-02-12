param(
    [string]$PanicReport = "target/reports/panic-inventory.json",
    [string]$RecoveryReport = "target/reports/recovery-reconciliation.json",
    [string]$BenchmarkReport = "target/benchmarks/smoke-ci.json",
    [string]$TestReport = "target/reports/test-timeout-report.json",
    [string]$OutJson = "target/reports/weekly-kpi.json",
    [string]$OutTxt = "target/reports/weekly-kpi.txt"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
Set-Location (Resolve-Path "$PSScriptRoot\..")

function Resolve-WorkspacePath {
    param([Parameter(Mandatory = $true)][string]$PathValue)

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return $PathValue
    }
    return (Join-Path (Get-Location) $PathValue)
}

function Ensure-ParentDirectory {
    param([Parameter(Mandatory = $true)][string]$PathValue)

    $dir = Split-Path -Path $PathValue -Parent
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

function Read-JsonReport {
    param([Parameter(Mandatory = $true)][string]$PathValue)

    $resolved = Resolve-WorkspacePath -PathValue $PathValue
    if (-not (Test-Path $resolved)) {
        return $null
    }

    $raw = Get-Content -Raw -Path $resolved
    if (-not $raw) {
        return $null
    }
    return ($raw | ConvertFrom-Json)
}

function Round-Value {
    param([Parameter(Mandatory = $true)][double]$Value, [int]$Digits = 2)
    return [Math]::Round($Value, $Digits)
}

$panic = Read-JsonReport -PathValue $PanicReport
$recovery = Read-JsonReport -PathValue $RecoveryReport
$benchmark = Read-JsonReport -PathValue $BenchmarkReport
$tests = Read-JsonReport -PathValue $TestReport

$panicStatus = "missing"
$panicTotal = 0
$panicHigh = 0
$panicMedium = 0
$panicLow = 0
if ($panic) {
    $panicTotal = [int]$panic.summary.total
    $panicHigh = [int]$panic.summary.high
    $panicMedium = [int]$panic.summary.medium
    $panicLow = [int]$panic.summary.low
    $panicStatus = if ($panicTotal -eq 0) { "ok" } else { "fail" }
}

$recoveryStatus = "missing"
$recoveryFirst = $false
$recoverySecond = $false
if ($recovery) {
    $recoveryFirst = [bool]$recovery.match_after_first_restart
    $recoverySecond = [bool]$recovery.match_after_second_restart
    $recoveryStatus = if ($recoveryFirst -and $recoverySecond) { "ok" } else { "fail" }
}

$benchmarkStatus = "missing"
$benchmarkMetricCount = 0
$benchmarkAvgP95Ms = 0.0
$benchmarkMaxP95Ms = 0.0
$benchmarkEngineStatus = ""
if ($benchmark) {
    $titanResult = @($benchmark.results | Where-Object { $_.engine -eq "titan" } | Select-Object -First 1)
    if ($titanResult.Count -gt 0) {
        $titan = $titanResult[0]
        $benchmarkEngineStatus = [string]$titan.status
        if ($titan.status -eq "ok") {
            $metrics = @($titan.metrics)
            $benchmarkMetricCount = $metrics.Count
            if ($benchmarkMetricCount -gt 0) {
                $sumP95 = [double](($metrics | Measure-Object -Property p95_ms -Sum).Sum)
                $maxP95 = [double](($metrics | Measure-Object -Property p95_ms -Maximum).Maximum)
                $benchmarkAvgP95Ms = Round-Value -Value ($sumP95 / $benchmarkMetricCount) -Digits 4
                $benchmarkMaxP95Ms = Round-Value -Value $maxP95 -Digits 4
            }
            $benchmarkStatus = "ok"
        } else {
            $benchmarkStatus = "fail"
        }
    } else {
        $benchmarkStatus = "missing"
    }
}

$testsStatus = "missing"
$testsTargetCount = 0
$testsTotalElapsedSec = 0.0
$testsSlowestLabel = ""
$testsSlowestSec = 0.0
$testsTimeoutSec = 0
if ($tests) {
    $targets = @($tests.targets)
    $testsTargetCount = $targets.Count
    $testsTimeoutSec = [int]$tests.timeout_sec
    if ($testsTargetCount -gt 0) {
        $testsTotalElapsedSec = Round-Value -Value ([double](($targets | Measure-Object -Property elapsed_sec -Sum).Sum))
        $slowest = @($targets | Sort-Object -Property elapsed_sec -Descending | Select-Object -First 1)
        if ($slowest.Count -gt 0) {
            $testsSlowestLabel = [string]$slowest[0].label
            $testsSlowestSec = Round-Value -Value ([double]$slowest[0].elapsed_sec)
        }
    }
    $testsStatus = "ok"
}

$score = 100
if ($panicStatus -eq "missing") { $score -= 10 }
if ($panicStatus -eq "fail") { $score -= 35 }
if ($recoveryStatus -eq "missing") { $score -= 10 }
if ($recoveryStatus -eq "fail") { $score -= 25 }
if ($benchmarkStatus -eq "missing") { $score -= 15 }
if ($benchmarkStatus -eq "fail") { $score -= 25 }
if ($benchmarkStatus -eq "ok") {
    if ($benchmarkAvgP95Ms -gt 60) { $score -= 20 }
    elseif ($benchmarkAvgP95Ms -gt 30) { $score -= 10 }
}
if ($testsStatus -eq "missing") { $score -= 10 }
if ($testsStatus -eq "ok" -and $testsTimeoutSec -gt 0 -and $testsSlowestSec -gt 0) {
    $ratio = $testsSlowestSec / [double]$testsTimeoutSec
    if ($ratio -ge 0.85) { $score -= 10 }
    elseif ($ratio -ge 0.70) { $score -= 5 }
}

$score = [Math]::Max(0, [Math]::Min(100, $score))
$qualityBand = if ($score -ge 90) {
    "excellent"
} elseif ($score -ge 75) {
    "good"
} elseif ($score -ge 60) {
    "fair"
} else {
    "at_risk"
}

$report = [pscustomobject]@{
    generated_at = (Get-Date).ToString("o")
    score = $score
    quality_band = $qualityBand
    sections = [pscustomobject]@{
        correctness = [pscustomobject]@{
            panic_status = $panicStatus
            panic_total = $panicTotal
            panic_high = $panicHigh
            panic_medium = $panicMedium
            panic_low = $panicLow
        }
        durability = [pscustomobject]@{
            recovery_status = $recoveryStatus
            match_after_first_restart = $recoveryFirst
            match_after_second_restart = $recoverySecond
        }
        performance = [pscustomobject]@{
            benchmark_status = $benchmarkStatus
            titan_engine_status = $benchmarkEngineStatus
            query_metrics = $benchmarkMetricCount
            avg_p95_ms = $benchmarkAvgP95Ms
            max_p95_ms = $benchmarkMaxP95Ms
        }
        reliability = [pscustomobject]@{
            tests_status = $testsStatus
            total_targets = $testsTargetCount
            total_elapsed_sec = $testsTotalElapsedSec
            timeout_sec = $testsTimeoutSec
            slowest_target = $testsSlowestLabel
            slowest_elapsed_sec = $testsSlowestSec
        }
    }
}

$outJsonPath = Resolve-WorkspacePath -PathValue $OutJson
Ensure-ParentDirectory -PathValue $outJsonPath
$report | ConvertTo-Json -Depth 8 | Set-Content -Path $outJsonPath -Encoding UTF8

$outTxtPath = Resolve-WorkspacePath -PathValue $OutTxt
Ensure-ParentDirectory -PathValue $outTxtPath

$summaryLines = @(
    "Titan Weekly KPI",
    "generated_at: $($report.generated_at)",
    "score: $($report.score)",
    "quality_band: $($report.quality_band)",
    "panic_status: $panicStatus (total=$panicTotal, high=$panicHigh, medium=$panicMedium, low=$panicLow)",
    "recovery_status: $recoveryStatus (first_restart=$recoveryFirst, second_restart=$recoverySecond)",
    "benchmark_status: $benchmarkStatus (engine=$benchmarkEngineStatus, metrics=$benchmarkMetricCount, avg_p95_ms=$benchmarkAvgP95Ms, max_p95_ms=$benchmarkMaxP95Ms)",
    "tests_status: $testsStatus (targets=$testsTargetCount, total_elapsed_sec=$testsTotalElapsedSec, timeout_sec=$testsTimeoutSec, slowest='$testsSlowestLabel' ${testsSlowestSec}s)"
)
$summaryLines -join [Environment]::NewLine | Set-Content -Path $outTxtPath -Encoding UTF8

Write-Host "Weekly KPI report written: $outJsonPath"
Write-Host "Weekly KPI text summary written: $outTxtPath"
Write-Host "Score: $score ($qualityBand)"

if ($env:GITHUB_STEP_SUMMARY) {
    $md = @(
        "## Weekly KPI",
        "",
        "- Score: **$score** ($qualityBand)",
        "- Panic status: **$panicStatus** (total=$panicTotal, high=$panicHigh, medium=$panicMedium, low=$panicLow)",
        "- Recovery status: **$recoveryStatus** (first=$recoveryFirst, second=$recoverySecond)",
        "- Benchmark status: **$benchmarkStatus** (engine=$benchmarkEngineStatus, avg_p95_ms=$benchmarkAvgP95Ms, max_p95_ms=$benchmarkMaxP95Ms)",
        "- Tests status: **$testsStatus** (targets=$testsTargetCount, slowest=$testsSlowestSec s)"
    )
    Add-Content -Path $env:GITHUB_STEP_SUMMARY -Value ($md -join [Environment]::NewLine)
}
