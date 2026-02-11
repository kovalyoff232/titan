param(
    [Parameter(Mandatory = $true)]
    [string]$ReportJson,
    [Parameter(Mandatory = $true)]
    [string]$ThresholdJson
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ReportJson)) {
    throw "Benchmark report not found: $ReportJson"
}
if (-not (Test-Path $ThresholdJson)) {
    throw "Threshold config not found: $ThresholdJson"
}

$report = Get-Content -Raw $ReportJson | ConvertFrom-Json
$threshold = Get-Content -Raw $ThresholdJson | ConvertFrom-Json

$engineName = [string]$threshold.engine
$engineResult = $report.results | Where-Object { $_.engine -eq $engineName } | Select-Object -First 1
if (-not $engineResult) {
    throw "Engine '$engineName' not found in benchmark report"
}
if ($engineResult.status -ne "ok") {
    throw "Engine '$engineName' benchmark status is '$($engineResult.status)'"
}

$failures = @()
$checks = @()

foreach ($limitProp in $threshold.limits.PSObject.Properties) {
    $queryName = $limitProp.Name
    $limit = $limitProp.Value
    $metric = $engineResult.metrics | Where-Object { $_.name -eq $queryName } | Select-Object -First 1

    if (-not $metric) {
        $failures += "Missing metric '$queryName' in report"
        continue
    }

    $p95 = [double]$metric.p95_ms
    $mean = [double]$metric.mean_ms
    $maxP95 = [double]$limit.max_p95_ms
    $maxMean = [double]$limit.max_mean_ms

    $checks += [pscustomobject]@{
        Query = $queryName
        P95 = $p95
        MaxP95 = $maxP95
        Mean = $mean
        MaxMean = $maxMean
    }

    if ($p95 -gt $maxP95) {
        $failures += "Query '$queryName' p95_ms=$p95 exceeds max_p95_ms=$maxP95"
    }
    if ($mean -gt $maxMean) {
        $failures += "Query '$queryName' mean_ms=$mean exceeds max_mean_ms=$maxMean"
    }
}

Write-Host "Benchmark threshold checks:"
foreach ($check in $checks) {
    Write-Host (" - {0}: p95={1} (limit {2}), mean={3} (limit {4})" -f `
            $check.Query, `
            ([Math]::Round($check.P95, 4)), `
            $check.MaxP95, `
            ([Math]::Round($check.Mean, 4)), `
            $check.MaxMean)
}

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "Threshold violations:"
    foreach ($failure in $failures) {
        Write-Host " - $failure"
    }
    exit 1
}

Write-Host "All benchmark thresholds passed."
