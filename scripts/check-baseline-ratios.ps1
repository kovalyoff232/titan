param(
    [Parameter(Mandatory = $true)]
    [string]$ReportJson,
    [Parameter(Mandatory = $true)]
    [string]$ThresholdJson
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ReportJson)) {
    throw "Baseline report not found: $ReportJson"
}
if (-not (Test-Path $ThresholdJson)) {
    throw "Ratio threshold config not found: $ThresholdJson"
}

$report = Get-Content -Raw $ReportJson | ConvertFrom-Json
$threshold = Get-Content -Raw $ThresholdJson | ConvertFrom-Json

$titanEngine = [string]$threshold.titan_engine
$referenceEngine = [string]$threshold.reference_engine
$minReferenceMs = 0.5
if ($null -ne $threshold.min_reference_ms) {
    $minReferenceMs = [double]$threshold.min_reference_ms
}

$titanResult = @($report.results | Where-Object { $_.engine -eq $titanEngine } | Select-Object -First 1)
$referenceResult = @($report.results | Where-Object { $_.engine -eq $referenceEngine } | Select-Object -First 1)

if (-not $titanResult) {
    throw "Engine '$titanEngine' not found in baseline report"
}
if (-not $referenceResult) {
    throw "Engine '$referenceEngine' not found in baseline report"
}
if ($titanResult.status -ne "ok") {
    throw "Engine '$titanEngine' benchmark status is '$($titanResult.status)'"
}
if ($referenceResult.status -ne "ok") {
    throw "Engine '$referenceEngine' benchmark status is '$($referenceResult.status)'"
}

$failures = @()
$checks = @()

foreach ($limitProp in $threshold.limits.PSObject.Properties) {
    $queryName = $limitProp.Name
    $limit = $limitProp.Value
    $maxP95Ratio = [double]$limit.max_p95_ratio
    $maxMeanRatio = [double]$limit.max_mean_ratio

    $titanMetric = @($titanResult.metrics | Where-Object { $_.name -eq $queryName } | Select-Object -First 1)
    $referenceMetric = @($referenceResult.metrics | Where-Object { $_.name -eq $queryName } | Select-Object -First 1)

    if (-not $titanMetric) {
        $failures += "Missing Titan metric '$queryName'"
        continue
    }
    if (-not $referenceMetric) {
        $failures += "Missing reference metric '$queryName'"
        continue
    }

    $titanP95 = [double]$titanMetric.p95_ms
    $titanMean = [double]$titanMetric.mean_ms
    $referenceP95 = [Math]::Max(([double]$referenceMetric.p95_ms), $minReferenceMs)
    $referenceMean = [Math]::Max(([double]$referenceMetric.mean_ms), $minReferenceMs)

    $p95Ratio = $titanP95 / $referenceP95
    $meanRatio = $titanMean / $referenceMean

    $checks += [pscustomobject]@{
        Query = $queryName
        TitanP95 = $titanP95
        RefP95 = $referenceP95
        P95Ratio = $p95Ratio
        MaxP95Ratio = $maxP95Ratio
        TitanMean = $titanMean
        RefMean = $referenceMean
        MeanRatio = $meanRatio
        MaxMeanRatio = $maxMeanRatio
    }

    if ($p95Ratio -gt $maxP95Ratio) {
        $failures += "Query '$queryName' p95 ratio=$([Math]::Round($p95Ratio, 4)) exceeds limit=$maxP95Ratio"
    }
    if ($meanRatio -gt $maxMeanRatio) {
        $failures += "Query '$queryName' mean ratio=$([Math]::Round($meanRatio, 4)) exceeds limit=$maxMeanRatio"
    }
}

Write-Host "Baseline ratio checks ($titanEngine vs $referenceEngine):"
foreach ($check in $checks) {
    Write-Host (" - {0}: p95_ratio={1} (limit {2}), mean_ratio={3} (limit {4})" -f `
            $check.Query, `
            ([Math]::Round($check.P95Ratio, 4)), `
            $check.MaxP95Ratio, `
            ([Math]::Round($check.MeanRatio, 4)), `
            $check.MaxMeanRatio)
}

if ($failures.Count -gt 0) {
    Write-Host ""
    Write-Host "Ratio threshold violations:"
    foreach ($failure in $failures) {
        Write-Host " - $failure"
    }
    exit 1
}

Write-Host "All baseline ratio thresholds passed."
