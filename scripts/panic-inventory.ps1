param(
    [string]$OutJson = "target/reports/panic-inventory.json",
    [switch]$IncludeTests,
    [switch]$FailOnAny
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-RgExe {
    $rgCmd = Get-Command rg -ErrorAction SilentlyContinue
    if ($rgCmd) {
        return $rgCmd.Source
    }

    throw "rg executable not found in PATH"
}

function Get-FirstCfgTestLine {
    param([string]$FilePath)

    $lines = Get-Content -Path $FilePath
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match '^\s*#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]') {
            return ($i + 1)
        }
    }
    return 0
}

$rgExe = Resolve-RgExe
$targets = @("bedrock/src", "titan_bin/src")
$pattern = 'unwrap\(|expect\(|panic!\('

$rawMatches = & $rgExe -n --no-heading --color never $pattern @targets

$cfgTestLineByFile = @{}
$entries = @()

foreach ($raw in $rawMatches) {
    if ($raw -notmatch '^(.+?):(\d+):(.*)$') {
        continue
    }

    $file = $matches[1]
    $line = [int]$matches[2]
    $snippet = $matches[3].Trim()

    if (-not $cfgTestLineByFile.ContainsKey($file)) {
        $cfgTestLineByFile[$file] = Get-FirstCfgTestLine -FilePath $file
    }
    $cfgTestLine = [int]$cfgTestLineByFile[$file]
    $likelyTestScope = $cfgTestLine -gt 0 -and $line -gt $cfgTestLine
    if ($IncludeTests.IsPresent -eq $false -and $likelyTestScope) {
        continue
    }

    $severity = if ($likelyTestScope) {
        "low"
    } elseif (
        $file -eq "titan_bin/src/lib.rs" -or
        $file -eq "bedrock/src/transaction.rs" -or
        $file -eq "bedrock/src/wal.rs" -or
        $file -eq "bedrock/src/buffer_pool.rs"
    ) {
        "high"
    } else {
        "medium"
    }

    $entries += [pscustomobject]@{
        file              = $file
        line              = $line
        snippet           = $snippet
        likely_test_scope = $likelyTestScope
        severity          = $severity
    }
}

$summary = [pscustomobject]@{
    total = $entries.Count
    high  = @($entries | Where-Object { $_.severity -eq "high" }).Count
    medium = @($entries | Where-Object { $_.severity -eq "medium" }).Count
    low   = @($entries | Where-Object { $_.severity -eq "low" }).Count
}

$report = [pscustomobject]@{
    generated_at = (Get-Date).ToString("o")
    include_tests = $IncludeTests.IsPresent
    summary = $summary
    entries = $entries
}

$outPath = if ([System.IO.Path]::IsPathRooted($OutJson)) {
    $OutJson
} else {
    Join-Path (Get-Location) $OutJson
}

$outDir = Split-Path -Path $outPath -Parent
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

$json = $report | ConvertTo-Json -Depth 6
Set-Content -Path $outPath -Value $json -Encoding UTF8

Write-Host "Panic inventory generated: $outPath"
Write-Host ("Summary: total={0}, high={1}, medium={2}, low={3}" -f $summary.total, $summary.high, $summary.medium, $summary.low)

if ($FailOnAny.IsPresent -and $summary.total -gt 0) {
    throw "panic inventory contains disallowed runtime panic sites ($($summary.total))"
}
