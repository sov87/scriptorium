param(
  [Parameter(Mandatory = $true)]
  [string]$Window,

  [Parameter(Mandatory = $true)]
  [string]$Tag,

  # Optional override; if omitted we use: configs\window_<Window>.toml
  [string]$Config = "",

  [Alias("Snapshot")]
  [switch]$SnapshotRelease
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Project root = parent of /src
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  # Prefer venv python if present
  $py = Join-Path $root ".venv\Scripts\python.exe"
  if (-not (Test-Path -LiteralPath $py)) {
    $pyCmd = Get-Command python -ErrorAction Stop
    $py = $pyCmd.Source
  }

  function Resolve-UnderRoot([string]$p) {
    if ([string]::IsNullOrWhiteSpace($p)) { return $null }
    if ([IO.Path]::IsPathRooted($p)) { return $p }
    return (Join-Path $root $p)
  }

  # Resolve config path (no TOML parsing needed here)
  if ([string]::IsNullOrWhiteSpace($Config)) {
    $Config = ("configs\window_{0}.toml" -f $Window)
  }
  $cfgPath = Resolve-UnderRoot $Config
  if (-not (Test-Path -LiteralPath $cfgPath)) { throw "Config not found: $cfgPath" }
  $cfgPath = (Resolve-Path -LiteralPath $cfgPath).Path

  $window = [string]$Window
  $tag    = [string]$Tag

  Write-Host "[release_window] root   = $root"
  Write-Host "[release_window] config = $cfgPath"
  Write-Host "[release_window] window = $window"
  Write-Host "[release_window] tag    = $tag"

  # Paths
  $dataProc = Join-Path $root "data_proc"
  $dataGen  = Join-Path $root "data_gen"
  $reports  = Join-Path $root "reports"
  $srcDir   = Join-Path $root "src"

  if (-not (Test-Path -LiteralPath $reports)) {
    New-Item -ItemType Directory -Path $reports | Out-Null
  }

  # Canon inputs
  $asc = Join-Path $dataProc "asc_A_prod.jsonl"
  if (-not (Test-Path -LiteralPath $asc)) { throw "Missing ASC canon: $asc" }

  $bedeCandidates = @(
    (Join-Path $dataProc "oe_bede_prod_utf8.jsonl"),
    (Join-Path $dataProc "oe_bede_prod.jsonl")
  )
  $bede = $bedeCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if (-not $bede) { throw "Missing Bede canon (expected oe_bede_prod_utf8.jsonl or oe_bede_prod.jsonl under $dataProc)" }

  function Select-MachineFile([string]$dir, [string]$window, [string]$tag) {
    if (-not (Test-Path -LiteralPath $dir)) { return $null }

    $files = Get-ChildItem -LiteralPath $dir -Filter "reader_machine*.jsonl" -File -ErrorAction SilentlyContinue |
             Sort-Object LastWriteTime -Descending
    if (-not $files) { return $null }

    $w = [Regex]::Escape($window)
    $t = [Regex]::Escape($tag)

    $f = $files | Where-Object { $_.Name -match $w -and $_.Name -match $t -and $_.Name -match "final" } | Select-Object -First 1
    if (-not $f) { $f = $files | Where-Object { $_.Name -match $w -and $_.Name -match $t } | Select-Object -First 1 }
    if (-not $f) { $f = $files | Where-Object { $_.Name -match $w } | Select-Object -First 1 }
    if (-not $f) { $f = $files | Select-Object -First 1 }
    return $f
  }

  $machine = Select-MachineFile $dataGen $window $tag
  if (-not $machine) { throw "No machine file found under $dataGen (expected reader_machine*.jsonl)" }
  $machinePath = $machine.FullName
  Write-Host "[release_window] machine = $machinePath"

  # --- Validation (preferred: validate_reader_bundle.py; fallback: PS JSONL validator) ---

  $validateFile = Get-ChildItem -LiteralPath $srcDir -Recurse -Filter "validate_reader_bundle.py" -File -ErrorAction SilentlyContinue |
                  Sort-Object FullName |
                  Select-Object -First 1

  if ($validateFile) {
    Write-Host "[release_window] validate_reader_bundle.py -> $($validateFile.FullName)"
    & $py $validateFile.FullName --asc $asc --bede $bede --machine $machinePath
    if ($LASTEXITCODE -ne 0) { throw "validate_reader_bundle.py failed ($LASTEXITCODE)" }
  }
  else {
    Write-Warning "[release_window] validate_reader_bundle.py not found under src\; using PowerShell JSONL validator."

    function Test-Jsonl([string]$path, [switch]$RequireId) {
      $n = 0
      $lineNo = 0
      $firstObj = $null

      foreach ($line in Get-Content -LiteralPath $path) {
        $lineNo++
        $s = $line.Trim()
        if ($s -eq "") { continue }

        try {
          $obj = $s | ConvertFrom-Json -ErrorAction Stop
        } catch {
          throw ("[ERROR] {0}:{1}: invalid JSON: {2}" -f (Split-Path -Leaf $path), $lineNo, $_.Exception.Message)
        }

        if ($null -eq $firstObj) { $firstObj = $obj }

        if ($RequireId) {
          if (-not ($obj.PSObject.Properties.Name -contains "id")) {
            throw ("[ERROR] {0}:{1}: missing required 'id'" -f (Split-Path -Leaf $path), $lineNo)
          }
        }

        $n++
      }

      $keys = @()
      if ($firstObj -ne $null) { $keys = @($firstObj.PSObject.Properties.Name) }

      return [pscustomobject]@{
        Count    = $n
        FirstKeys = $keys
      }
    }

    # Require id for canon
    $ascInfo  = Test-Jsonl $asc -RequireId
    $bedeInfo = Test-Jsonl $bede -RequireId

    # Do NOT require id for machine (schema differs)
    $machInfo = Test-Jsonl $machinePath

    Write-Host ("[OK] fallback bundle validation: machine_records={0} asc_records={1} bede_records={2}" -f $machInfo.Count, $ascInfo.Count, $bedeInfo.Count)
    if ($machInfo.FirstKeys.Count -gt 0) {
      Write-Host ("[info] machine first keys: {0}" -f ($machInfo.FirstKeys -join ", "))
    }
  }

  # Render reader (use newest render_reader_*.py anywhere under src)
  $renderer = Get-ChildItem -LiteralPath $srcDir -Recurse -Filter "render_reader_*.py" -File -ErrorAction SilentlyContinue |
              Sort-Object LastWriteTime -Descending |
              Select-Object -First 1
  if (-not $renderer) { throw "No renderer found under $srcDir (expected render_reader_*.py)" }

  $readerMd   = Join-Path $reports ("reader_{0}_{1}.md" -f $window, $tag)
  $readerMeta = Join-Path $reports ("reader_{0}_{1}_meta.json" -f $window, $tag)

  Write-Host "[release_window] render -> $readerMd"
  & $py $renderer.FullName --asc $asc --bede $bede --machine $machinePath --out $readerMd --meta_out $readerMeta
  if ($LASTEXITCODE -ne 0) { throw "render_reader failed ($LASTEXITCODE)" }

  if ($SnapshotRelease) {
    Write-Host "[release_window] snapshot_bundle (include_canon=true; rights-gated)"

    $tmpPy = Join-Path $env:TEMP ("scriptorium_snapshot_{0}.py" -f ([guid]::NewGuid().ToString("N")))
    $pySrc = @'
import sys
import pathlib
from scriptorium.snapshot_bundle import build_snapshot_bundle

root = pathlib.Path(sys.argv[1])
window = sys.argv[2]
tag = sys.argv[3]
cfg = pathlib.Path(sys.argv[4])

extra = []
for rel in ["docs/RIGHTS_LEDGER.md", "docs/RELEASE_CHECKLIST.md"]:
    if (root / rel).exists():
        extra.append(rel)

p = build_snapshot_bundle(
    project_root=root,
    window=window,
    tag=tag,
    config_path=cfg,
    include_canon=True,
    include_extra=extra,
)
print(str(p))
'@
    Set-Content -LiteralPath $tmpPy -Value $pySrc -Encoding UTF8

    try {
      $snapPath = & $py -u $tmpPy $root $window $tag $cfgPath
      if ($LASTEXITCODE -ne 0) { throw "snapshot_bundle failed ($LASTEXITCODE)" }
      Write-Host "[OK] snapshot -> $snapPath"
    }
    finally {
      Remove-Item -LiteralPath $tmpPy -Force -ErrorAction SilentlyContinue
    }
  }

  Write-Host "[OK] reader -> $readerMd"
  Write-Host "[OK] meta   -> $readerMeta"
}
finally {
  Pop-Location
}