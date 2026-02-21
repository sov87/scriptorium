#requires -Version 5.1
[CmdletBinding()]
param(
  [string]$Config = "configs\sample_demo.toml",
  [string]$QueryText = "baptism and remission of sins"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$p) { [IO.Directory]::CreateDirectory($p) | Out-Null }

function Get-PythonExe {
  if ($env:VIRTUAL_ENV) {
    $venvPy = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPy) { return $venvPy }
  }
  $python = Get-Command python -ErrorAction SilentlyContinue
  if ($python) { return $python.Source }
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) { return $py.Source }
  throw "No Python found. Activate your venv or install Python."
}

function Py([string[]]$argsList) {
  $pyexe = Get-PythonExe
  if ($pyexe.EndsWith("\py.exe")) { & $pyexe -3 @argsList } else { & $pyexe @argsList }
  if ($LASTEXITCODE -ne 0) { throw "Python failed ($LASTEXITCODE): python $($argsList -join ' ')" }
}

$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
Push-Location $root
try {
  if (-not (Test-Path -LiteralPath $Config)) { throw "Missing config: $Config" }

  Write-Host "[1/5] python syntax preflight (compileall src)"
  Py @("-m","compileall","-q","src")

  # sample paths
  $bedeIn   = "sample_data\data_proc\oe_bede_sample_utf8.jsonl"
  $bm25Out  = "sample_data\indexes\bm25\oe_bede_prod_utf8.pkl"
  $vecDir   = "sample_data\indexes\vec_faiss"

  if (-not (Test-Path -LiteralPath $bedeIn)) { throw "Missing sample canon: $bedeIn" }
  Ensure-Dir (Split-Path -Parent $bm25Out)
  Ensure-Dir $vecDir

  $needBm25 = -not (Test-Path -LiteralPath $bm25Out)
  $idxAny = Get-ChildItem -LiteralPath $vecDir -File -Filter "*.index" -ErrorAction SilentlyContinue | Select-Object -First 1
  $needVec = -not $idxAny

  if ($needBm25 -or $needVec) {
    Write-Host "[2/5] building sample indexes (BM25 + FAISS)"
  } else {
    Write-Host "[2/5] sample indexes already present"
  }

  if ($needBm25) {
    Py @("src\build_bm25_bede_prod.py","--in",$bedeIn,"--out",$bm25Out)
  }

  if ($needVec) {
    Py @("src\build_vec_bede_faiss.py","--in",$bedeIn,"--out_dir",$vecDir,"--model","intfloat/multilingual-e5-base","--batch","16","--use_e5_prefix")
  }

  # Normalize vector bundle base name to oe_bede_prod.*
  $idx = Get-ChildItem -LiteralPath $vecDir -File -Filter "*.index" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (-not $idx) { throw "Vector index build did not produce a .index file in $vecDir" }

  $base = [IO.Path]::GetFileNameWithoutExtension($idx.Name)
  $ids  = Join-Path $vecDir ($base + "_ids.json")
  $meta = Join-Path $vecDir ($base + "_meta.jsonl")
  if (-not (Test-Path -LiteralPath $ids))  { throw "Missing companion: $(Split-Path -Leaf $ids)" }
  if (-not (Test-Path -LiteralPath $meta)) { throw "Missing companion: $(Split-Path -Leaf $meta)" }

  if ($base -ne "oe_bede_prod") {
    Copy-Item -Force $idx.FullName (Join-Path $vecDir "oe_bede_prod.index")
    Copy-Item -Force $ids          (Join-Path $vecDir "oe_bede_prod_ids.json")
    Copy-Item -Force $meta         (Join-Path $vecDir "oe_bede_prod_meta.jsonl")
  }

  Write-Host "[3/5] doctor (non-strict, JSON)"
  Py @("-m","scriptorium","doctor","--config",$Config,"--json")

  Write-Host "[4/5] query (pure retrieval; no LLM)"
  Py @("-m","scriptorium","query","--config",$Config,"--text",$QueryText)

  $qParent = "sample_data\runs\query_hybrid"
  $latestQ = Get-ChildItem -LiteralPath $qParent -Directory -Filter "q_*" -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (-not $latestQ) { throw "No query output folder found under $qParent" }

  $cand = Join-Path $latestQ.FullName "candidates.jsonl"
  if (-not (Test-Path -LiteralPath $cand)) { throw "Missing candidates.jsonl: $cand" }

  Write-Host "[5/5] answer --dry-run (retrieval-only; no LLM)"
  Py @("-m","scriptorium","answer","--config",$Config,"--text",$QueryText,"--dry-run")

  Write-Host "[OK] smoke test complete"
  Write-Host "     query candidates: $cand"
}
finally {
  Pop-Location
}