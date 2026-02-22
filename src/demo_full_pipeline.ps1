param(
  [string]$Config = "configs\window_0597_0865.toml"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

Write-Host "[demo] root: $root"
Write-Host "[demo] config: $Config"

python -m scriptorium doctor --config $Config --strict --json | Out-Host

python -m scriptorium catalog-status --config $Config | Out-Host
python -m scriptorium catalog-fetch  --config $Config | Out-Host
python -m scriptorium catalog-ingest --config $Config | Out-Host

python -m scriptorium db-build  --config $Config --overwrite | Out-Host
python -m scriptorium vec-build --config $Config | Out-Host

Write-Host "[demo] sample search: Beowulf token"
python -m scriptorium db-search --config $Config --q "we" --k 3 --corpus oe_beowulf_9700 | Out-Host

Write-Host "[demo] sample hybrid retrieve: Beowulf query"
python -m scriptorium retrieve --config $Config --q "What is said of Scyld?" --k 5 --corpus oe_beowulf_9700 | Out-Host

Write-Host "[demo] snapshot (rights-gated canon + db included)"
python -m scriptorium release --config $Config --snapshot --skip-ps | Out-Host
