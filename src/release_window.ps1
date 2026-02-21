param(
  [string]$Config = "configs\window_0597_0865.toml",
  [switch]$Snapshot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Run from project root even if invoked elsewhere
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
  $args = @("-m","scriptorium","release","--config",$Config)
  if ($Snapshot) { $args += "--snapshot" }
  python @args
}
finally {
  Pop-Location
}