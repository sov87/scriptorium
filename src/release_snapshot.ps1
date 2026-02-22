param(
  [string]$Config = "configs\window_0597_0865.toml"
)

.\.venv\Scripts\activate

python .\src\rights_lint.py
if ($LASTEXITCODE -ne 0) { throw "rights_lint failed" }

python -m scriptorium doctor --config $Config --strict --json
if ($LASTEXITCODE -ne 0) { throw "doctor failed" }

python -m scriptorium release --config $Config --snapshot
if ($LASTEXITCODE -ne 0) { throw "release failed" }