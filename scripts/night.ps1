param(
  [Parameter(Mandatory=$true, Position=0)][ValidateSet("check","run","llvm")][string]$Command,
  [Parameter(Mandatory=$true, Position=1)][string]$File,
  [Parameter(ValueFromRemainingArguments=$true)][string[]]$ExtraArgs
)
$jar = if ($env:OW_JAR) { $env:OW_JAR } else { "ow-lang/target/OwIDE.jar" }
if (-not (Test-Path $jar)) {
  throw "OW compiler not found at $jar (set OW_JAR or build ow-lang)"
}
& java -jar $jar $Command $File @ExtraArgs
exit $LASTEXITCODE
