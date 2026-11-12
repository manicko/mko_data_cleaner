$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath $PSScriptRoot

$exitCode = 1
try {
    $pythonCmd = $null
    $pythonArgs = @()

    if (Get-Command py -ErrorAction SilentlyContinue) {
        $pythonCmd = 'py'
        $pythonArgs = @('-3')
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        $pythonCmd = 'python'
        $pythonArgs = @()
    } elseif (Get-Command uv -ErrorAction SilentlyContinue) {
        $pythonCmd = 'uv'
        $pythonArgs = @('run', 'python')
    }

    $scriptPath = Join-Path $PSScriptRoot 'run_report.py'

    if (-not $pythonCmd) {
        Write-Host 'No suitable Python launcher found (tried: py -3, python, uv run python).'
        $exitCode = 1
    } else {
        & $pythonCmd @pythonArgs $scriptPath
        if ($null -eq $LASTEXITCODE) {
            $exitCode = 0
        } else {
            $exitCode = $LASTEXITCODE
        }
    }
} catch {
    Write-Host 'RUN STATUS: FAILED - launcher error. No success claim.'
    Write-Host "$($_.Exception.GetType().Name): $($_.Exception.Message)"
    $exitCode = 1
} finally {
    Write-Host ''
    Write-Host "Script finished with exit code $exitCode."
    Read-Host 'Press Enter to close'
}

exit $exitCode
