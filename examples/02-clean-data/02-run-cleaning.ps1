<#
.SYNOPSIS
    Runs interactive mko_data_cleaner data cleaning (Script 2 of 2).

.DESCRIPTION
    Interactive PowerShell workflow for Windows that:
      - Resolves a Python interpreter/environment capable of running mko_data_cleaner.
      - Verifies default platformdirs settings and safely initializes them if missing.
      - Detects/validates saved repeat state in last_run_state.txt (same folder).
      - Supports first/new cleaning and repeat-previous cleaning flows.
      - Normalizes raw-data layout (report-dir mode or staging+junction mode).
      - Runs cleaning with explicit deterministic flags and captures structured results
        via a minimal colocated Python helper driver.
      - Optionally saves repeat state on user-confirmed successful runs.

    This script does not modify mko_data_cleaner module code. It keeps the console
    open on success and failure and returns non-zero exit codes on failure.

.NOTES
    - Requires Windows with PowerShell 5.1+.
    - Console messages are English-only.
    - State file: last_run_state.txt (created/updated only on user-confirmed save).
    - Staging workspace (raw-data-contents mode): .staging\default\
#>

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

# ----------------------------- Constants -----------------------------------
$script:ScriptName = '02-run-cleaning.ps1'
$script:ScriptRoot = $PSScriptRoot
$script:StateFileName = 'last_run_state.txt'
$script:StateFilePath = Join-Path $script:ScriptRoot $script:StateFileName
$script:StagingRoot = Join-Path $script:ScriptRoot '.staging\default'
$script:HelperFileName = '02-run-cleaning.driver.py'
$script:HelperPath = Join-Path $script:ScriptRoot $script:HelperFileName
$script:ExitSuccess = 0
$script:ExitFailure = 1

# ----------------------------- State ---------------------------------------
$script:ExitCode = $script:ExitSuccess
$script:FailureStep = $null
$script:FailureMessage = $null
$script:FailureNextAction = $null

# ----------------------------- UI Helpers ----------------------------------
function Write-Step {
    param([string]$Title)
    Write-Host ''
    Write-Host '============================================================'
    Write-Host $Title
    Write-Host '============================================================'
}

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] $Message"
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] $Message"
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[OK]   $Message"
}

function Write-Fail {
    param([string]$Message)
    Write-Host "[FAIL] $Message"
}

function Set-Failure {
    param(
        [Parameter(Mandatory = $true)][string]$Step,
        [Parameter(Mandatory = $true)][string]$Message,
        [Parameter(Mandatory = $true)][string]$NextAction
    )
    if (-not $script:FailureStep) {
        $script:FailureStep = $Step
        $script:FailureMessage = $Message
        $script:FailureNextAction = $NextAction
        $script:ExitCode = $script:ExitFailure
    }
}

function Read-YesNo {
    param(
        [Parameter(Mandatory = $true)][string]$Prompt,
        [bool]$Default = $false
    )
    $suffix = if ($Default) { '[Y/n]' } else { '[y/N]' }
    while ($true) {
        $answer = Read-Host "$Prompt $suffix"
        if ($null -eq $answer) { $answer = '' }
        $trimmed = $answer.Trim()
        if ($trimmed.Length -eq 0) { return $Default }
        switch ($trimmed.ToLowerInvariant()) {
            'y' { return $true }
            'yes' { return $true }
            'n' { return $false }
            'no' { return $false }
            default { Write-Host 'Please answer y or n.' }
        }
    }
}

function ConvertTo-BoolText {
    param([bool]$Value)
    if ($Value) { return 'true' } else { return 'false' }
}

function Parse-BoolStrict {
    param([string]$Text)
    if ($null -eq $Text) { return $null }
    switch ($Text.Trim().ToLowerInvariant()) {
        'true' { return $true }
        'false' { return $false }
        '1' { return $true }
        '0' { return $false }
        'yes' { return $true }
        'no' { return $false }
        'y' { return $true }
        'n' { return $false }
        default { return $null }
    }
}

function Quote-CommandArg {
    param([string]$Value)
    if ($null -eq $Value) { return '""' }
    # Use double-quote style suitable for PowerShell/cmd command display.
    $escaped = $Value -replace '"', '`"'
    return '"' + $escaped + '"'
}

function Resolve-ExistingAbsolutePath {
    param([string]$PathText)
    if (-not $PathText) { return $null }
    try {
        return (Resolve-Path -LiteralPath $PathText -ErrorAction Stop).Path
    } catch {
        return $null
    }
}

function Get-NormalizedAbsolutePath {
    param([string]$PathText)
    if (-not $PathText) { return $null }
    try {
        return [System.IO.Path]::GetFullPath($PathText)
    } catch {
        return $null
    }
}

# ----------------------------- External/JSON Helpers -----------------------
function Invoke-External {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter()][string[]]$Arguments = @()
    )
    $global:LASTEXITCODE = $null
    $output = & $FilePath @Arguments 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    $text = ($output | Out-String)
    return [pscustomobject]@{
        ExitCode = [int]$code
        Output   = $text
        Text     = $text.TrimEnd()
    }
}

function ConvertFrom-JsonBestEffort {
    param([string]$Text)
    if (-not $Text) { return $null }
    $trimmed = $Text.Trim()
    if (-not $trimmed) { return $null }
    try {
        return ($trimmed | ConvertFrom-Json -ErrorAction Stop)
    } catch { }

    $lines = @($trimmed -split "`r?`n" | Where-Object { $_.Trim().Length -gt 0 })
    for ($i = $lines.Count - 1; $i -ge 0; $i--) {
        $candidate = $lines[$i].Trim()
        if ($candidate.StartsWith('{') -or $candidate.StartsWith('[')) {
            try {
                return ($candidate | ConvertFrom-Json -ErrorAction Stop)
            } catch { }
        }
    }
    return $null
}

function Invoke-Driver {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string[]]$DriverArgs
    )
    if (-not (Test-Path -LiteralPath $script:HelperPath)) {
        return [pscustomobject]@{
            ExitCode = 1
            Output   = $null
            Text     = "Helper driver not found: $script:HelperPath"
            Object   = $null
            ParseError = $null
        }
    }

    $allArgs = @($script:HelperPath) + $DriverArgs
    $global:LASTEXITCODE = $null
    $output = & $PythonExe @allArgs 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    $text = ($output | Out-String)
    $obj = ConvertFrom-JsonBestEffort -Text $text
    $parseError = $null
    if (-not $obj -and $text.Trim().Length -gt 0) {
        $parseError = 'Could not parse helper JSON output.'
    }
    return [pscustomobject]@{
        ExitCode   = [int]$code
        Output     = $output
        Text       = $text.TrimEnd()
        Object     = $obj
        ParseError = $parseError
    }
}

# ----------------------------- Interpreter Discovery -----------------------
function Get-PythonCandidateSpecs {
    $specs = @()
    if (Get-Command -Name 'py' -ErrorAction SilentlyContinue) {
        $specs += @{ Label = 'py -3.14'; FilePath = 'py'; PrefixArgs = @('-3.14') }
        $specs += @{ Label = 'py -3.13'; FilePath = 'py'; PrefixArgs = @('-3.13') }
        $specs += @{ Label = 'py -3.12'; FilePath = 'py'; PrefixArgs = @('-3.12') }
        $specs += @{ Label = 'py -3';    FilePath = 'py'; PrefixArgs = @('-3') }
    }
    foreach ($name in @('python3', 'python')) {
        if (Get-Command -Name $name -ErrorAction SilentlyContinue) {
            $specs += @{ Label = $name; FilePath = $name; PrefixArgs = @() }
        }
    }
    return $specs
}

function Probe-PythonCandidate {
    param([hashtable]$Spec)
    $probeCode = @'
import json, sys
info = {'ok': False, 'executable': sys.executable, 'python_version': sys.version.split()[0]}
try:
    import mko_data_cleaner
    info['ok'] = True
    info['package_version'] = getattr(mko_data_cleaner, '__version__', None)
except Exception as e:
    info['error_type'] = type(e).__name__
    info['error_message'] = str(e)
print(json.dumps(info))
'@
    $global:LASTEXITCODE = $null
    $output = $probeCode | & $Spec.FilePath @($Spec.PrefixArgs) - 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    $text = ($output | Out-String)
    $obj = ConvertFrom-JsonBestEffort -Text $text
    if (-not $obj) { return $null }
    if ($obj.ok -ne $true) { return $null }
    $exe = [string]$obj.executable
    if (-not $exe) { return $null }
    return [pscustomobject]@{
        Label          = $Spec.Label
        Executable     = $exe
        PythonVersion  = [string]$obj.python_version
        PackageVersion = [string]$obj.package_version
        ProbeExitCode  = [int]$code
    }
}

function Probe-PythonExecutablePath {
    param([string]$PythonExe)
    if (-not $PythonExe) { return $null }
    $resolved = Get-NormalizedAbsolutePath -PathText $PythonExe
    if (-not $resolved) { return $null }
    if (-not (Test-Path -LiteralPath $resolved)) { return $null }
    $probeCode = @'
import json, sys
info = {'ok': False, 'executable': sys.executable, 'python_version': sys.version.split()[0]}
try:
    import mko_data_cleaner
    info['ok'] = True
    info['package_version'] = getattr(mko_data_cleaner, '__version__', None)
except Exception as e:
    info['error_type'] = type(e).__name__
    info['error_message'] = str(e)
print(json.dumps(info))
'@
    $global:LASTEXITCODE = $null
    $output = $probeCode | & $resolved - 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    $text = ($output | Out-String)
    $obj = ConvertFrom-JsonBestEffort -Text $text
    if (-not $obj -or $obj.ok -ne $true) { return $null }
    $exe = [string]$obj.executable
    if (-not $exe) { $exe = $resolved }
    return [pscustomobject]@{
        Label          = 'explicit'
        Executable     = $exe
        PythonVersion  = [string]$obj.python_version
        PackageVersion = [string]$obj.package_version
        ProbeExitCode  = [int]$code
    }
}

function Resolve-MkoPython {
    param([string]$PreferredExecutable)
    $seen = @{}

    if ($PreferredExecutable) {
        $preferred = Probe-PythonExecutablePath -PythonExe $PreferredExecutable
        if ($preferred) {
            $seen[$preferred.Executable.ToLowerInvariant()] = $true
            return $preferred
        }
    }

    foreach ($spec in (Get-PythonCandidateSpecs)) {
        $probe = Probe-PythonCandidate -Spec $spec
        if (-not $probe) { continue }
        $key = $probe.Executable.ToLowerInvariant()
        if ($seen.ContainsKey($key)) { continue }
        $seen[$key] = $true
        return $probe
    }

    return $null
}

# ----------------------------- State File ----------------------------------
function Read-StateFile {
    param([string]$Path)
    $result = [ordered]@{
        Exists = $false
        Valid = $false
        Reason = $null
        Data = $null
        Errors = @()
    }
    if (-not (Test-Path -LiteralPath $Path)) {
        $result.Reason = 'State file does not exist.'
        return [pscustomobject]$result
    }

    $result.Exists = $true
    $map = @{}
    $lineNo = 0
    try {
        foreach ($line in Get-Content -LiteralPath $Path -Encoding UTF8) {
            $lineNo++
            if ($null -eq $line) { continue }
            $trimmed = $line.Trim()
            if ($trimmed.Length -eq 0) { continue }
            if ($trimmed.StartsWith('#')) { continue }
            $eq = $trimmed.IndexOf('=')
            if ($eq -lt 1) {
                $result.Errors += "Line ${lineNo}: expected key=value format."
                continue
            }
            $key = $trimmed.Substring(0, $eq).Trim()
            $value = $trimmed.Substring($eq + 1)
            if ($key.Length -eq 0) {
                $result.Errors += "Line ${lineNo}: empty key."
                continue
            }
            # First occurrence wins for structured fields; unknown/duplicate keys ignored for forward safety.
            if (-not $map.ContainsKey($key)) {
                $map[$key] = $value
            }
        }
    } catch {
        $result.Reason = "Could not read state file: $($_.Exception.Message)"
        return [pscustomobject]$result
    }

    $required = @(
        'SchemaVersion',
        'SavedAt',
        'RawDataSelection',
        'RawDataMode',
        'ResolvedReportDir',
        'DictPath',
        'ClearExportFolder',
        'Force',
        'SettingsMode',
        'PythonExecutable',
        'LaunchCommand'
    )

    foreach ($k in $required) {
        if (-not $map.ContainsKey($k) -or [string]::IsNullOrWhiteSpace([string]$map[$k])) {
            $result.Errors += "Missing required field: $k"
        }
    }

    if ($result.Errors.Count -gt 0) {
        $result.Reason = ($result.Errors -join '; ')
        $result.Data = [pscustomobject]$map
        return [pscustomobject]$result
    }

    if (($map['SchemaVersion']).Trim() -ne '1') {
        $result.Errors += "Unsupported SchemaVersion: $($map['SchemaVersion']) (expected 1)."
        $result.Reason = ($result.Errors -join '; ')
        $result.Data = [pscustomobject]$map
        return [pscustomobject]$result
    }

    $mode = ($map['RawDataMode']).Trim()
    if ($mode -ne 'report_dir' -and $mode -ne 'raw_data_contents') {
        $result.Errors += "Invalid RawDataMode: $mode"
    }
    $clear = Parse-BoolStrict -Text ([string]$map['ClearExportFolder'])
    $force = Parse-BoolStrict -Text ([string]$map['Force'])
    if ($null -eq $clear) { $result.Errors += "Invalid ClearExportFolder boolean: $($map['ClearExportFolder'])" }
    if ($null -eq $force) { $result.Errors += "Invalid Force boolean: $($map['Force'])" }
    $settingsMode = ($map['SettingsMode']).Trim()
    if ($settingsMode -ne 'default_platformdirs') {
        $result.Errors += "Unsupported SettingsMode: $settingsMode (expected default_platformdirs)."
    }

    if ($result.Errors.Count -gt 0) {
        $result.Reason = ($result.Errors -join '; ')
        $result.Data = [pscustomobject]$map
        return [pscustomobject]$result
    }

    $result.Valid = $true
    $result.Data = [pscustomobject]@{
        SchemaVersion     = '1'
        SavedAt           = [string]$map['SavedAt']
        RawDataSelection  = [string]$map['RawDataSelection']
        RawDataMode       = $mode
        ResolvedReportDir = [string]$map['ResolvedReportDir']
        DictPath          = [string]$map['DictPath']
        ClearExportFolder = [bool]$clear
        Force             = [bool]$force
        SettingsMode      = $settingsMode
        PythonExecutable  = [string]$map['PythonExecutable']
        LaunchCommand     = [string]$map['LaunchCommand']
        RawMap            = $map
    }
    return [pscustomobject]$result
}

function Write-StateFileAtomic {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Fields
    )
    $orderedKeys = @(
        'SchemaVersion',
        'SavedAt',
        'RawDataSelection',
        'RawDataMode',
        'ResolvedReportDir',
        'DictPath',
        'ClearExportFolder',
        'Force',
        'SettingsMode',
        'PythonExecutable',
        'LaunchCommand'
    )
    $lines = @()
    foreach ($k in $orderedKeys) {
        if (-not $Fields.ContainsKey($k)) {
            throw "Cannot write state file: missing field '$k'."
        }
        $v = [string]$Fields[$k]
        # Single-line key-value format; normalize line breaks in values.
        $v = $v -replace "`r?`n", ' '
        $lines += "$k=$v"
    }
    $content = ($lines -join "`r`n") + "`r`n"
    $dir = Split-Path -Parent $Path
    if ($dir -and -not (Test-Path -LiteralPath $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    $tmp = "$Path.tmp"
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($tmp, $content, $utf8NoBom)
    Move-Item -LiteralPath $tmp -Destination $Path -Force
}

# ----------------------------- Layout/Junction Helpers ---------------------
function Test-ReparsePoint {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    try {
        $item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
        return (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0)
    } catch {
        return $false
    }
}

function Remove-JunctionSafely {
    param([string]$LinkPath)
    if (-not (Test-Path -LiteralPath $LinkPath)) { return $true }
    if (-not (Test-ReparsePoint -Path $LinkPath)) {
        return $false
    }
    # Use cmd rmdir to remove junction link itself safely on PowerShell 5.1.
    $quoted = '"' + ($LinkPath -replace '"', '""') + '"'
    $res = Invoke-External -FilePath 'cmd.exe' -Arguments @('/c', 'rmdir', $quoted)
    if ($res.ExitCode -ne 0) { return $false }
    if (Test-Path -LiteralPath $LinkPath) { return $false }
    return $true
}

function New-JunctionSafely {
    param(
        [Parameter(Mandatory = $true)][string]$LinkPath,
        [Parameter(Mandatory = $true)][string]$TargetPath
    )
    # Prefer New-Item -ItemType Junction; fall back to cmd mklink /J.
    try {
        $item = New-Item -ItemType Junction -Path $LinkPath -Target $TargetPath -Force -ErrorAction Stop
        if ($item -and (Test-Path -LiteralPath $LinkPath) -and (Test-ReparsePoint -Path $LinkPath)) {
            return $true
        }
    } catch { }

    $linkQ = '"' + ($LinkPath -replace '"', '""') + '"'
    $targetQ = '"' + ($TargetPath -replace '"', '""') + '"'
    $res = Invoke-External -FilePath 'cmd.exe' -Arguments @('/c', 'mklink', '/J', $linkQ, $targetQ)
    if ($res.ExitCode -ne 0) { return $false }
    return ((Test-Path -LiteralPath $LinkPath) -and (Test-ReparsePoint -Path $LinkPath))
}

function Resolve-Layout {
    param(
        [Parameter(Mandatory = $true)][string]$RawDataSelection
    )

    $resolvedSelection = Resolve-ExistingAbsolutePath -PathText $RawDataSelection
    if (-not $resolvedSelection) {
        return [pscustomobject]@{
            Ok = $false
            Step = 'Layout normalization'
            Message = "Selected raw data location does not exist: $RawDataSelection"
            NextAction = 'Provide an existing raw data folder path and rerun Script 2.'
        }
    }
    if (-not (Test-Path -LiteralPath $resolvedSelection -PathType Container)) {
        return [pscustomobject]@{
            Ok = $false
            Step = 'Layout normalization'
            Message = "Selected raw data location is not a directory: $resolvedSelection"
            NextAction = 'Provide a directory path for raw data (report folder or raw-data contents folder) and rerun Script 2.'
        }
    }

    $rawDataSubfolder = Join-Path $resolvedSelection 'raw_data'
    if (Test-Path -LiteralPath $rawDataSubfolder -PathType Container) {
        # Report-dir mode: treat selected path as report_dir.
        return [pscustomobject]@{
            Ok = $true
            Mode = 'report_dir'
            RawDataSelection = $resolvedSelection
            ResolvedReportDir = $resolvedSelection
            StagingRawDataLink = $null
            Notes = "Detected report-dir mode (found 'raw_data' subfolder)."
        }
    }

    # Raw-data-contents mode: use staging report dir with raw_data junction.
    try {
        if (-not (Test-Path -LiteralPath $script:StagingRoot)) {
            New-Item -ItemType Directory -Path $script:StagingRoot -Force | Out-Null
        }
        $stagingResolved = (Resolve-Path -LiteralPath $script:StagingRoot -ErrorAction Stop).Path
    } catch {
        return [pscustomobject]@{
            Ok = $false
            Step = 'Layout normalization'
            Message = "Failed to create or access staging directory '$($script:StagingRoot)': $($_.Exception.Message)"
            NextAction = 'Check write permissions for the examples/02-clean-data folder and rerun Script 2.'
        }
    }

    $linkPath = Join-Path $stagingResolved 'raw_data'

    if (Test-Path -LiteralPath $linkPath) {
        $isReparse = Test-ReparsePoint -Path $linkPath
        if ($isReparse) {
            if (-not (Remove-JunctionSafely -LinkPath $linkPath)) {
                return [pscustomobject]@{
                    Ok = $false
                    Step = 'Layout normalization'
                    Message = "Failed to safely remove existing staging junction: $linkPath"
                    NextAction = 'Close programs using the staging folder, verify permissions, and rerun Script 2. Do not manually delete source data.'
                }
            }
        } else {
            # Exists but not a reparse point (real dir/file). Refuse destructive replacement.
            return [pscustomobject]@{
                Ok = $false
                Step = 'Layout normalization'
                Message = "Staging path '$linkPath' exists but is not a junction/reparse point. Refusing to replace it automatically."
                NextAction = "Inspect '$linkPath'. If it is safe and no longer needed, remove or rename it manually, then rerun Script 2. Do not point it at or delete your source raw data."
            }
        }
    }

    $created = New-JunctionSafely -LinkPath $linkPath -TargetPath $resolvedSelection
    if (-not $created) {
        return [pscustomobject]@{
            Ok = $false
            Step = 'Layout normalization'
            Message = "Failed to create staging raw_data junction '$linkPath' -> '$resolvedSelection'."
            NextAction = 'Ensure the filesystem supports NTFS junctions and you have permission to create them in the examples folder, then rerun Script 2. Raw data was not copied.'
        }
    }

    return [pscustomobject]@{
        Ok = $true
        Mode = 'raw_data_contents'
        RawDataSelection = $resolvedSelection
        ResolvedReportDir = $stagingResolved
        StagingRawDataLink = $linkPath
        Notes = "Using staging report directory with raw_data junction (no raw-data copy)."
    }
}

# ----------------------------- Config/Input Preflight -----------------------
function Test-InputDataPresence {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string]$ResolvedReportDir
    )

    $cfg = Invoke-Driver -PythonExe $PythonExe -DriverArgs @('--action', 'config-info')
    if ($cfg.ParseError) {
        Write-Warn "Config preflight: helper output could not be parsed. Details: $($cfg.Text)"
        return [pscustomobject]@{ Ok = $true; Config = $null; Skipped = $true }
    }
    if ($cfg.ExitCode -ne 0 -or -not $cfg.Object -or $cfg.Object.ok -ne $true) {
        $msg = 'Could not read effective mko_data_cleaner settings/config for input preflight.'
        if ($cfg.Object -and $cfg.Object.error_message) { $msg += " $($cfg.Object.error_message)" }
        elseif ($cfg.Text) { $msg += " $($cfg.Text)" }
        $next = 'Run Script 1 to initialize/fix the environment and default settings, or inspect user settings files, then rerun Script 2.'
        return [pscustomobject]@{
            Ok = $false
            Step = 'Config/input preflight'
            Message = $msg
            NextAction = $next
            Config = $cfg.Object
        }
    }

    $config = $cfg.Object
    $importFolder = [string]$config.import_folder
    if (-not $importFolder) { $importFolder = 'raw_data' }
    $ext = [string]$config.data_extension
    if (-not $ext) { $ext = 'gz' }
    $extNorm = $ext.Trim().TrimStart('.').ToLowerInvariant()

    $importPath = Join-Path $ResolvedReportDir $importFolder
    if (-not (Test-Path -LiteralPath $importPath -PathType Container)) {
        return [pscustomobject]@{
            Ok = $false
            Step = 'Config/input preflight'
            Message = "Configured import folder was not found: '$importPath' (import_folder='$importFolder')."
            NextAction = "Ensure the resolved report directory contains '$importFolder' with input data files, or adjust the selected raw-data/report location, then rerun Script 2."
            Config = $config
        }
    }

    $pattern = "*.$extNorm"
    $files = @()
    try {
        $files = @(Get-ChildItem -LiteralPath $importPath -File -Filter $pattern -ErrorAction Stop)
    } catch {
        Write-Warn "Config preflight: could not enumerate input files in '$importPath': $($_.Exception.Message)"
        return [pscustomobject]@{ Ok = $true; Config = $config; Skipped = $true }
    }

    if ($files.Count -eq 0) {
        # Best-effort detection: fail clearly if obviously incompatible (no matching files found).
        $allFiles = @()
        try { $allFiles = @(Get-ChildItem -LiteralPath $importPath -File -ErrorAction Stop) } catch { }
        $sample = ($allFiles | Select-Object -First 5 -ExpandProperty Name) -join ', '
        $msg = "No input files matching configured extension '.$extNorm' were found in '$importPath'."
        if ($sample) { $msg += " Existing files (sample): $sample." }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Config/input preflight'
            Message = $msg
            NextAction = "Provide a raw-data location containing '.$extNorm' files in the configured import folder ('$importFolder'), or update the effective mko_data_cleaner settings if a different extension is intended, then rerun Script 2."
            Config = $config
        }
    }

    Write-Ok "Input preflight: found $($files.Count) file(s) matching '.$extNorm' in '$importPath'."
    return [pscustomobject]@{ Ok = $true; Config = $config; Skipped = $false; InputPath = $importPath; FileCount = $files.Count }
}

# ----------------------------- Settings Preflight --------------------------
function Ensure-SettingsReady {
    param([Parameter(Mandatory = $true)][string]$PythonExe)

    $cfg = Invoke-Driver -PythonExe $PythonExe -DriverArgs @('--action', 'config-info')
    if ($cfg.ExitCode -eq 0 -and $cfg.Object -and $cfg.Object.ok -eq $true) {
        Write-Ok "Default platformdirs settings detected: $($cfg.Object.settings_path)"
        return [pscustomobject]@{ Ok = $true; Config = $cfg.Object; InitializedNow = $false }
    }

    $missingLike = $false
    if ($cfg.Object) {
        if ($cfg.Object.error_type -eq 'SettingsNotInitializedError') { $missingLike = $true }
        if ($cfg.Object.missing_files) { $missingLike = $true }
    }
    if (-not $cfg.Object -and $cfg.Text -match 'SettingsNotInitializedError|missing|required file') { $missingLike = $true }

    if (-not $missingLike) {
        $msg = 'Settings preflight failed while checking default platformdirs settings.'
        if ($cfg.Object -and $cfg.Object.error_message) { $msg += " $($cfg.Object.error_message)" }
        elseif ($cfg.Text) { $msg += " $($cfg.Text)" }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Settings preflight'
            Message = $msg
            NextAction = 'Inspect the mko_data_cleaner installation and user settings directory, then rerun Script 2 (or run Script 1 to repair the environment).'
        }
    }

    Write-Info 'Default platformdirs settings are missing or incomplete. Running safe initialization (initialize_settings, no --force)...'
    $init = Invoke-Driver -PythonExe $PythonExe -DriverArgs @('--action', 'settings-init')
    if ($init.ParseError -or $init.ExitCode -ne 0 -or -not $init.Object -or $init.Object.ok -ne $true) {
        $msg = 'Automatic safe settings initialization failed.'
        if ($init.Object -and $init.Object.error_message) { $msg += " $($init.Object.error_message)" }
        elseif ($init.Text) { $msg += " $($init.Text)" }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Settings preflight'
            Message = $msg
            NextAction = 'Run Script 1 to initialize the environment/settings, verify the user config directory is writable, and rerun Script 2.'
        }
    }

    $createdApp = [bool]$init.Object.created_app_config
    $createdLog = [bool]$init.Object.created_log_config
    $statusParts = @()
    $statusParts += ("app_config.json5: " + ($(if ($createdApp) { 'created' } elseif ($init.Object.post.app_config_exists) { 'already present' } else { 'missing' })))
    $statusParts += ("log_config.json5: " + ($(if ($createdLog) { 'created' } elseif ($init.Object.post.log_config_exists) { 'already present' } else { 'missing' })))
    Write-Ok "Settings initialized safely (no overwrite). Actual settings path: $($init.Object.settings_path)"
    Write-Info ($statusParts -join '; ')

    $cfg2 = Invoke-Driver -PythonExe $PythonExe -DriverArgs @('--action', 'config-info')
    if ($cfg2.ExitCode -ne 0 -or -not $cfg2.Object -or $cfg2.Object.ok -ne $true) {
        $msg = 'Settings were initialized but effective config could not be read afterwards.'
        if ($cfg2.Object -and $cfg2.Object.error_message) { $msg += " $($cfg2.Object.error_message)" }
        elseif ($cfg2.Text) { $msg += " $($cfg2.Text)" }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Settings preflight'
            Message = $msg
            NextAction = 'Inspect the user settings files reported above and rerun Script 2.'
        }
    }

    return [pscustomobject]@{ Ok = $true; Config = $cfg2.Object; InitializedNow = $true; InitInfo = $init.Object }
}

# ----------------------------- Reporting/Execution --------------------------
function Format-ResultSummary {
    param([Parameter(Mandatory = $true)]$ResultObject)

    $r = $ResultObject
    $lines = @()
    if ($r.report_dir) { $lines += "Resolved report dir: $($r.report_dir)" }
    if ($r.import_dir) { $lines += "Import dir:         $($r.import_dir)" }
    if ($r.export_dir) { $lines += "Export dir:         $($r.export_dir)" }
    if ($r.db_path) { $lines += "Database path:      $($r.db_path)" }
    if ($null -ne $r.rows_loaded) { $lines += "Rows loaded:        $($r.rows_loaded)" }
    if ($null -ne $r.rows_exported) { $lines += "Rows exported:      $($r.rows_exported)" }
    if ($null -ne $r.duration) { $lines += "Duration (s):       $($r.duration)" }
    if ($r.started_at) { $lines += "Started at:         $($r.started_at)" }
    if ($r.finished_at) { $lines += "Finished at:        $($r.finished_at)" }

    if ($r.null_data_files) {
        $ndf = @($r.null_data_files)
        $lines += "Null-data files:    $($ndf.Count)"
        if ($ndf.Count -gt 0) {
            $sample = ($ndf | Select-Object -First 10)
            foreach ($f in $sample) { $lines += "  - $f" }
            if ($ndf.Count -gt 10) { $lines += "  - ... ($($ndf.Count - 10) more)" }
        }
    }

    if ($r.timings) {
        $lines += 'Timings (s):'
        try {
            $props = $r.timings.PSObject.Properties
            foreach ($p in $props) {
                $lines += ("  - {0}: {1}" -f $p.Name, $p.Value)
            }
        } catch {
            $lines += "  - $($r.timings)"
        }
    }

    return $lines
}

function Invoke-CleaningRun {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string]$ResolvedReportDir,
        [Parameter(Mandatory = $true)][string]$DictPath,
        [Parameter(Mandatory = $true)][bool]$ClearExportFolder,
        [Parameter(Mandatory = $true)][bool]$Force
    )

    $clearText = ConvertTo-BoolText -Value $ClearExportFolder
    $forceText = ConvertTo-BoolText -Value $Force
    $launchCommand = "& $(Quote-CommandArg $PythonExe) -m mko_data_cleaner.app run $(Quote-CommandArg $ResolvedReportDir) --dict-path $(Quote-CommandArg $DictPath) $(if ($ClearExportFolder) { '--clear-export-folder' } else { '--no-clear-export-folder' }) $(if ($Force) { '--force' } else { '--no-force' })"

    Write-Step 'Execution'
    Write-Info 'Resolved deterministic launch command (for transparency/copy):'
    Write-Host $launchCommand
    Write-Info 'Running cleaning via helper driver (API execution with structured result capture)...'

    $driverArgs = @(
        '--action', 'run',
        '--report-dir', $ResolvedReportDir,
        '--dict-path', $DictPath,
        '--clear-export-folder', $clearText,
        '--force', $forceText
    )
    $run = Invoke-Driver -PythonExe $PythonExe -DriverArgs $driverArgs

    if ($run.ParseError -or -not $run.Object) {
        $msg = 'Cleaning run failed: helper driver returned unparseable output.'
        if ($run.Text) { $msg += "`nHelper output:`n$($run.Text)" }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Cleaning execution'
            Message = $msg
            NextAction = 'Review the helper output above, verify the selected paths/settings, and rerun Script 2.'
            LaunchCommand = $launchCommand
        }
    }

    if ($run.ExitCode -ne 0 -or $run.Object.ok -ne $true) {
        $errType = [string]$run.Object.error_type
        $errMsg = [string]$run.Object.error_message
        $details = @()
        if ($errType) { $details += "Error type: $errType" }
        if ($errMsg) { $details += "Error: $errMsg" }
        if ($run.Object.hint) { $details += "Hint: $($run.Object.hint)" }
        if ($run.Object.file_path) { $details += "File: $($run.Object.file_path)" }
        if ($run.Object.step) { $details += "Step: $($run.Object.step)" }
        if ($run.Object.actual_columns) { $details += "Actual columns: $([string]::Join(', ', @($run.Object.actual_columns)))" }
        $msg = 'Cleaning run failed.'
        if ($details.Count -gt 0) { $msg += ' ' + ($details -join ' | ') }
        elseif ($run.Text) { $msg += " Helper output: $($run.Text)" }
        return [pscustomobject]@{
            Ok = $false
            Step = 'Cleaning execution'
            Message = $msg
            NextAction = 'Fix the reported issue (paths, dictionary/settings, or data layout) and rerun Script 2. No successful save was recorded.'
            LaunchCommand = $launchCommand
            Raw = $run.Object
        }
    }

    return [pscustomobject]@{
        Ok = $true
        LaunchCommand = $launchCommand
        Result = $run.Object.result
        PackageVersion = $run.Object.package_version
        Raw = $run.Object
    }
}

function Show-RunOutcome {
    param(
        [Parameter(Mandatory = $true)]$Run,
        [Parameter(Mandatory = $true)][string]$RawDataSelection,
        [Parameter(Mandatory = $true)][string]$RawDataMode,
        [Parameter(Mandatory = $true)][string]$DictPath
    )

    Write-Step 'Run Result'
    Write-Ok 'Cleaning completed successfully.'
    Write-Info "Raw data selection: $RawDataSelection"
    Write-Info "Raw data mode:      $RawDataMode"
    Write-Info "Dictionary path:    $DictPath"
    if ($Run.PackageVersion) {
        Write-Info "mko_data_cleaner version: $($Run.PackageVersion)"
    }
    Write-Info 'Resolved launch command:'
    Write-Host $Run.LaunchCommand

    if ($Run.Result) {
        Write-Host ''
        Write-Host 'Summary:'
        foreach ($line in (Format-ResultSummary -ResultObject $Run.Result)) {
            Write-Host "  $line"
        }
    } else {
        Write-Warn 'No structured ReportResult payload was returned by the helper driver.'
    }
}

# ----------------------------- New/Repeat Flows ----------------------------
function Invoke-NewCleaningFlow {
    param([string]$Reason)

    Write-Step 'New Cleaning'
    if ($Reason) { Write-Info $Reason }

    $python = Resolve-MkoPython -PreferredExecutable $null
    if (-not $python) {
        Set-Failure -Step 'Environment preflight' `
            -Message 'No Python interpreter/environment capable of running mko_data_cleaner was found.' `
            -NextAction 'Run Script 1 (01-init-environment.ps1) to install/update mko_data_cleaner and initialize settings, then rerun Script 2.'
        return
    }
    Write-Ok "Resolved Python interpreter: $($python.Executable)"
    if ($python.PackageVersion) { Write-Info "Detected mko_data_cleaner version: $($python.PackageVersion)" }

    $settings = Ensure-SettingsReady -PythonExe $python.Executable
    if (-not $settings.Ok) {
        Set-Failure -Step $settings.Step -Message $settings.Message -NextAction $settings.NextAction
        return
    }
    if ($settings.InitializedNow) {
        Write-Info "Settings mode: default platformdirs (actual path reported above)."
    } else {
        Write-Info "Settings mode: default platformdirs. Actual settings path: $($settings.Config.settings_path)"
    }

    $rawInput = Read-Host 'Enter raw data location (report folder containing raw_data, or folder with raw-data contents)'
    $rawInput = if ($null -eq $rawInput) { '' } else { $rawInput.Trim() }
    if ([string]::IsNullOrWhiteSpace($rawInput)) {
        Set-Failure -Step 'New cleaning input' -Message 'Raw data location was not provided.' -NextAction 'Rerun Script 2 and enter a valid raw data location.'
        return
    }

    $dictInput = Read-Host 'Enter dictionary file location (full or relative path to dictionary file)'
    $dictInput = if ($null -eq $dictInput) { '' } else { $dictInput.Trim() }
    if ([string]::IsNullOrWhiteSpace($dictInput)) {
        Set-Failure -Step 'New cleaning input' -Message 'Dictionary file location was not provided.' -NextAction 'Rerun Script 2 and enter a valid dictionary file path.'
        return
    }

    $dictResolved = Resolve-ExistingAbsolutePath -PathText $dictInput
    if (-not $dictResolved) {
        $normalizedDict = Get-NormalizedAbsolutePath -PathText $dictInput
        $shown = if ($normalizedDict) { $normalizedDict } else { $dictInput }
        Set-Failure -Step 'New cleaning input' -Message "Dictionary file was not found: $shown" -NextAction 'Provide an existing dictionary file path and rerun Script 2.'
        return
    }
    if (-not (Test-Path -LiteralPath $dictResolved -PathType Leaf)) {
        Set-Failure -Step 'New cleaning input' -Message "Dictionary path is not a file: $dictResolved" -NextAction 'Provide a path to a dictionary file (not a directory) and rerun Script 2.'
        return
    }

    $layout = Resolve-Layout -RawDataSelection $rawInput
    if (-not $layout.Ok) {
        Set-Failure -Step $layout.Step -Message $layout.Message -NextAction $layout.NextAction
        return
    }
    Write-Info $layout.Notes
    Write-Info "Raw data selection: $($layout.RawDataSelection)"
    Write-Info "Resolved report dir: $($layout.ResolvedReportDir)"
    if ($layout.StagingRawDataLink) {
        Write-Info "Staging raw_data junction: $($layout.StagingRawDataLink)"
    }

    $inputPreflight = Test-InputDataPresence -PythonExe $python.Executable -ResolvedReportDir $layout.ResolvedReportDir
    if (-not $inputPreflight.Ok) {
        Set-Failure -Step $inputPreflight.Step -Message $inputPreflight.Message -NextAction $inputPreflight.NextAction
        return
    }

    Write-Step 'Export/Interaction Policy (persisted for repeat)'
    $clearExportFolder = Read-YesNo -Prompt 'Clear export folder? (y/n)' -Default:$false
    $force = Read-YesNo -Prompt 'Force/non-interactive confirmations? (y/n)' -Default:$false
    Write-Info ("Selected flags: ClearExportFolder={0}, Force={1}" -f (ConvertTo-BoolText $clearExportFolder), (ConvertTo-BoolText $force))

    $run = Invoke-CleaningRun -PythonExe $python.Executable -ResolvedReportDir $layout.ResolvedReportDir -DictPath $dictResolved -ClearExportFolder $clearExportFolder -Force $force
    if (-not $run.Ok) {
        Set-Failure -Step $run.Step -Message $run.Message -NextAction $run.NextAction
        return
    }

    Show-RunOutcome -Run $run -RawDataSelection $layout.RawDataSelection -RawDataMode $layout.Mode -DictPath $dictResolved

    $save = Read-YesNo -Prompt 'Save this run for repeat? (y/n)' -Default:$false
    if ($save) {
        $savedAt = (Get-Date).ToString('o')
        $fields = @{
            SchemaVersion     = '1'
            SavedAt           = $savedAt
            RawDataSelection  = $layout.RawDataSelection
            RawDataMode       = $layout.Mode
            ResolvedReportDir = $layout.ResolvedReportDir
            DictPath          = $dictResolved
            ClearExportFolder = (ConvertTo-BoolText $clearExportFolder)
            Force             = (ConvertTo-BoolText $force)
            SettingsMode      = 'default_platformdirs'
            PythonExecutable  = $python.Executable
            LaunchCommand     = $run.LaunchCommand
        }
        try {
            Write-StateFileAtomic -Path $script:StateFilePath -Fields $fields
            Write-Ok "Saved repeat state: $script:StateFilePath"
        } catch {
            # Run succeeded but state save failed: report actionable error and non-zero exit.
            Set-Failure -Step 'State save' -Message "Cleaning succeeded, but saving repeat state failed: $($_.Exception.Message)" -NextAction 'Check write permissions for the examples/02-clean-data folder and rerun Script 2. The cleaning result was not marked as saved for repeat.'
            return
        }
    } else {
        Write-Info 'Run was not saved for repeat (state file unchanged).'
    }

    $script:ExitCode = $script:ExitSuccess
}

function Invoke-RepeatCleaningFlow {
    param([Parameter(Mandatory = $true)]$State)

    Write-Step 'Repeat Previous Cleaning'

    $st = $State
    $rawSelection = [string]$st.RawDataSelection
    $mode = [string]$st.RawDataMode
    $savedReportDir = [string]$st.ResolvedReportDir
    $dictPath = [string]$st.DictPath
    $clear = [bool]$st.ClearExportFolder
    $force = [bool]$st.Force
    $savedPython = [string]$st.PythonExecutable
    $launch = [string]$st.LaunchCommand

    Write-Info "Saved raw data selection: $rawSelection"
    Write-Info "Saved raw data mode:      $mode"
    Write-Info "Saved report dir:         $savedReportDir"
    Write-Info "Saved dictionary path:    $dictPath"
    Write-Info ("Saved flags:              ClearExportFolder={0}, Force={1}" -f (ConvertTo-BoolText $clear), (ConvertTo-BoolText $force))
    Write-Info "Saved Python executable:  $savedPython"
    Write-Info 'Saved launch command:'
    Write-Host $launch

    # Revalidate interpreter/environment availability before reuse (prefer saved interpreter).
    $python = Resolve-MkoPython -PreferredExecutable $savedPython
    if (-not $python) {
        Set-Failure -Step 'Repeat preflight' `
            -Message 'Saved Python interpreter/environment is unavailable or cannot run mko_data_cleaner.' `
            -NextAction 'Run Script 1 (01-init-environment.ps1) to repair/install the environment, or start a new cleaning run so a valid interpreter can be resolved and saved.'
        return
    }
    Write-Ok "Resolved Python interpreter for repeat: $($python.Executable)"
    if ($python.PackageVersion) { Write-Info "Detected mko_data_cleaner version: $($python.PackageVersion)" }

    $settings = Ensure-SettingsReady -PythonExe $python.Executable
    if (-not $settings.Ok) {
        Set-Failure -Step $settings.Step -Message $settings.Message -NextAction $settings.NextAction
        return
    }
    if ($settings.InitializedNow) {
        Write-Info 'Default settings were missing and were safely initialized during repeat preflight.'
    } else {
        Write-Info "Default settings path: $($settings.Config.settings_path)"
    }

    # Revalidate dictionary.
    $dictResolved = Resolve-ExistingAbsolutePath -PathText $dictPath
    if (-not $dictResolved -or -not (Test-Path -LiteralPath $dictResolved -PathType Leaf)) {
        Set-Failure -Step 'Repeat preflight' -Message "Saved dictionary file is missing or invalid: $dictPath" -NextAction 'Start a new cleaning run and select a valid dictionary file.'
        return
    }

    # Revalidate paths/mode and resolve report dir deterministically from saved structured fields.
    $rawResolved = Resolve-ExistingAbsolutePath -PathText $rawSelection
    if (-not $rawResolved) {
        Set-Failure -Step 'Repeat preflight' -Message "Saved raw data selection no longer exists: $rawSelection" -NextAction 'Start a new cleaning run and select an existing raw data location.'
        return
    }

    $resolvedReportDir = $null
    if ($mode -eq 'report_dir') {
        if (-not (Test-Path -LiteralPath $rawResolved -PathType Container)) {
            Set-Failure -Step 'Repeat preflight' -Message "Saved report-dir selection is not a directory: $rawResolved" -NextAction 'Start a new cleaning run and select a valid report directory.'
            return
        }
        $rawSub = Join-Path $rawResolved 'raw_data'
        if (-not (Test-Path -LiteralPath $rawSub -PathType Container)) {
            Set-Failure -Step 'Repeat preflight' -Message "Saved report-dir mode is no longer valid because 'raw_data' subfolder was not found in: $rawResolved" -NextAction 'Restore the report layout or start a new cleaning run.'
            return
        }
        $resolvedReportDir = $rawResolved
    } elseif ($mode -eq 'raw_data_contents') {
        # Recreate/refresh staging junction deterministically from saved raw selection.
        $layout = Resolve-Layout -RawDataSelection $rawResolved
        if (-not $layout.Ok) {
            Set-Failure -Step $layout.Step -Message $layout.Message -NextAction $layout.NextAction
            return
        }
        if ($layout.Mode -ne 'raw_data_contents') {
            Set-Failure -Step 'Repeat preflight' -Message "Saved raw-data-contents mode is inconsistent with the current raw data selection layout: $rawResolved" -NextAction 'Start a new cleaning run to save a new valid state.'
            return
        }
        $resolvedReportDir = $layout.ResolvedReportDir
        Write-Info "Refreshed staging layout: $($layout.Notes)"
        if ($layout.StagingRawDataLink) { Write-Info "Staging raw_data junction: $($layout.StagingRawDataLink)" }
    } else {
        Set-Failure -Step 'Repeat preflight' -Message "Unsupported saved RawDataMode: $mode" -NextAction 'Start a new cleaning run to create a valid state file.'
        return
    }

    # Optional consistency note if saved report dir differs after deterministic resolution.
    $savedReportNorm = Get-NormalizedAbsolutePath -PathText $savedReportDir
    $resolvedNorm = Get-NormalizedAbsolutePath -PathText $resolvedReportDir
    if ($savedReportNorm -and $resolvedNorm -and ($savedReportNorm.ToLowerInvariant() -ne $resolvedNorm.ToLowerInvariant())) {
        Write-Warn "Resolved report directory differs from saved value. Using deterministically resolved directory: $resolvedReportDir"
    }

    $inputPreflight = Test-InputDataPresence -PythonExe $python.Executable -ResolvedReportDir $resolvedReportDir
    if (-not $inputPreflight.Ok) {
        Set-Failure -Step $inputPreflight.Step -Message $inputPreflight.Message -NextAction $inputPreflight.NextAction
        return
    }

    $run = Invoke-CleaningRun -PythonExe $python.Executable -ResolvedReportDir $resolvedReportDir -DictPath $dictResolved -ClearExportFolder $clear -Force $force
    if (-not $run.Ok) {
        Set-Failure -Step $run.Step -Message $run.Message -NextAction $run.NextAction
        return
    }

    Show-RunOutcome -Run $run -RawDataSelection $rawResolved -RawDataMode $mode -DictPath $dictResolved
    Write-Info 'Repeat run completed using saved structured values and persisted explicit flags. State file was not rewritten.'
    $script:ExitCode = $script:ExitSuccess
}

# ----------------------------- Main ----------------------------------------
$exitCode = $script:ExitFailure
try {
    Write-Host 'mko_data_cleaner interactive cleaning (Script 2/2)'
    Write-Host "Script: $script:ScriptName"
    Write-Host "Script root: $script:ScriptRoot"
    Write-Host "State file: $script:StateFilePath"
    Write-Host "Staging root (raw-data-contents mode): $script:StagingRoot"

    if (-not (Test-Path -LiteralPath $script:HelperPath)) {
        Set-Failure -Step 'Preflight' -Message "Required helper driver is missing: $script:HelperPath" -NextAction 'Restore examples/02-clean-data/02-run-cleaning.driver.py and rerun Script 2.'
        $exitCode = $script:ExitFailure
        return
    }

    $state = Read-StateFile -Path $script:StateFilePath
    if ($state.Valid) {
        Write-Step 'Saved Run Detected'
        Write-Info "Valid saved state found: $script:StateFilePath"
        Write-Info "Saved at: $($state.Data.SavedAt)"
        Write-Host 'Choose an action:'
        Write-Host '  (1) Repeat previous cleaning'
        Write-Host '  (2) New cleaning'
        $choice = $null
        while ($true) {
            $rawChoice = Read-Host 'Enter 1 or 2'
            $rawChoice = if ($null -eq $rawChoice) { '' } else { $rawChoice.Trim() }
            if ($rawChoice -eq '1' -or $rawChoice -eq '2') { $choice = $rawChoice; break }
            Write-Host 'Please enter 1 or 2.'
        }

        if ($choice -eq '1') {
            Invoke-RepeatCleaningFlow -State $state.Data
        } else {
            Invoke-NewCleaningFlow -Reason 'Starting a new cleaning run (user selected new cleaning).'
        }
    } elseif ($state.Exists) {
        $reason = if ($state.Reason) { $state.Reason } else { 'Unknown validation error.' }
        Invoke-NewCleaningFlow -Reason "Saved state file is invalid and will not be reused: $reason"
    } else {
        Invoke-NewCleaningFlow -Reason 'No saved state file found. Starting a new cleaning run.'
    }

    if ($script:FailureStep) {
        $exitCode = $script:ExitFailure
    } else {
        $exitCode = $script:ExitSuccess
        $script:ExitCode = $script:ExitSuccess
    }
} catch {
    $errMsg = "$($_.Exception.GetType().Name): $($_.Exception.Message)"
    Write-Fail "Unexpected script error: $errMsg"
    if (-not $script:FailureStep) {
        $script:FailureStep = 'Unexpected'
        $script:FailureMessage = $errMsg
        $script:FailureNextAction = 'Review the error above, correct the issue, and rerun Script 2.'
    }
    $script:ExitCode = $script:ExitFailure
    $exitCode = $script:ExitFailure
} finally {
    if ($script:FailureStep) {
        Write-Step 'Result'
        Write-Host 'RESULT: FAILED.'
        Write-Host "Failed step: $script:FailureStep"
        Write-Host "Cause: $script:FailureMessage"
        Write-Host "Next action: $script:FailureNextAction"
        $exitCode = $script:ExitFailure
        $script:ExitCode = $script:ExitFailure
    } elseif ($exitCode -ne $script:ExitSuccess) {
        Write-Step 'Result'
        Write-Host 'RESULT: FAILED.'
        Write-Host 'Cause: The script did not complete successfully.'
        Write-Host 'Next action: Review the output above and rerun Script 2 after correcting the issue.'
    } else {
        Write-Step 'Result'
        Write-Host 'RESULT: SUCCESS.'
    }

    Write-Host ''
    Write-Host "Script finished with exit code $exitCode."
    try {
        Read-Host 'Press Enter to close'
    } catch {
        # In non-interactive hosts Read-Host may fail; ignore.
    }
}

exit $exitCode
