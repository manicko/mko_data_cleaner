<#
.SYNOPSIS
    Bootstraps the local environment for mko_data_cleaner (Script 1 of 2).

.DESCRIPTION
    Performs a stepwise, idempotent bootstrap on Windows:
      1) Git: detect, install or upgrade via winget (with restart-required gate).
      2) Package: install/update latest default-branch HEAD of mko_data_cleaner
         from GitHub using a detected toolchain (uv -> pipx -> py/python+pip).
      3) Settings: initialize default platformdirs user settings without overwrite.

    This script does not create last_run_state.txt (Script 2 responsibility).

.EXIT CODES
    0   - Success (all required steps completed).
    301 - Restart required after first-time Git install (PATH not yet refreshed
          in this session). Close and reopen the terminal, then run Script 1 again.
    1   - Failure (actionable error reported).

.NOTES
    - Requires Windows with PowerShell 5.1+.
    - Console messages are English-only.
    - No settings overwrite is performed.
#>

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

# ----------------------------- Constants -----------------------------------
$script:ScriptName = '01-init-environment.ps1'
$script:ScriptRoot = $PSScriptRoot
$script:PackageName = 'mko_data_cleaner'
$script:PackageVcsUrl = 'git+https://github.com/manicko/mko_data_cleaner.git'
$script:PackageSourcePolicy = 'Latest default-branch HEAD from git+https://github.com/manicko/mko_data_cleaner.git (forced refresh on each run).'
$script:WingetGitId = 'Git.Git'
$script:MinPythonMajor = 3
$script:MinPythonMinor = 12
$script:ExitSuccess = 0
$script:ExitFailure = 1
$script:ExitRestartRequired = 301

# ----------------------------- State ---------------------------------------
$script:ExitCode = $script:ExitSuccess
$script:FailureStep = $null
$script:FailureMessage = $null
$script:FailureNextAction = $null
$script:RestartRequired = $false

# Summary fields
$script:GitSummary = 'Not evaluated'
$script:PackageSummary = 'Not evaluated'
$script:SettingsSummary = 'Not evaluated'

# ----------------------------- Helpers -------------------------------------
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
        [string]$Step,
        [string]$Message,
        [string]$NextAction
    )
    if (-not $script:FailureStep) {
        $script:FailureStep = $Step
        $script:FailureMessage = $Message
        $script:FailureNextAction = $NextAction
        $script:ExitCode = $script:ExitFailure
    }
}

function Set-RestartRequired {
    param(
        [string]$Step,
        [string]$Message,
        [string]$NextAction
    )
    $script:RestartRequired = $true
    $script:FailureStep = $Step
    $script:FailureMessage = $Message
    $script:FailureNextAction = $NextAction
    $script:ExitCode = $script:ExitRestartRequired
}

function Test-CommandAvailable {
    param([string]$Name)
    return [bool](Get-Command -Name $Name -ErrorAction SilentlyContinue)
}

function Update-ProcessPathFromSystem {
    # Refresh PATH in the current process from Machine/User environment so that
    # tools installed earlier in this run (e.g. Git) can be discovered.
    try {
        $machinePath = [System.Environment]::GetEnvironmentVariable('Path', 'Machine')
        $userPath = [System.Environment]::GetEnvironmentVariable('Path', 'User')
        $parts = @()
        if ($machinePath) { $parts += $machinePath }
        if ($userPath) { $parts += $userPath }
        if ($parts.Count -gt 0) {
            $env:Path = ($parts -join ';')
        }
    } catch {
        Write-Warn "Could not refresh PATH from system environment: $($_.Exception.Message)"
    }
}

function Get-GitCommandInfo {
    $cmd = Get-Command -Name 'git' -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd }
    return $null
}

function Get-GitVersionText {
    try {
        $out = & git --version 2>&1
        if ($LASTEXITCODE -eq 0 -and $out) {
            return (($out | Out-String).Trim())
        }
    } catch { }
    return $null
}

function Invoke-External {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter()][string[]]$Arguments = @()
    )
    $global:LASTEXITCODE = $null
    $output = & $FilePath @Arguments 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    return [pscustomobject]@{
        ExitCode = [int]$code
        Output   = ($output | Out-String)
    }
}

function Test-WingetExitCodeBenignForUpgrade {
    param([int]$Code)
    # Winget exit codes vary by version; treat common "no upgrade/applicable update"
    # codes as benign when git remains usable. Always rely on post-conditions.
    $benign = @(
        0,
        -1978335189, # 0x8A15002B - no applicable update found (common)
        -1978335212, # 0x8A150014 - no applicable upgrade / already installed variant
        -1978335215, # 0x8A150011 - no packages found / no applicable update variant
        -1978335135  # 0x8A150061 - package already installed (context dependent)
    )
    return ($benign -contains $Code)
}

function Invoke-PythonJson {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [Parameter(Mandatory = $true)][string]$Code
    )
    $global:LASTEXITCODE = $null
    $output = $Code | & $PythonExe - 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    $text = ($output | Out-String).Trim()
    $obj = $null
    $parseError = $null
    if ($text) {
        # Take the last non-empty line that looks like JSON in case of noise.
        $lines = @($text -split "`r?`n" | Where-Object { $_.Trim().Length -gt 0 })
        for ($i = $lines.Count - 1; $i -ge 0; $i--) {
            $candidate = $lines[$i].Trim()
            if ($candidate.StartsWith('{') -or $candidate.StartsWith('[')) {
                try {
                    $obj = $candidate | ConvertFrom-Json -ErrorAction Stop
                    $parseError = $null
                    break
                } catch {
                    $parseError = $_.Exception.Message
                }
            }
        }
        if (-not $obj) {
            try { $obj = $text | ConvertFrom-Json -ErrorAction Stop } catch { $parseError = $_.Exception.Message }
        }
    }
    return [pscustomobject]@{
        ExitCode   = [int]$code
        Output     = $text
        Object     = $obj
        ParseError = $parseError
    }
}

# ----------------------------- Step 1: Git ---------------------------------
function Invoke-GitStep {
    Write-Step 'Step 1/3: Git (detect, install/update via winget)'

    $gitBefore = Get-GitCommandInfo
    $gitBeforeVer = $null
    if ($gitBefore) {
        $gitBeforeVer = Get-GitVersionText
        Write-Info "Git detected in current session: $($gitBefore.Source)"
        if ($gitBeforeVer) { Write-Info "Git version: $gitBeforeVer" }
    } else {
        Write-Info 'Git is not available in the current session.'
    }

    $wingetAvailable = Test-CommandAvailable 'winget'
    if (-not $wingetAvailable) {
        # Winget unavailable when install/update is required.
        $msg = 'winget (Windows Package Manager / App Installer) is not available, but it is required to install or update Git.'
        $next = 'Install or update App Installer from Microsoft Store (provides winget), or install Git manually from https://git-scm.com/download/win, then reopen the terminal and rerun this script.'
        Set-Failure -Step 'Git' -Message $msg -NextAction $next
        $script:GitSummary = 'Failed: winget unavailable; cannot install/update Git.'
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    $wingetCmd = (Get-Command 'winget' -ErrorAction SilentlyContinue).Source
    Write-Info "winget detected: $wingetCmd"

    if (-not $gitBefore) {
        # --- Install path ---
        Write-Info "Attempting Git installation via winget (id: $script:WingetGitId, source: winget)..."
        $installArgs = @(
            'install',
            '--id', $script:WingetGitId,
            '-e',
            '--source', 'winget',
            '--silent',
            '--disable-interactivity',
            '--accept-package-agreements',
            '--accept-source-agreements'
        )
        $res = Invoke-External -FilePath 'winget' -Arguments $installArgs
        Write-Info "winget install exit code: $($res.ExitCode)"
        if ($res.Output) {
            Write-Host '--- winget output (install) ---'
            Write-Host $res.Output.TrimEnd()
            Write-Host '--- end winget output ---'
        }

        # Refresh PATH and re-probe before declaring restart required.
        Update-ProcessPathFromSystem
        $gitAfter = Get-GitCommandInfo
        if (-not $gitAfter) {
            $msg = 'Git was installed (or installation was attempted) but git is not available in the current session. A terminal restart is required for PATH changes to take effect.'
            $next = 'Close this terminal, open a new terminal, and run this script again: .\01-init-environment.ps1'
            Set-RestartRequired -Step 'Git' -Message $msg -NextAction $next
            $script:GitSummary = "Restart required: Git installed but not available in current session (winget exit code $($res.ExitCode)). Close/reopen terminal and rerun Script 1."
            Write-Warn $msg
            Write-Host "Next action: $next"
            Write-Host "Exit code: $script:ExitRestartRequired (restart required, not full success)."
            return
        }

        $gitAfterVer = Get-GitVersionText
        $script:GitSummary = "Installed and available: $($gitAfter.Source)"
        if ($gitAfterVer) { $script:GitSummary += "; $gitAfterVer" }
        $script:GitSummary += " (winget install exit code $($res.ExitCode))."
        Write-Ok "Git is available after install: $($gitAfter.Source)"
        if ($gitAfterVer) { Write-Ok "Git version: $gitAfterVer" }
        return
    }

    # --- Upgrade path (Git already installed) ---
    Write-Info "Attempting Git upgrade via winget (id: $script:WingetGitId, source: winget)..."
    $upgradeArgs = @(
        'upgrade',
        '--id', $script:WingetGitId,
        '-e',
        '--source', 'winget',
        '--silent',
        '--disable-interactivity',
        '--accept-package-agreements',
        '--accept-source-agreements'
    )
    $ures = Invoke-External -FilePath 'winget' -Arguments $upgradeArgs
    Write-Info "winget upgrade exit code: $($ures.ExitCode)"
    if ($ures.Output) {
        Write-Host '--- winget output (upgrade) ---'
        Write-Host $ures.Output.TrimEnd()
        Write-Host '--- end winget output ---'
    }

    # Re-probe git usability (post-condition).
    Update-ProcessPathFromSystem
    $gitNow = Get-GitCommandInfo
    $gitNowVer = $null
    if ($gitNow) { $gitNowVer = Get-GitVersionText }

    if ($gitNow) {
        $benign = Test-WingetExitCodeBenignForUpgrade -Code $ures.ExitCode
        $script:GitSummary = "Already installed and usable: $($gitNow.Source)"
        if ($gitNowVer) { $script:GitSummary += "; $gitNowVer" }
        if ($ures.ExitCode -eq 0) {
            $script:GitSummary += ' (winget upgrade completed, exit code 0).'
            Write-Ok 'Git remains usable after upgrade attempt (exit code 0).'
        } elseif ($benign) {
            $script:GitSummary += " (winget upgrade reported no applicable update/benign state, exit code $($ures.ExitCode))."
            Write-Ok "Git remains usable; winget reported benign/no-update state (exit code $($ures.ExitCode))."
        } else {
            $script:GitSummary += " (winget upgrade exit code $($ures.ExitCode); git remains usable so continuing)."
            Write-Warn "winget upgrade returned exit code $($ures.ExitCode), but git remains usable; continuing."
        }
        if ($gitNowVer) { Write-Info "Current Git version: $gitNowVer" }
        return
    }

    # Git became unusable after upgrade attempt - treat as failure.
    $msg = "Git is no longer usable after the winget upgrade attempt (winget exit code $($ures.ExitCode))."
    $next = 'Repair or reinstall Git manually from https://git-scm.com/download/win, ensure git is available in a new terminal, then rerun this script.'
    Set-Failure -Step 'Git' -Message $msg -NextAction $next
    $script:GitSummary = "Failed: git unusable after upgrade attempt (winget exit code $($ures.ExitCode))."
    Write-Fail $msg
    Write-Host "Next action: $next"
}

# ----------------------------- Step 2: Package ------------------------------
function Get-PythonCandidateCommands {
    # Returns ordered candidate launcher specs to probe for Python >= 3.12.
    # Each item: @{ Label; FilePath; PrefixArgs }
    $candidates = @()

    if (Test-CommandAvailable 'py') {
        # Prefer explicit minor versions if available, then generic -3.
        $candidates += @{ Label = 'py -3.14'; FilePath = 'py'; PrefixArgs = @('-3.14') }
        $candidates += @{ Label = 'py -3.13'; FilePath = 'py'; PrefixArgs = @('-3.13') }
        $candidates += @{ Label = 'py -3.12'; FilePath = 'py'; PrefixArgs = @('-3.12') }
        $candidates += @{ Label = 'py -3';    FilePath = 'py'; PrefixArgs = @('-3') }
    }
    foreach ($name in @('python3', 'python')) {
        if (Test-CommandAvailable $name) {
            $candidates += @{ Label = $name; FilePath = $name; PrefixArgs = @() }
        }
    }
    return $candidates
}

function Resolve-PythonCandidate {
    param([hashtable]$Candidate)

    $probeCode = @"
import json, sys
print(json.dumps({
  'executable': sys.executable,
  'version': sys.version.split()[0],
  'major': sys.version_info.major,
  'minor': sys.version_info.minor,
  'micro': sys.version_info.micro,
}))
"@
    $global:LASTEXITCODE = $null
    $output = $probeCode | & $Candidate.FilePath @($Candidate.PrefixArgs) - 2>&1
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
    if ($code -ne 0) { return $null }

    $text = ($output | Out-String).Trim()
    if (-not $text) { return $null }
    $obj = $null
    foreach ($line in @($text -split "`r?`n")) {
        $t = $line.Trim()
        if ($t.StartsWith('{')) {
            try { $obj = $t | ConvertFrom-Json -ErrorAction Stop; break } catch { }
        }
    }
    if (-not $obj) { return $null }

    try {
        $major = [int]$obj.major
        $minor = [int]$obj.minor
    } catch { return $null }

    if ($major -lt $script:MinPythonMajor) { return $null }
    if ($major -eq $script:MinPythonMajor -and $minor -lt $script:MinPythonMinor) { return $null }

    $exe = [string]$obj.executable
    if (-not $exe) { return $null }

    return [pscustomobject]@{
        Label      = $Candidate.Label
        FilePath   = $Candidate.FilePath
        PrefixArgs = $Candidate.PrefixArgs
        Executable = $exe
        Version    = [string]$obj.version
        Major      = $major
        Minor      = $minor
    }
}

function Get-ValidPythonTargets {
    $seen = @{}
    $targets = @()
    foreach ($cand in (Get-PythonCandidateCommands)) {
        $resolved = Resolve-PythonCandidate -Candidate $cand
        if (-not $resolved) { continue }
        $key = $resolved.Executable.ToLowerInvariant()
        if ($seen.ContainsKey($key)) { continue }
        $seen[$key] = $true
        $targets += $resolved
    }
    return $targets
}

function Test-PipAvailable {
    param([string]$PythonExe)
    $res = Invoke-External -FilePath $PythonExe -Arguments @('-m', 'pip', '--version')
    return ($res.ExitCode -eq 0)
}

function Get-PipxVenvPython {
    # Best-effort resolution of pipx venv Python for mko_data_cleaner.
    $venvsRoot = $null
    if (Test-CommandAvailable 'pipx') {
        try {
            $global:LASTEXITCODE = $null
            $out = & pipx environment --value PIPX_LOCAL_VENVS 2>&1
            if ($LASTEXITCODE -eq 0 -and $out) {
                $candidate = (($out | Out-String).Trim())
                if ($candidate) { $venvsRoot = $candidate }
            }
        } catch { }
        if (-not $venvsRoot) {
            try {
                $global:LASTEXITCODE = $null
                $out = & pipx environment 2>&1
                if ($LASTEXITCODE -eq 0 -and $out) {
                    $text = ($out | Out-String)
                    $m = [regex]::Match($text, 'PIPX_LOCAL_VENVS\s*=\s*(.+)')
                    if ($m.Success) { $venvsRoot = $m.Groups[1].Value.Trim() }
                }
            } catch { }
        }
    }

    $candidates = @()
    if ($venvsRoot) {
        $candidates += (Join-Path $venvsRoot "$script:PackageName\Scripts\python.exe")
        $candidates += (Join-Path $venvsRoot "$script:PackageName\bin\python")
    }
    if ($env:PIPX_HOME) {
        $candidates += (Join-Path $env:PIPX_HOME "venvs\$script:PackageName\Scripts\python.exe")
    }
    if ($env:USERPROFILE) {
        $candidates += (Join-Path $env:USERPROFILE "pipx\venvs\$script:PackageName\Scripts\python.exe")
        $candidates += (Join-Path $env:USERPROFILE ".local\pipx\venvs\$script:PackageName\Scripts\python.exe")
    }
    if ($env:LOCALAPPDATA) {
        $candidates += (Join-Path $env:LOCALAPPDATA "pipx\venvs\$script:PackageName\Scripts\python.exe")
    }

    foreach ($p in $candidates) {
        if ($p -and (Test-Path -LiteralPath $p)) {
            return (Resolve-Path -LiteralPath $p).Path
        }
    }
    return $null
}

function Get-PackageProbeInfo {
    param([string]$PythonExe)
    if (-not $PythonExe -or -not (Test-Path -LiteralPath $PythonExe)) { return $null }

    $code = @"
import json
info = {'ok': False}
try:
    import mko_data_cleaner
    info['ok'] = True
    info['version'] = getattr(mko_data_cleaner, '__version__', None)
    info['module_file'] = getattr(mko_data_cleaner, '__file__', None)
except Exception as e:
    info['import_error'] = str(e)
try:
    import importlib.metadata as md
    info['meta_version'] = md.version('mko_data_cleaner')
except Exception:
    info['meta_version'] = None
try:
    import importlib.metadata as md
    dist = md.distribution('mko_data_cleaner')
    direct = None
    try:
        direct = dist.read_text('direct_url.json')
    except Exception:
        direct = None
    info['direct_url'] = direct
except Exception:
    info['direct_url'] = None
print(json.dumps(info))
"@
    $res = Invoke-PythonJson -PythonExe $PythonExe -Code $code
    return $res
}

function Get-CommitFromDirectUrl {
    param([string]$DirectUrlJson)
    if (-not $DirectUrlJson) { return $null }
    try {
        $obj = $DirectUrlJson | ConvertFrom-Json -ErrorAction Stop
        if ($obj.vcs_info -and $obj.vcs_info.commit_id) {
            return [string]$obj.vcs_info.commit_id
        }
    } catch { }
    return $null
}

function Invoke-SettingsInit {
    param([string]$PythonExe)

    $code = @"
import json
try:
    from mko_data_cleaner.core.paths import APP_PATHS
    from mko_data_cleaner.app import initialize_settings
    settings_dir = APP_PATHS.user_settings_dir
    app_cfg = settings_dir / 'app_config.json5'
    log_cfg = settings_dir / 'log_config.json5'
    pre = {
        'dir_exists': settings_dir.exists(),
        'app_config_exists': app_cfg.exists(),
        'log_config_exists': log_cfg.exists(),
    }
    path = initialize_settings(force=False)
    post = {
        'dir_exists': settings_dir.exists(),
        'app_config_exists': app_cfg.exists(),
        'log_config_exists': log_cfg.exists(),
    }
    created_app = (not pre['app_config_exists']) and post['app_config_exists']
    created_log = (not pre['log_config_exists']) and post['log_config_exists']
    print(json.dumps({
        'ok': True,
        'path': str(path),
        'pre': pre,
        'post': post,
        'created_app_config': created_app,
        'created_log_config': created_log,
    }))
except Exception as e:
    import traceback
    print(json.dumps({'ok': False, 'error': str(e), 'trace': traceback.format_exc()}))
"@
    return Invoke-PythonJson -PythonExe $PythonExe -Code $code
}

function Invoke-PackageStep {
    Write-Step 'Step 2/3: mko_data_cleaner package (install/update to latest default-branch HEAD)'

    # Require Git usable in session for VCS install.
    $gitCmd = Get-GitCommandInfo
    if (-not $gitCmd) {
        $msg = 'Git is not usable in the current session. A usable git command is required to install/update mko_data_cleaner from the Git repository.'
        $next = 'Run Step 1 successfully first. If Git was just installed, close and reopen the terminal, rerun Script 1, and then proceed.'
        Set-Failure -Step 'Package' -Message $msg -NextAction $next
        $script:PackageSummary = 'Failed: Git not usable in current session; cannot perform VCS install/update.'
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }
    Write-Info "Git available for VCS install: $($gitCmd.Source)"

    $hasUv = Test-CommandAvailable 'uv'
    $hasPipx = Test-CommandAvailable 'pipx'
    if ($hasUv) { Write-Info "Detected installer candidate: uv ($((Get-Command uv).Source))" } else { Write-Info 'Installer candidate uv: not found.' }
    if ($hasPipx) { Write-Info "Detected installer candidate: pipx ($((Get-Command pipx).Source))" } else { Write-Info 'Installer candidate pipx: not found.' }

    $targets = @(Get-ValidPythonTargets)
    if ($targets.Count -eq 0) {
        $msg = "No Python interpreter >= $script:MinPythonMajor.$script:MinPythonMinor was found."
        $next = 'Install Python 3.12+ (for example via https://www.python.org/downloads/windows/ or `winget install Python.Python.3.12`), ensure py/python is available, then rerun this script.'
        Set-Failure -Step 'Package' -Message $msg -NextAction $next
        $script:PackageSummary = "Failed: no Python >= $script:MinPythonMajor.$script:MinPythonMinor found."
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    Write-Info 'Detected Python >= 3.12 candidate(s):'
    foreach ($t in $targets) {
        Write-Info (" - {0} (v{1}) [{2}]" -f $t.Executable, $t.Version, $t.Label)
    }

    # Select installer by priority: uv -> pipx -> pip (py/python with pip).
    $pipTarget = $null
    foreach ($t in $targets) {
        if (Test-PipAvailable -PythonExe $t.Executable) { $pipTarget = $t; break }
    }

    $selected = $null
    if ($hasUv) {
        $basePy = $targets[0]
        $selected = [pscustomobject]@{
            Installer    = 'uv'
            BasePython   = $basePy
            VerifyPython = $basePy.Executable
            Description  = "uv pip install into $($basePy.Executable) (Python $($basePy.Version))"
        }
    } elseif ($hasPipx) {
        $basePy = $targets[0]
        $selected = [pscustomobject]@{
            Installer    = 'pipx'
            BasePython   = $basePy
            VerifyPython = $null # resolved after install (best effort)
            Description  = "pipx install using $($basePy.Executable) (Python $($basePy.Version))"
        }
    } elseif ($pipTarget) {
        $selected = [pscustomobject]@{
            Installer    = 'pip'
            BasePython   = $pipTarget
            VerifyPython = $pipTarget.Executable
            Description  = "pip install into $($pipTarget.Executable) (Python $($pipTarget.Version))"
        }
    } else {
        $msg = 'No supported package toolchain is fully satisfiable. Found Python >= 3.12 but pip is unavailable for pip-based install, and neither uv nor pipx is available.'
        $next = 'Install uv (https://docs.astral.sh/uv/), or install pipx, or ensure pip is available for Python 3.12+ (`py -3 -m ensurepip --upgrade`), then rerun this script.'
        Set-Failure -Step 'Package' -Message $msg -NextAction $next
        $script:PackageSummary = 'Failed: no fully satisfiable toolchain (need uv, pipx, or Python>=3.12 with pip).'
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    Write-Info "Selected toolchain: $($selected.Installer)"
    Write-Info "Install plan: $($selected.Description)"
    Write-Info "Install source/ref policy: $script:PackageSourcePolicy"
    Write-Info "VCS URL: $script:PackageVcsUrl"

    $installResult = $null
    if ($selected.Installer -eq 'uv') {
        $installArgs = @(
            'pip', 'install',
            '--python', $selected.BasePython.Executable,
            '--upgrade',
            '--force-reinstall',
            '--no-cache-dir',
            $script:PackageVcsUrl
        )
        Write-Info "Running: uv $($installArgs -join ' ')"
        $installResult = Invoke-External -FilePath 'uv' -Arguments $installArgs
    } elseif ($selected.Installer -eq 'pipx') {
        $installArgs = @(
            'install',
            '--force',
            '--python', $selected.BasePython.Executable,
            $script:PackageVcsUrl
        )
        Write-Info "Running: pipx $($installArgs -join ' ')"
        $installResult = Invoke-External -FilePath 'pipx' -Arguments $installArgs
    } elseif ($selected.Installer -eq 'pip') {
        $installArgs = @(
            '-m', 'pip', 'install',
            '--upgrade',
            '--force-reinstall',
            '--no-cache-dir',
            $script:PackageVcsUrl
        )
        Write-Info "Running: $($selected.BasePython.Executable) $($installArgs -join ' ')"
        $installResult = Invoke-External -FilePath $selected.BasePython.Executable -Arguments $installArgs
    }

    if ($installResult -and $installResult.Output) {
        Write-Host '--- installer output ---'
        Write-Host $installResult.Output.TrimEnd()
        Write-Host '--- end installer output ---'
    }
    if ($installResult) {
        Write-Info "Installer exit code: $($installResult.ExitCode)"
    }

    $verifyPython = $selected.VerifyPython
    if ($selected.Installer -eq 'pipx') {
        $verifyPython = Get-PipxVenvPython
        if ($verifyPython) {
            Write-Info "Resolved pipx environment interpreter: $verifyPython"
        } else {
            Write-Warn 'Could not deterministically resolve the pipx venv Python path for deep probing; reporting based on installer result and policy.'
        }
    }

    $versionInfo = $null
    $commitInfo = $null
    $probeOk = $false
    if ($verifyPython) {
        $probe = Get-PackageProbeInfo -PythonExe $verifyPython
        if ($probe -and $probe.Object) {
            if ($probe.Object.ok -eq $true) {
                $probeOk = $true
                $versionInfo = $probe.Object.version
                if (-not $versionInfo -and $probe.Object.meta_version) { $versionInfo = $probe.Object.meta_version }
                $commitInfo = Get-CommitFromDirectUrl -DirectUrlJson ([string]$probe.Object.direct_url)
            } else {
                Write-Warn "Package import/probe did not succeed in target interpreter: $($probe.Object.import_error)"
            }
        } elseif ($probe -and $probe.ParseError) {
            Write-Warn "Could not parse package probe output: $($probe.ParseError)"
        }
    }

    # Determine success/failure: installer must succeed; if we could probe, probe must succeed.
    $installerOk = ($installResult -and $installResult.ExitCode -eq 0)
    $overallOk = $installerOk -and (($verifyPython -and $probeOk) -or (-not $verifyPython))

    $summary = "Toolchain: $($selected.Installer). Target/base interpreter: $($selected.BasePython.Executable) (Python $($selected.BasePython.Version))."
    $summary += " Source/ref policy: $script:PackageSourcePolicy"
    if ($installResult) { $summary += " Installer exit code: $($installResult.ExitCode)." }
    if ($verifyPython) { $summary += " Verified interpreter: $verifyPython." }
    if ($versionInfo) { $summary += " Installed version: $versionInfo." } else { $summary += ' Installed version: not discoverable.' }
    if ($commitInfo) { $summary += " Source commit (direct_url): $commitInfo." } else { $summary += ' Source commit: not discoverable from environment metadata; configured source/ref policy reported above.' }

    if ($overallOk) {
        $script:PackageSummary = "Installed/updated successfully. $summary"
        Write-Ok 'Package installation/update step completed.'
        if ($versionInfo) { Write-Ok "Installed mko_data_cleaner version: $versionInfo" }
        if ($commitInfo) { Write-Ok "Installed source commit: $commitInfo" }
    } else {
        $msg = 'Package installation/update did not complete successfully.'
        $next = 'Check the installer/probe output above. Ensure network access to GitHub, a working Git installation, and a compatible Python 3.12+ environment/toolchain, then rerun this script.'
        if (-not $installerOk) { $msg += " Installer exit code: $($installResult.ExitCode)." }
        if ($verifyPython -and -not $probeOk) { $msg += ' Post-install verification probe failed in the resolved target interpreter.' }
        Set-Failure -Step 'Package' -Message $msg -NextAction $next
        $script:PackageSummary = "Failed. $summary"
        Write-Fail $msg
        Write-Host "Next action: $next"
    }

    # Expose selected verify python for settings step (prefer verified target; fall back to base python).
    $script:SettingsPythonExe = $verifyPython
    if (-not $script:SettingsPythonExe -and $selected -and $selected.BasePython) {
        $script:SettingsPythonExe = $selected.BasePython.Executable
    }
}

# ----------------------------- Step 3: Settings ----------------------------
$script:SettingsPythonExe = $null

function Invoke-SettingsStep {
    Write-Step 'Step 3/3: Default user settings (initialize without overwrite)'

    # Use the resolved target Python from package step when available; otherwise
    # try to discover any installed mko_data_cleaner environment for idempotent reruns.
    $pythonExe = $script:SettingsPythonExe

    if (-not $pythonExe) {
        Write-Info 'No Python interpreter was resolved by the package step; attempting to locate an interpreter with mko_data_cleaner installed...'
        $targets = @(Get-ValidPythonTargets)
        foreach ($t in $targets) {
            $p = Get-PackageProbeInfo -PythonExe $t.Executable
            if ($p -and $p.Object -and $p.Object.ok -eq $true) {
                $pythonExe = $t.Executable
                Write-Info "Found mko_data_cleaner in: $pythonExe (Python $($t.Version))"
                break
            }
        }
        if (-not $pythonExe) {
            $pipxPy = Get-PipxVenvPython
            if ($pipxPy) {
                $p = Get-PackageProbeInfo -PythonExe $pipxPy
                if ($p -and $p.Object -and $p.Object.ok -eq $true) {
                    $pythonExe = $pipxPy
                    Write-Info "Found mko_data_cleaner in pipx environment: $pythonExe"
                }
            }
        }
    }

    if (-not $pythonExe) {
        $msg = 'Cannot initialize default settings because no resolved Python environment with mko_data_cleaner is available.'
        $next = 'Complete the package installation step successfully (Step 2), then rerun this script.'
        Set-Failure -Step 'Settings' -Message $msg -NextAction $next
        $script:SettingsSummary = 'Failed: no resolved Python environment with mko_data_cleaner available for settings initialization.'
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    Write-Info "Using Python interpreter for settings initialization: $pythonExe"
    Write-Info 'Calling initialize_settings(force=False) via resolved Python to target default platformdirs user settings (no overwrite).'

    $res = Invoke-SettingsInit -PythonExe $pythonExe
    if ($res.Output) {
        Write-Host '--- settings init output ---'
        Write-Host $res.Output.TrimEnd()
        Write-Host '--- end settings init output ---'
    }

    $ok = $false
    $obj = $res.Object
    if ($obj -and $obj.ok -eq $true) { $ok = $true }

    if (-not $ok) {
        $detail = $null
        if ($obj -and $obj.error) { $detail = [string]$obj.error }
        elseif ($res.ParseError) { $detail = "Unparseable output: $($res.ParseError)" }
        elseif ($res.Output) { $detail = $res.Output }
        else { $detail = 'Unknown error.' }

        $msg = "Settings initialization failed: $detail"
        $next = 'Verify that mko_data_cleaner is correctly installed in the resolved Python environment and that the user config directory is writable, then rerun this script.'
        Set-Failure -Step 'Settings' -Message $msg -NextAction $next
        $script:SettingsSummary = "Failed: settings initialization error. $detail"
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    $settingsPath = [string]$obj.path
    $pre = $obj.pre
    $post = $obj.post
    $createdApp = [bool]$obj.created_app_config
    $createdLog = [bool]$obj.created_log_config

    $appState = if ($createdApp) { 'created' } elseif ($post.app_config_exists) { 'already present' } else { 'missing (unexpected)' }
    $logState = if ($createdLog) { 'created' } elseif ($post.log_config_exists) { 'already present' } else { 'missing (unexpected)' }

    Write-Ok "Default platformdirs settings path (actual): $settingsPath"
    Write-Info ("Pre-init presence: app_config.json5={0}, log_config.json5={1}, dir={2}" -f $pre.app_config_exists, $pre.log_config_exists, $pre.dir_exists)
    Write-Info ("Post-init presence: app_config.json5={0}, log_config.json5={1}" -f $post.app_config_exists, $post.log_config_exists)
    Write-Info "app_config.json5: $appState (no overwrite)"
    Write-Info "log_config.json5: $logState (no overwrite)"

    $unexpectedMissing = (-not $post.app_config_exists) -or (-not $post.log_config_exists)
    if ($unexpectedMissing) {
        $msg = "Settings initialization completed but required files are missing in '$settingsPath' (app_config.json5 present: $($post.app_config_exists), log_config.json5 present: $($post.log_config_exists))."
        $next = 'Check permissions for the user config directory and reinstall/repair mko_data_cleaner package templates, then rerun this script.'
        Set-Failure -Step 'Settings' -Message $msg -NextAction $next
        $script:SettingsSummary = "Failed: required settings files missing after initialization at '$settingsPath'."
        Write-Fail $msg
        Write-Host "Next action: $next"
        return
    }

    $script:SettingsSummary = "Path: $settingsPath. app_config.json5: $appState. log_config.json5: $logState. No settings overwrite performed."
    Write-Ok 'Settings initialization step completed (no overwrite).'
}

# ----------------------------- Summary & Exit ------------------------------
function Write-FinalSummary {
    Write-Step 'Summary'
    Write-Host "Git:      $script:GitSummary"
    Write-Host "Package:  $script:PackageSummary"
    Write-Host "Settings: $script:SettingsSummary"
    Write-Host "Install source/ref policy: $script:PackageSourcePolicy"

    if ($script:RestartRequired) {
        Write-Host ''
        Write-Host 'RESULT: RESTART REQUIRED (not full success).'
        Write-Host "Failed/blocked step: $script:FailureStep"
        Write-Host "Reason: $script:FailureMessage"
        Write-Host "Next action: $script:FailureNextAction"
        return
    }

    if ($script:ExitCode -eq $script:ExitSuccess -and -not $script:FailureStep) {
        Write-Host ''
        Write-Host 'RESULT: SUCCESS. All required bootstrap steps completed.'
        Write-Host 'You can now run Script 2 (02-run-cleaning.ps1) to perform data cleaning.'
        return
    }

    Write-Host ''
    Write-Host 'RESULT: FAILED.'
    if ($script:FailureStep) { Write-Host "Failed step: $script:FailureStep" }
    if ($script:FailureMessage) { Write-Host "Cause: $script:FailureMessage" }
    if ($script:FailureNextAction) { Write-Host "Next action: $script:FailureNextAction" }
}

# ----------------------------- Main ----------------------------------------
$exitCode = $script:ExitFailure
try {
    Write-Host 'mko_data_cleaner environment bootstrap (Script 1/2)'
    Write-Host "Script: $script:ScriptName"
    Write-Host "Script root: $script:ScriptRoot"

    # Step 1: Git
    Invoke-GitStep
    if ($script:RestartRequired) {
        $exitCode = $script:ExitRestartRequired
        # Stop further steps; do not claim full success.
        return
    }
    if ($script:FailureStep) {
        $exitCode = $script:ExitFailure
        # Stop further steps on failure.
        return
    }

    # Step 2: Package
    Invoke-PackageStep
    if ($script:FailureStep) {
        $exitCode = $script:ExitFailure
        # Settings depend on successful package step/probe; stop.
        return
    }

    # Step 3: Settings
    Invoke-SettingsStep
    if ($script:FailureStep) {
        $exitCode = $script:ExitFailure
        return
    }

    # All steps completed successfully.
    $script:ExitCode = $script:ExitSuccess
    $exitCode = $script:ExitSuccess
} catch {
    $errMsg = "$($_.Exception.GetType().Name): $($_.Exception.Message)"
    Write-Fail "Unexpected script error: $errMsg"
    if (-not $script:FailureStep) {
        $script:FailureStep = 'Unexpected'
        $script:FailureMessage = $errMsg
        $script:FailureNextAction = 'Review the error above, correct the issue, and rerun this script.'
    }
    $script:ExitCode = $script:ExitFailure
    $exitCode = $script:ExitFailure
} finally {
    try {
        Write-FinalSummary
    } catch {
        Write-Host "Summary generation error: $($_.Exception.Message)"
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
