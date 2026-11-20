$ErrorActionPreference = 'Stop'

$propertiesPath = Join-Path $PSScriptRoot 'maven-wrapper.properties'
$distributionUrl = (Get-Content $propertiesPath | Where-Object { $_ -match '^distributionUrl=' } | Select-Object -First 1) -replace '^distributionUrl=', ''
if (-not $distributionUrl) { throw "distributionUrl missing in $propertiesPath" }

$archiveName = [IO.Path]::GetFileName($distributionUrl)
$distributionName = $archiveName -replace '-bin\.zip$', ''
$cacheRoot = Join-Path $env:USERPROFILE '.m2/wrapper/dists'
$installDir = Join-Path $cacheRoot $distributionName
$mavenCmd = Join-Path $installDir 'bin/mvn.cmd'

if (-not (Test-Path $mavenCmd)) {
    New-Item -ItemType Directory -Force -Path $cacheRoot | Out-Null
    $tempDir = Join-Path ([IO.Path]::GetTempPath()) ([guid]::NewGuid().ToString())
    New-Item -ItemType Directory -Path $tempDir | Out-Null
    try {
        $archivePath = Join-Path $tempDir $archiveName
        Write-Host "Downloading Maven from $distributionUrl"
        Invoke-WebRequest -Uri $distributionUrl -OutFile $archivePath

        $checksumPath = "$archivePath.sha512"
        Invoke-WebRequest -Uri "$distributionUrl.sha512" -OutFile $checksumPath
        $expectedHash = ((Get-Content $checksumPath -Raw).Trim() -split '\s+')[0].ToLowerInvariant()
        $actualHash = (Get-FileHash $archivePath -Algorithm SHA512).Hash.ToLowerInvariant()
        if ($actualHash -ne $expectedHash) { throw 'Maven archive SHA-512 verification failed.' }

        Expand-Archive -Path $archivePath -DestinationPath $tempDir
        Move-Item -LiteralPath (Join-Path $tempDir $distributionName) -Destination $installDir
    }
    finally {
        Remove-Item -LiteralPath $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

& $mavenCmd @args
exit $LASTEXITCODE
