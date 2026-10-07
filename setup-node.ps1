# setup-node.ps1 - 一键配置盯盘工具运行环境（Node.js LTS）
# 双击「一键配置环境.bat」即调用本脚本：已装则跳过；缺失则自动安装（winget 优先，官方 MSI 兜底）。
$ErrorActionPreference = 'Stop'

function Test-NodeMajor {
  try { $v = (& node --version) 2>$null; if ($v -match 'v(\d+)') { return [int]$Matches[1] } } catch {}
  return 0
}

$major = Test-NodeMajor
if ($major -ge 22) {
  Write-Host "[OK] Node.js $(& node --version) 已安装，环境就绪，无需配置。" -ForegroundColor Green
  Write-Host ''
  Write-Host '接下来只需两步：' -ForegroundColor Yellow
  Write-Host '  1) 双击「启动Chrome调试.bat」(或 Edge 版)，登录你的交易平台账号，进入滚动撮合页保持开着'
  Write-Host '  2) 双击「启动监控.bat」，看板会自动打开'
  exit 0
}

if ($major -gt 0) { Write-Host "检测到 Node v$major，低于要求（v22+），将安装最新 LTS 升级。" -ForegroundColor Yellow }

# 安装需要管理员权限（winget 装 MSI / msiexec 静默安装），非管理员时自我提权重启
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
  Write-Host '安装 Node.js 需要管理员权限，即将弹出确认窗口（请点「是」）...' -ForegroundColor Yellow
  Start-Process powershell -Verb RunAs -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-Command', ("& '$PSCommandPath'; Read-Host '按回车键关闭'")
  exit 0
}

Write-Host '正在安装 Node.js LTS，可能需要几分钟，请勿关闭窗口...'
if (Get-Command winget -ErrorAction SilentlyContinue) {
  winget install OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
} else {
  Write-Host '未找到 winget，改用官方安装包（MSI）静默安装...'
  $v = (Invoke-RestMethod https://nodejs.org/dist/index.json | Where-Object lts | Select-Object -First 1).version
  $msi = Join-Path $env:TEMP "node-$v-x64.msi"
  Invoke-WebRequest "https://nodejs.org/dist/$v/node-$v-x64.msi" -OutFile $msi
  Start-Process msiexec.exe -ArgumentList '/i', $msi, '/qn' -Wait -NoNewWindow
}

# 刷新当前会话的 PATH 后再验证（安装器写入的是注册表，当前窗口不会自动生效）
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + [Environment]::GetEnvironmentVariable('Path','User')
$major = Test-NodeMajor
if ($major -ge 22) {
  Write-Host "[OK] 安装成功：Node $(& node --version)，环境就绪。" -ForegroundColor Green
  Write-Host '关掉本窗口后：双击「启动Chrome调试.bat」登录平台 → 双击「启动监控.bat」' -ForegroundColor Yellow
} else {
  Write-Host '[X] 安装后未检测到可用的 node：请关闭本窗口，重新双击一次；仍失败请到 nodejs.org 手动下载 LTS 安装。' -ForegroundColor Red
}
