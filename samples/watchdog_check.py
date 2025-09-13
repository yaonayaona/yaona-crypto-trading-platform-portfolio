#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
watchdog_check.py ― 監視スクリプト（cron 前提版）

・cron（例: */10 * * * *）で 10 分ごとに 1 回実行し、直ちに終了する。
・以下をチェックして Discord Webhook へレポート送信。
  1) position_monitor.py の稼働有無・多重起動
  2) ws_listener.py の稼働有無
  3) 指定ログの更新有無
  4) CPU / RAM 使用率
  5) 直近エラー行（Traceback / [ERROR]）の抜粋
"""

import os
import sys
import time
import psutil
import requests
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import List

from dotenv import load_dotenv

# ───────── 設定 ─────────
PROCESS_NAME        = "position_monitor.py"
WS_PROCESS_NAME     = "ws_listener.py"

# ログは固定パス（cronのリダイレクト先と一致させる）
LOG_PATH            = "/root/crypto_yaona3/logs/yaona_short.log"

# 表示用しきい値
CHECK_INTERVAL_MIN  = 10
CPU_THRESHOLD       = 80.0  # %
RAM_THRESHOLD       = 80.0  # %
TAIL_LINES          = 200   # 最新ログからこの行数だけ見てエラー抽出

# Discord Webhook
load_dotenv()
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_kadoustatus") or os.getenv("WATCHDOG_WEBHOOK_URL")
if not WEBHOOK_URL:
    print("ERROR: DISCORD_WEBHOOK_URL_kadoustatus (or WATCHDOG_WEBHOOK_URL) not set.", file=sys.stderr)
    sys.exit(1)

# ───────── ユーティリティ ─────────
def send_discord_embed(title: str, color: int, fields: List[dict], footer: str | None = None) -> None:
    embed = {
        "title": title,
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if footer:
        embed["footer"] = {"text": footer}
    try:
        resp = requests.post(WEBHOOK_URL, json={"embeds": [embed]}, timeout=15)
        if resp.status_code >= 300:
            print(f"[watchdog] webhook post failed: {resp.status_code} {resp.text}", file=sys.stderr)
    except Exception as e:
        print(f"[watchdog] webhook post error: {e}", file=sys.stderr)

def format_jst(dt_utc: datetime) -> str:
    jst = timezone(timedelta(hours=9))
    return dt_utc.astimezone(jst).strftime("%Y年%m月%d日 %H:%M:%S %Z")

def get_uptime_str(start_epoch: float) -> str:
    sec = time.time() - start_epoch
    days = int(sec // 86400); sec %= 86400
    hours = int(sec // 3600); sec %= 3600
    mins = int(sec // 60)
    return f"{days}日 {hours}時間 {mins}分"

def is_process_running(name: str) -> bool:
    for p in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            if any(name in s for s in (p.info["cmdline"] or [])):
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return False

def list_processes(name: str) -> List[psutil.Process]:
    found: List[psutil.Process] = []
    for p in psutil.process_iter(attrs=["pid", "cmdline"]):
        try:
            if any(name in s for s in (p.info["cmdline"] or [])):
                found.append(p)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return found

def get_cpu_usage() -> float:
    try:
        return psutil.cpu_percent(interval=0.5)
    except Exception:
        return 0.0

def get_process_ram_usage(name: str) -> float:
    total = psutil.virtual_memory().total
    rss_sum = 0
    for p in list_processes(name):
        try:
            rss_sum += p.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    if total == 0:
        return 0.0
    return (rss_sum / total) * 100.0

def get_memory_info() -> str:
    try:
        vm = psutil.virtual_memory()
        used_gb = (vm.total - vm.available) / (1024**3)
        total_gb = vm.total / (1024**3)
        percent = (used_gb / total_gb) * 100.0
        return f"{used_gb:.1f}GB / {total_gb:.1f}GB ({percent:.1f}%)"
    except:
        return "取得失敗"

def get_disk_info() -> str:
    try:
        disk = psutil.disk_usage("/")
        used_gb = disk.used / (1024**3)
        total_gb = disk.total / (1024**3)
        percent = (disk.used / disk.total) * 100
        return f"{used_gb:.1f}GB / {total_gb:.1f}GB ({percent:.1f}%)"
    except:
        return "取得失敗"

def log_updated_recently(path: str, minutes: int) -> bool:
    return os.path.isfile(path) and (time.time() - os.path.getmtime(path) <= minutes * 60)

def extract_errors(path: str, tail_lines: int) -> List[str]:
    if not os.path.isfile(path):
        return ["ログファイルが存在しません"]
    dq: deque[str] = deque(maxlen=tail_lines)
    with open(path, "r", encoding="utf-8", errors="ignore") as fp:
        for line in fp:
            dq.append(line.rstrip("\n"))
    return [ln for ln in dq if "[ERROR]" in ln or "Traceback" in ln]

# ───────── 本体（単発実行）─────────
def main() -> None:
    now_jst   = format_jst(datetime.now(timezone.utc))
    boot_time = psutil.boot_time()  # 参考として OS 稼働時間を表示（Bot 稼働時間ではない）

    # プロセス監視
    pm_list      = list_processes(PROCESS_NAME)
    pm_running   = len(pm_list) > 0
    pm_duplicate = len(pm_list) > 1
    ws_running   = is_process_running(WS_PROCESS_NAME)

    # ログ監視
    log_ok      = log_updated_recently(LOG_PATH, CHECK_INTERVAL_MIN)
    error_lines = extract_errors(LOG_PATH, TAIL_LINES)
    err_count   = len(error_lines)

    # リソース
    cpu_pct = get_cpu_usage()
    ram_pct = get_process_ram_usage("python")  # 粗い合計
    cpu_ok  = cpu_pct < CPU_THRESHOLD
    ram_ok  = ram_pct < RAM_THRESHOLD

    # ステータス色
    color = 0xF1C40F  # yellow
    status_emoji = "🟡"
    if err_count >= 1 or not log_ok:
        color = 0xE67E22  # orange
        status_emoji = "🟠"
    if not pm_running or not ws_running:
        color = 0xE74C3C  # red
        status_emoji = "🔴"
    if pm_running and ws_running and log_ok and err_count == 0 and cpu_ok and ram_ok:
        color = 0x2ECC71  # green
        status_emoji = "🟢"

    # 表示フィールド
    fields = [
        {
            "name": "🕒 ステータス・時刻",
            "value": f"実行: {now_jst}\n稼働時間(OS): {get_uptime_str(boot_time)}",
            "inline": False
        },
        {
            "name": "🧰 プロセス監視",
            "value": (
                f"{'✅' if pm_running else '❌'} position_monitor"
                + (f"（重複 {len(pm_list)} 個）" if pm_duplicate else "")
                + f"\n{'✅' if ws_running else '❌'} ws_listener"
                + f"\n{'✅' if log_ok else '❌'} botログ更新"
            ),
            "inline": False
        },
        {
            "name": "💻 リソース使用状況",
            "value": (
                f"{'✅' if cpu_ok else '⚠️'} CPU: {cpu_pct:.1f}%\n"
                f"{'✅' if ram_ok else '⚠️'} プロセスRAM: {ram_pct:.1f}%\n"
                f"💾 システム: {get_memory_info()}"
            ),
            "inline": False
        },
        {
            "name": "💿 ストレージ・ログ",
            "value": f"📁 ディスク: {get_disk_info()}\n⚠️ エラー: {err_count}件",
            "inline": False
        },
        {
            "name": "🧾 最新エラー詳細",
            "value": "\n".join(error_lines[-5:] if err_count > 0 else ["（直近エラーなし）"]),
            "inline": False
        },
    ]

    # 推奨アクション
    actions = []
    if not pm_running:
        actions.append("・position_monitor の起動確認")
    if not ws_running:
        actions.append("・ws_listener の起動確認")
    if not log_ok:
        actions.append(f"・ログ更新停止の確認（{LOG_PATH}）")
    if err_count > 0:
        actions.append("・エラー詳細の調査（ログ全文）")
    if actions:
        fields.append({"name": "🔧 推奨アクション", "value": "\n".join(actions), "inline": False})

    # 送信
    title  = f"🔍 Yaona Bot ヘルスチェック {status_emoji}"
    footer = f"次回チェック: cron（*/{CHECK_INTERVAL_MIN}） | 実行方式: cron"
    send_discord_embed(title, color, fields, footer)

if __name__ == "__main__":
    main()
