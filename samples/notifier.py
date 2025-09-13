#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
notifier.py

Discord通知用モジュール。
環境変数 DISCORD_WEBHOOK_URL にWebhook URLを設定してください。
"""

import os
import requests
from dotenv import load_dotenv

# .envから環境変数をロード
load_dotenv()

# Discord Webhook URL
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def notify(message: str):
    """
    DiscordのWebhookにメッセージを送信する。
    WEBHOOK_URLが未設定なら何もしない。
    """
    if not WEBHOOK_URL:
        print("[WARN] Discord Webhook URL not set.")
        return

    payload = {"content": message}
    try:
        response = requests.post(WEBHOOK_URL, json=payload, timeout=5)
        if response.status_code != 204:
            print(f"[ERROR] Discord notify failed. Status: {response.status_code}, Body: {response.text}")
    except Exception as e:
        print(f"[EXCEPTION] Failed to send Discord notification: {e}")

if __name__ == "__main__":
    # テスト用
    notify("🔔 テスト通知：Discord送信できるか確認中！")
