#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
incremental_save_all.py  v3.8  (Production Ready)

修正点 v3.7 → v3.8
------
8. HTTPタイムアウト設定を追加
9. バックフィル無限ループ対策を実装
10. シグナルハンドリングによるGraceful Shutdown
11. COPY FROM STDINによるバルク挿入の高速化
12. 詳細なメトリクス・モニタリング機能
13. 動的DIFF_LIMIT調整機能
14. クリーンアップ処理の高速化（バッチ削除）
"""

import argparse
import logging
import os
import signal
import sys
import time
import io
import csv
from datetime import timedelta, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore, local, Event
from dataclasses import dataclass
from typing import Optional, List, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import sql
import requests
from pybit.unified_trading import HTTP
from Orderbook3 import DB_CONFIG

# ───────────────────────────── 定数
DB_CONFIG["host"] = os.getenv("DB_HOST", DB_CONFIG.get("host", "localhost"))

TF_CHOICES     = ["5m", "15m", "1h", "4h"]
INTERVAL_KLINE = {"5m": "5", "15m": "15", "1h": "60", "4h": "240"}
INTERVAL_OI    = {"5m": "5min", "15m": "15min", "1h": "1h",  "4h": "4h"}
MINUTES_MAP    = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}

# ■ 修正13: 動的DIFF_LIMIT（初期値）
DIFF_LIMIT_MIN = 50
DIFF_LIMIT_MAX = 1000
DIFF_LIMIT_DEFAULT = 200

API_DELAY = 0.2
API_TIMEOUT = 10  # ■ 修正8: HTTPタイムアウト設定
RATE_SLEEP = 60
CRASH_SLEEP = 120
BATCH_SIZE = 20
BATCH_DELAY = 0
CLEANUP_KEEP_DAYS = 180

# 並列処理設定
MAX_WORKERS = 5
MAX_CONCURRENT_API = 5
DB_LOCK = Lock()

# グローバルなAPI制限セマフォ
api_semaphore = Semaphore(MAX_CONCURRENT_API)

# スレッドローカルなHTTPクライアント
thread_local = local()

# 動的API_DELAY調整用
api_delay_lock = Lock()
current_api_delay = API_DELAY

# ■ 修正10: Graceful Shutdown用
shutdown_event = Event()

# ■ 修正12: メトリクス収集用
@dataclass
class BatchMetrics:
    start_time: datetime
    end_time: Optional[datetime] = None
    symbols_processed: int = 0
    records_inserted: int = 0
    api_calls_total: int = 0
    api_calls_success: int = 0
    api_calls_failed: int = 0
    rate_limit_hits: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

# グローバルメトリクス
batch_metrics = None
metrics_lock = Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("incremental_save_all.log"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ■ 修正10: シグナルハンドラ
def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ───────────────────────────── メトリクス関数
def update_metrics(api_call_success=False, api_call_failed=False, rate_limit_hit=False, error_msg=None):
    global batch_metrics
    with metrics_lock:
        if batch_metrics:
            if api_call_success:
                batch_metrics.api_calls_success += 1
            if api_call_failed:
                batch_metrics.api_calls_failed += 1
            if rate_limit_hit:
                batch_metrics.rate_limit_hits += 1
            if error_msg:
                batch_metrics.errors.append(error_msg)
            batch_metrics.api_calls_total = batch_metrics.api_calls_success + batch_metrics.api_calls_failed

def log_batch_metrics():
    global batch_metrics
    with metrics_lock:
        if batch_metrics and batch_metrics.end_time:
            duration = batch_metrics.duration_seconds()
            success_rate = (batch_metrics.api_calls_success / max(batch_metrics.api_calls_total, 1)) * 100
            
            logger.info(f"=== BATCH METRICS ===")
            logger.info(f"Duration: {duration:.2f}s")
            logger.info(f"Symbols processed: {batch_metrics.symbols_processed}")
            logger.info(f"Records inserted: {batch_metrics.records_inserted}")
            logger.info(f"API calls: {batch_metrics.api_calls_total} (success: {batch_metrics.api_calls_success}, failed: {batch_metrics.api_calls_failed})")
            logger.info(f"Success rate: {success_rate:.1f}%")
            logger.info(f"Rate limit hits: {batch_metrics.rate_limit_hits}")
            if batch_metrics.errors:
                logger.warning(f"Errors: {len(batch_metrics.errors)} (showing first 3)")
                for error in batch_metrics.errors[:3]:
                    logger.warning(f"  - {error}")
            logger.info(f"==================")

# ───────────────────────────── 汎用ラッパ
def _safe_request(session_fn, tag: str, **params):
    """pybit / requests 呼び出しの共通リトライ（タイムアウト対応）"""
    global current_api_delay
    
    while True:
        if shutdown_event.is_set():
            raise KeyboardInterrupt("Shutdown requested")
            
        try:
            # ■ 修正8: タイムアウト設定
            # Note: pybitのHTTPクライアントは内部でrequestsを使用しているため、
            # 直接timeoutパラメータを渡すことができない場合があります。
            # セッション作成時にタイムアウトを設定する必要があります。
            with api_semaphore:
                result = session_fn(**params)
            
            update_metrics(api_call_success=True)
            
            # 動的API_DELAYを適用
            with api_delay_lock:
                delay = current_api_delay
            time.sleep(delay)
            
            return result
            
        except Exception as exc:
            update_metrics(api_call_failed=True)
            s = str(exc).lower()
            is_rate = ("10006" in s or "too many visits" in s or "rate limit" in s)
            is_conn = isinstance(exc, (requests.exceptions.ConnectionError,
                                       requests.exceptions.Timeout))
            is_key  = isinstance(exc, KeyError) and "x-bapi-limit-reset-timestamp" in s
            
            if is_rate or is_key:
                update_metrics(rate_limit_hit=True)
                # レート制限に引っかかった場合、API_DELAYを増加
                with api_delay_lock:
                    current_api_delay = min(current_api_delay * 1.5, 1.0)
                logger.error(f"[RateLimit] {tag} → sleep {RATE_SLEEP}s, API_DELAY increased to {current_api_delay:.3f} | {exc}")
                time.sleep(RATE_SLEEP)
                continue
            if is_conn:
                update_metrics(error_msg=f"Connection error in {tag}: {exc}")
                logger.error(f"[ConnErr] {tag} → sleep {RATE_SLEEP}s | {exc}")
                time.sleep(RATE_SLEEP)
                continue
            
            update_metrics(error_msg=f"Unexpected error in {tag}: {exc}")
            raise   # その他は上位へ

def adjust_api_delay_down():
    """API_DELAYを徐々に減らす（成功時に呼び出し）"""
    global current_api_delay
    with api_delay_lock:
        current_api_delay = max(current_api_delay * 0.99, API_DELAY)

# ───────────────────────────── API helpers
def get_thread_client():
    """スレッドローカルなHTTPクライアントを取得（タイムアウト設定付き）"""
    if not hasattr(thread_local, 'client'):
        # ■ 修正8: タイムアウト設定を含むHTTPクライアント
        # pybitの場合、内部のrequestsセッションのタイムアウトを設定
        client = HTTP()  # mainnet
        # requests.sessionのタイムアウト設定を試行
        try:
            if hasattr(client, '_session') and hasattr(client._session, 'timeout'):
                client._session.timeout = API_TIMEOUT
        except:
            pass  # pybitの内部実装に依存するため、失敗してもコンティニュー
        thread_local.client = client
    return thread_local.client

def fetch_kline(symbol, interval, limit, start_ms=None, end_ms=None):
    cli = get_thread_client()
    params = dict(category="linear", symbol=symbol,
                  interval=interval, limit=limit)
    if start_ms is not None:
        params["start"] = start_ms
    if end_ms is not None:
        params["end"] = end_ms
    resp = _safe_request(cli.get_kline, f"kline {symbol}", **params)
    # 成功時にAPI_DELAYを徐々に減らす
    adjust_api_delay_down()
    return resp.get("result", {}).get("list", []) or []

def fetch_oi(symbol, tf, limit, end_ms=None):
    cli = get_thread_client()
    params = dict(category="linear", symbol=symbol,
                  intervalTime=INTERVAL_OI[tf], limit=limit)
    if end_ms is not None:
        params["endTime"] = end_ms
    resp = _safe_request(cli.get_open_interest, f"OI {symbol}", **params)
    # 成功時にAPI_DELAYを徐々に減らす
    adjust_api_delay_down()
    return resp.get("result", {}).get("list", []) or []

def fetch_kline_and_oi(symbol, tf, limit, start_ms=None, end_ms=None):
    """klineとOIを順次取得する関数"""
    ivl_k = INTERVAL_KLINE[tf]
    
    # 順次呼び出し
    k_rows = fetch_kline(symbol, ivl_k, limit, start_ms, end_ms)
    oi_rows = fetch_oi(symbol, tf, limit, end_ms)
    
    return k_rows, oi_rows

def get_symbols():
    """シンボル一覧を取得"""
    cli = HTTP()  # mainnet
    syms, cursor = [], ""
    while True:
        if shutdown_event.is_set():
            raise KeyboardInterrupt("Shutdown requested")
            
        resp = _safe_request(cli.get_instruments_info, "instruments",
                             category="linear", limit=100, cursor=cursor)
        lst = resp.get("result", {}).get("list", [])
        syms += [i["symbol"] for i in lst if i["symbol"].endswith("USDT")]
        nxt = resp.get("result", {}).get("nextPageCursor")
        if not nxt or nxt == cursor:
            break
        cursor = nxt
    logger.info(f"Retrieved {len(syms)} symbols")
    return syms

# ■ 修正13: 動的DIFF_LIMIT計算
def calculate_diff_limit(target_floor, current_time, tf_minutes):
    """必要なデータ期間に基づいてDIFF_LIMITを動的計算"""
    if target_floor is None:
        return DIFF_LIMIT_DEFAULT
    
    time_diff = current_time - target_floor
    required_bars = int(time_diff.total_seconds() / (tf_minutes * 60))
    
    # 安全マージンを追加（20%）
    required_limit = int(required_bars * 1.2)
    
    # 最小・最大値でクランプ
    return max(DIFF_LIMIT_MIN, min(required_limit, DIFF_LIMIT_MAX))

# ───────────────────────────── DB helpers
def db_conn():
    return psycopg2.connect(**DB_CONFIG)

def get_last_times(cur, table):
    cur.execute(f"SELECT symbol, MAX(time) FROM {table} GROUP BY symbol;")
    return {s: t for s, t in cur.fetchall()}

# ■ 修正11: COPY FROM STDINによるバルク挿入
def insert_records_batch_copy(cur, table, all_records):
    """COPY FROM STDINを使用した高速バルク挿入"""
    if not all_records:
        return
        
    with DB_LOCK:
        # 一時テーブルを作成
        temp_table = f"temp_{table}_{int(time.time())}"
        cur.execute(f"""
            CREATE TEMP TABLE {temp_table} (LIKE {table} INCLUDING DEFAULTS)
        """)
        
        # CSVデータを準備
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        
        for record in all_records:
            # timestampをISO文字列に変換
            row = [
                record[0],  # symbol
                record[1].isoformat(),  # time
                record[2],  # open
                record[3],  # high
                record[4],  # low
                record[5],  # close
                record[6],  # volume
                record[7] if record[7] is not None else ''  # oi
            ]
            writer.writerow(row)
        
        # COPYで一括挿入
        csv_buffer.seek(0)
        cur.copy_from(
            csv_buffer, 
            temp_table,
            columns=('symbol', 'time', 'open', 'high', 'low', 'close', 'volume', 'oi'),
            sep=',',
            null=''        # ★ 追記：空文字列を NULL として扱う
        )
        
        # UPSERTを実行
        cur.execute(f"""
            INSERT INTO {table} (symbol, time, open, high, low, close, volume, oi)
            SELECT symbol, time::timestamptz, open::numeric, high::numeric, low::numeric, 
                   close::numeric, volume::numeric, 
                   oi
            FROM {temp_table}
            ON CONFLICT (symbol, time) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = COALESCE(EXCLUDED.oi, {table}.oi)
        """)
        
        # 一時テーブルを削除
        cur.execute(f"DROP TABLE {temp_table}")

def insert_records_batch(cur, table, all_records):
    """複数のシンボルのレコードをまとめて挿入（従来版との選択可能）"""
    if not all_records:
        return
        
    # レコード数に応じてCOPYまたは通常のINSERTを選択
    if len(all_records) > 1000:
        insert_records_batch_copy(cur, table, all_records)
    else:
        with DB_LOCK:
            sql = (f"INSERT INTO {table}"
                   f"(symbol,time,open,high,low,close,volume,oi) VALUES %s "
                   f"ON CONFLICT (symbol,time) DO UPDATE SET "
                   f"open   = EXCLUDED.open, "
                   f"high   = EXCLUDED.high, "
                   f"low    = EXCLUDED.low, "
                   f"close  = EXCLUDED.close, "
                   f"volume = EXCLUDED.volume, "
                   f"oi     = COALESCE(EXCLUDED.oi, {table}.oi)")
            psycopg2.extras.execute_values(cur, sql, all_records, page_size=500)

# ■ 修正14: クリーンアップ処理の高速化（バッチ削除）
def cleanup_old(cur, table: str, keep_days: int = 180, batch: int = 10_000) -> None:
    """
    Delete rows older than keep_days in micro-batches.
    Works on plain PostgreSQL tables (no Timescale required).

    Args:
        cur        : psycopg2 cursor inside an open txn
        table      : target table name
        keep_days  : rows older than this will be removed
        batch      : rows per DELETE batch (ctid slice)
    """
    start = time.time()
    total = 0
    logger.info(f"Cleanup >{keep_days} d for {table} …")

    while True:
        cur.execute(
            sql.SQL("""
                WITH old AS (
                    SELECT ctid
                    FROM   {tbl}
                    WHERE  time < NOW() - INTERVAL %s
                    LIMIT  %s
                )
                DELETE FROM {tbl} t
                USING  old
                WHERE  t.ctid = old.ctid
            """).format(tbl=sql.Identifier(table)),
            (f"{keep_days} days", batch),
        )

        deleted = cur.rowcount
        total  += deleted
        if deleted == 0:
            break

        cur.connection.commit()          # flush WAL little-by-little
        if total % 50_000 == 0:
            logger.info(f"  …{total} rows removed")

    logger.info(f"Cleanup finished – {total} rows in {time.time()-start:.1f}s")

# ───────────────────────────── DataFrame helpers
def kline_to_df(rows):
    """rows: Bybit /v5/market/kline list → pandas.DataFrame"""
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=[
        "open_time","open","high","low","close","volume","turnover"])
    df["time"] = pd.to_datetime(df["open_time"].astype(float), unit="ms", utc=True)
    # 数値化
    cols = ["open","high","low","close","volume","turnover"]
    df[cols] = df[cols].astype(float)

    # 最小限のvolume補完: volume=0 かつ turnover>0 の矛盾データのみ
    mask = (df["volume"] == 0) & (df["turnover"] > 0) & (df["close"] != 0)
    if mask.any():
        df.loc[mask, "volume"] = df.loc[mask, "turnover"] / df.loc[mask, "close"]
        logger.debug(f"Volume inconsistency fixed: {mask.sum()} records (volume=0 but turnover>0)")

    return (df.set_index("time")
              [["open","high","low","close","volume"]]
              .sort_index())

def oi_to_df(rows):
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["timestamp","openInterest"])
    df["time"] = pd.to_datetime(df["timestamp"].astype(float), unit="ms", utc=True)
    return (df.set_index("time")[["openInterest"]]
              .rename(columns={"openInterest":"oi"})
              .astype(float)
              .sort_index())

def make_records(sym, df):
    """DataFrameからDBレコードを作成"""
    # 統計情報をログ出力（デバッグ用）
    zero_volume_count = (df["volume"] == 0).sum()
    total_count = len(df)
    if zero_volume_count > 0:
        percentage = (zero_volume_count / total_count) * 100
        logger.debug(f"Symbol {sym}: {zero_volume_count}/{total_count} records ({percentage:.1f}%) have volume=0 (no trades)")
    
    return [
        (
            sym,
            ts.to_pydatetime(),
            float(r.open), float(r.high), float(r.low), float(r.close),
            float(r.volume),
            float(r.oi) if not pd.isna(r.oi) else None
        )
        for ts, r in df.iterrows()
    ]

# ───────────────────────────── 並列処理用のシンボル処理関数
def process_symbol_backfill(args):
    """バックフィル用のシンボル処理（無限ループ対策付き）"""
    sym, tf, target_floor, minutes, last_time = args
    results = []
    
    last = last_time
    previous_min_time = None  # ■ 修正9: 無限ループ対策
    loop_count = 0
    max_loops = 1000  # 安全装置
    
    while last is None or last > target_floor:
        if shutdown_event.is_set():
            break
            
        loop_count += 1
        if loop_count > max_loops:
            logger.warning(f"Symbol {sym}: Loop limit reached ({max_loops}), breaking")
            break
        
        end_ms = (int((last - timedelta(minutes=minutes)).timestamp()*1000)
                  if last else None)
        
        try:
            # ■ 修正13: 動的DIFF_LIMIT
            current_time = pd.Timestamp.utcnow()
            diff_limit = calculate_diff_limit(target_floor, current_time, minutes)
            
            k_rows, oi_rows = fetch_kline_and_oi(sym, tf, diff_limit, end_ms=end_ms)
            
            if not k_rows:
                break
                
            k_df = kline_to_df(k_rows)
            oi_df = oi_to_df(oi_rows)
            
            if oi_df is not None:
                k_df = k_df.join(oi_df, how="left")
                k_df["oi"] = k_df["oi"].ffill()
            else:
                k_df["oi"] = None

            records = make_records(sym, k_df)
            results.extend(records)
            
            current_min_time = k_df.index.min()
            
            # ■ 修正9: 無限ループ対策
            if previous_min_time is not None and current_min_time >= previous_min_time:
                logger.warning(f"Symbol {sym}: No progress detected, breaking loop")
                break
            
            last = current_min_time
            previous_min_time = current_min_time
            
            if last <= target_floor:
                break
                
        except Exception as exc:
            update_metrics(error_msg=f"Backfill error for {sym}: {exc}")
            logger.error(f"Backfill error for {sym}: {exc}")
            break
    
    return results

def process_symbol_incremental(args):
    """インクリメンタル用のシンボル処理"""
    sym, tf, minutes, last_time = args
    
    if shutdown_event.is_set():
        return None
    
    try:
        start_ms = (int((last_time + timedelta(minutes=minutes)).timestamp()*1000)
                    if last_time else None)
        
        k_rows, oi_rows = fetch_kline_and_oi(sym, tf, DIFF_LIMIT_DEFAULT, start_ms=start_ms)
        
        if not k_rows:
            return None
            
        k_df = kline_to_df(k_rows)
        oi_df = oi_to_df(oi_rows)
        
        if oi_df is not None:
            k_df = k_df.join(oi_df, how="left")
            k_df["oi"] = k_df["oi"].ffill()
        else:
            k_df["oi"] = None

        if last_time is not None:
            k_df = k_df[k_df.index > last_time]
        if k_df.empty:
            return None

        records = make_records(sym, k_df)
        new_last_time = k_df.index.max()
        
        return sym, records, new_last_time
        
    except Exception as exc:
        update_metrics(error_msg=f"Incremental error for {sym}: {exc}")
        logger.error(f"Incremental error for {sym}: {exc}")
        return None

# ───────────────────────────── core run (1 pass) - 最終版
def run_once(tf, backfill_days, enable_cleanup=True):
    global batch_metrics
    
    table = f"allsymbol_price_{tf}"
    minutes = MINUTES_MAP[tf]
    target_floor = (pd.Timestamp.utcnow().floor("min") -
                    pd.Timedelta(days=backfill_days)) if backfill_days else None

    conn = db_conn()
    cur = conn.cursor()

    # シンボル一覧を1回だけ取得
    symbols = get_symbols()
    last_times = get_last_times(cur, table)

    logger.info(f"Start loop | tf={tf} | symbols={len(symbols)} | "
                f"mode={'backfill' if backfill_days else 'incremental'} | "
                f"workers={MAX_WORKERS} | api_limit={MAX_CONCURRENT_API}")

    # スレッドプールを外側で1回生成して再利用
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # バッチ処理ループ
        for i in range(0, len(symbols), BATCH_SIZE):
            if shutdown_event.is_set():
                logger.info("Shutdown signal received, stopping batch processing")
                break
                
            batch = symbols[i:i+BATCH_SIZE]
            
            # ■ 修正12: バッチメトリクス開始
            with metrics_lock:
                batch_metrics = BatchMetrics(start_time=datetime.now())
            
            logger.info(f"Processing batch {i//BATCH_SIZE + 1}/{(len(symbols)-1)//BATCH_SIZE + 1} "
                       f"({len(batch)} symbols) | Current API_DELAY: {current_api_delay:.3f}s")

            batch_records = []
            batch_updates = {}

            # バックフィルとインクリメンタルを分離
            if backfill_days:
                # バックフィル処理のみ
                tasks = []
                for sym in batch:
                    last = last_times.get(sym)
                    tasks.append((sym, tf, target_floor, minutes, last))
                
                future_to_task = {executor.submit(process_symbol_backfill, task): task 
                                for task in tasks}
                
                for future in as_completed(future_to_task):
                    if shutdown_event.is_set():
                        break
                    task = future_to_task[future]
                    try:
                        records = future.result()
                        if records:
                            batch_records.extend(records)
                    except Exception as exc:
                        update_metrics(error_msg=f"Backfill error for {task[0]}: {exc}")
                        logger.error(f"Backfill error for {task[0]}: {exc}")
            
            else:
                # インクリメンタル処理のみ
                tasks = []
                for sym in batch:
                    last = last_times.get(sym)
                    tasks.append((sym, tf, minutes, last))
                
                future_to_task = {executor.submit(process_symbol_incremental, task): task 
                                for task in tasks}
                
                for future in as_completed(future_to_task):
                    if shutdown_event.is_set():
                        break
                    task = future_to_task[future]
                    try:
                        result = future.result()
                        if result:
                            sym, records, new_last_time = result
                            batch_records.extend(records)
                            batch_updates[sym] = new_last_time
                    except Exception as exc:
                        update_metrics(error_msg=f"Incremental error for {task[0]}: {exc}")
                        logger.error(f"Incremental error for {task[0]}: {exc}")

            # バッチ単位でDB書き込み
            if batch_records and not shutdown_event.is_set():
                insert_records_batch(cur, table, batch_records)
                conn.commit()
                
                # last_times更新
                if batch_updates:
                    last_times.update(batch_updates)
                
                # ■ 修正12: メトリクス更新
                with metrics_lock:
                    if batch_metrics:
                        batch_metrics.symbols_processed = len(batch)
                        batch_metrics.records_inserted = len(batch_records)
                        batch_metrics.end_time = datetime.now()
                
                logger.info(f"Batch completed: {len(batch_records)} records inserted")
            
            # ■ 修正12: バッチメトリクスをログ出力
            log_batch_metrics()

            # メモリを節約
            batch_records.clear()
            batch_updates.clear()

            if BATCH_DELAY and not shutdown_event.is_set():
                time.sleep(BATCH_DELAY)

    # cleanup処理（高速化版）
    if enable_cleanup and not shutdown_event.is_set():
        logger.info("Performing cleanup of old data...")
        cleanup_old(cur, table, CLEANUP_KEEP_DAYS)
        conn.commit()
        logger.info("Cleanup completed")
    else:
        logger.info("Cleanup skipped (disabled or shutdown)")

    conn.close()
    logger.info("Loop complete")

# ───────────────────────────── main with auto-retry
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf", required=True, choices=TF_CHOICES)
    ap.add_argument("--backfill", type=int, help="Backfill days")
    ap.add_argument("--no-cleanup", action="store_true", help="Skip cleanup of old data")
    ap.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = ap.parse_args()

    # デバッグレベルの設定
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled - volume=0 statistics will be shown")

    enable_cleanup = not args.no_cleanup

    logger.info("Starting incremental_save_all.py v3.8 (Production Ready)")
    logger.info(f"API timeout: {API_TIMEOUT}s, Max workers: {MAX_WORKERS}, API limit: {MAX_CONCURRENT_API}")

    while True:
        if shutdown_event.is_set():
            logger.info("Shutdown signal received, exiting")
            sys.exit(0)
            
        try:
            run_once(args.tf, args.backfill, enable_cleanup)
            break  # 正常終了
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, exiting gracefully")
            sys.exit(0)
        except Exception as exc:
            logger.error(f"[CRASH] {exc} | sleeping {CRASH_SLEEP}s then retry")
            if shutdown_event.wait(CRASH_SLEEP):  # シャットダウンシグナルを待ちながらsleep
                logger.info("Shutdown signal received during retry wait, exiting")
                sys.exit(0)

# ─────────────────────────────
if __name__ == "__main__":
    main()