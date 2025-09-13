# trade_interface.py
from dotenv import load_dotenv
load_dotenv()

from risk import session

def place_order(symbol, side="Sell", qty=0.001, tp=None, sl=None):
    params = {
        "category": "linear",
        "symbol": symbol,
        "side": side,
        "orderType": "Market",
        "qty": qty,
        "timeInForce": "GoodTillCancel",
        "reduceOnly": False,
    }
    if tp is not None:
        params["takeProfit"] = tp
    if sl is not None:
        params["stopLoss"] = sl

    try:
        resp = session.place_order(**params)
        print(f"[✓] 注文送信: {resp}")
        return resp
    except Exception as e:
        print(f"[!] 注文エラー: {e}")
        return None

if __name__ == "__main__":
    from risk import ensure_margin_mode, ensure_leverage, DEFAULT_LEVERAGE
    symbol = "BTCUSDT"
    ensure_margin_mode(symbol)
    ensure_leverage(symbol, DEFAULT_LEVERAGE)
    result = place_order(symbol, side="Buy", qty=0.001)
    print(f"Test order result: {result}")
