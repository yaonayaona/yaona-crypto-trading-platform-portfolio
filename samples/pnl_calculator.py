from dataclasses import dataclass
import logging

# 手数料率（maker: 0.025%, taker: 0.055%）
MAKER_FEE_RATE = 0.00025
TAKER_FEE_RATE = 0.00055

logger = logging.getLogger(__name__)

@dataclass
class PnLResult:
    symbol: str
    side: str
    qty: float
    entry_price: float
    exit_price: float
    raw_pnl: float
    fee: float
    net_pnl: float

class PnlCalculator:
    """
    ポジション情報と実決済価格から損益を計算するクラス。
    Usage:
        calc = PnlCalculator()
        result = calc.calculate(state, exit_price)
    """
    def calculate(self, state: dict, exit_price: float) -> PnLResult:
        """
        Args:
            state: dict with keys ['symbol', 'side', 'qty', 'entry_price']
            exit_price: 実決済価格
        Returns:
            PnLResult: raw_pnl, fee, net_pnlを含む結果オブジェクト
        """
        symbol = state.get('symbol')
        side = state.get('side')
        qty = state.get('qty', 0.0)
        entry_price = state.get('entry_price', 0.0)

        # raw PnL 計算
        if side == 'Sell':
            raw = (entry_price - exit_price) * qty
        else:
            raw = (exit_price - entry_price) * qty

        # 手数料計算
        entry_fee = entry_price * qty * MAKER_FEE_RATE
        # 利益か損失かで exit fee を分岐
        if raw >= 0:
            exit_fee = exit_price * qty * MAKER_FEE_RATE
        else:
            exit_fee = exit_price * qty * TAKER_FEE_RATE
        fee = entry_fee + exit_fee

        # net PnL
        net = raw - fee

        logger.debug(
            f"PnLCalc | {symbol} {side} qty={qty} entry={entry_price} "
            f"exit={exit_price} raw={raw} fee={fee} net={net}"
        )

        return PnLResult(
            symbol=symbol,
            side=side,
            qty=qty,
            entry_price=entry_price,
            exit_price=exit_price,
            raw_pnl=raw,
            fee=fee,
            net_pnl=net,
        )
