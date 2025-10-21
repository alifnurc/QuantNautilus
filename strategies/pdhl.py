from decimal import Decimal
from nautilus_trader.model import Bar, InstrumentId, Position, Quantity
from nautilus_trader.trading.strategy import Strategy, StrategyConfig


class PDHLConfig(StrategyConfig):
    instrument_id: InstrumentId
    trade_size: Decimal


class PDHLStrategy(Strategy):
    def __init__(
        self,
        config: PDHLConfig,
    ):
        super().__init__()

        self.instrument_id = config.instrument_id
        self.trade_size = Quantity.from_int(config.trade_size)

        # Trading states
        self.position: Position | None = None
        self.entry_price: float | None = None
        self.pdh: float | None = None
        self.pdl: float | None = None
        self.pdhl_is_taken: bool = False

    def on_start(self):
        self.log.info("Strategu started")

    def on_stop(self):
        self.log.info("Strategy stopped")

    def on_bar(self, bar: Bar):
        if bar.instrument_id != self.instrument_id:
            return
