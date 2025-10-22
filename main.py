#!/usr/bin/env python3


import pandas as pd
import numpy as np
import zipfile
import requests

from decimal import Decimal
from pathlib import Path
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
from nautilus_trader.model import Bar, BarType, Money, TraderId, Venue
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.currencies import USD
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from strategies.pdhl import PDHLConfig, PDHLStrategy


def download(url: str) -> None:
    filename = url.rsplit("/", maxsplit=1)[1]

    with open(filename, "wb") as f:
        f.write(requests.get(url).content)


if __name__ == "__main__":
    # Step 1: Configure and create backtest engine
    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTEST_TRADER-001"),
        logging=LoggingConfig(log_level="DEBUG"),
    )
    engine = BacktestEngine(config=engine_config)

    # Step 2: Define exchange and add it to the engine
    EXNESS = Venue("exness")
    engine.add_venue(
        venue=EXNESS,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)],
        base_currency=USD,
        default_leverage=Decimal(1),  # No leverage
    )

    # Step 3: Create instrument definition and add it to the engine
    EURUSD_INSTRUMENT = TestInstrumentProvider.default_fx_ccy(
        symbol="EUR/USD", venue=EXNESS
    )
    engine.add_instrument(EURUSD_INSTRUMENT)

    # Step 3a: Download CSV file
    download(
        "https://ticks.ex2archive.com/ticks/EURUSDc/2025/09/Exness_EURUSDc_2025_09.zip"
    )

    with zipfile.ZipFile("Exness_EURUSDc_2025_09.zip", "r") as zip_ref:
        zip_ref.extractall(Path(__file__).parent)

    # Step 4a: Load bar data from CSV file -> into pandas DataFrame
    csv_file_path = r"Exness_EURUSDc_2025_09.csv"
    df = pd.read_csv(csv_file_path, sep=",", decimal=".", header=0, index_col=False)

    # Step 4b: Restructure DataFrame into required structure
    #   -   2 columns: 'Bid', 'Ask'
    #   -   'timestamp' as index

    # Change order of columns
    df = df.reindex(columns=["Timestamp", "Bid", "Ask"])
    # Convert string timestamps into datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="ISO8601")
    # Seet column `timestamp` as index
    df = df.set_index("Timestamp")

    # DEBUG:
    print("\nData summary:")
    print(df.describe())
    print("\nMissing values:")
    print(df.isnull().sum())

    print("\n1. DATAFRAME INFO:")
    print(f"   Shape: {df.shape}")
    print(f"   Columns: {df.columns.tolist()}")
    print(f"   Index type: {type(df.index)}")
    print(f"   Index name: {df.index.name}")
    print(f"   Date range: {df.index.min()} to {df.index.max()}")
    print(f"   Total unique timestamps: {df.index.nunique()}")

    print("\n2. TIMESTAMP ANALYSIS:")
    if not isinstance(df.index, pd.DatetimeIndex):
        print("Index is not DatetimeIndex!")
        # Convert to datetime index
        df.index = pd.to_datetime(df.index)
        print("Converted to DatetimeIndex")

    print("\n3. DATA QUALITY CHECK:")
    duplicate_timestamps = df.index.duplicated().sum()
    print(f"   Duplicate timestamps: {duplicate_timestamps}")
    is_sorted = df.index.is_monotonic_increasing
    print(f"   Index is sorted: {is_sorted}")

    print("\n4. DATA VALIDITY CHECK:")
    print(f"   NaN in Bid: {df['Bid'].isna().sum()}")
    print(f"   NaN in Ask: {df['Ask'].isna().sum()}")
    print(f"   Inf in Bid: {np.isinf(df['Bid']).sum()}")
    print(f"   Inf in Ask: {np.isinf(df['Ask']).sum()}")

    print("\n5. DATA TYPES AND RANGES:")
    print(f"   Bid dtype: {df['Bid'].dtype}")
    print(f"   Ask dtype: {df['Ask'].dtype}")
    print(f"   Bid range: {df['Bid'].min():.6f} to {df['Bid'].max():.6f}")
    print(f"   Ask range: {df['Ask'].min():.6f} to {df['Ask'].max():.6f}")

    print("\n6. SAMPLE DATA (first 5 rows):")
    print(df.head())

    # Step 4c: Define type of loaded bars
    EURUSD_15MIN_BARTYPE = BarType.from_str(
        f"{EURUSD_INSTRUMENT.id}-15-MINUTE-BID-EXTERNAL",
    )

    # Step 4d: `BarDataWrangler` converts each row object of type `Bar`
    # wrangler = BarDataWrangler(EURUSD_15MIN_BARTYPE, EURUSD_INSTRUMENT)
    wrangler = QuoteTickDataWrangler(instrument=EURUSD_INSTRUMENT)
    ticks = wrangler.process_bar_data(
        bid_data=df["Bid"], ask_data=df["Ask"]
    )
    # eurusdc_15min_bars_list: list[Bar] = wrangler.process(df)

    # Step 4e: Add loaded data to the engine
    engine.add_data(ticks)

    # Step 5: Create strategy and add it to engine
    config = PDHLConfig(
        instrument_id=EURUSD_INSTRUMENT.id,
        bar_type=BarType.from_str(f"{EURUSD_INSTRUMENT.id}-15-MINUTE-MID-EXTERNAL"),
        trade_size=Decimal(10_000),
    )

    strategy = PDHLStrategy(config=config)
    engine.add_strategy(strategy=strategy)

    # Step 6: Run engine = Run backtest
    engine.run()

    # Generating reports
    engine.trader.generate_account_report(EXNESS)
    engine.trader.generate_order_fills_report()
    engine.trader.generate_positions_report()

    # Step 7: Release system resources
    engine.dispose()
