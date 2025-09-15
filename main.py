import requests

from os import PathLike
from pathlib import Path
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.persistence.loaders import CSVTickDataLoader
from nautilus_trader.test_kit.providers import TestInstrumentProvider

CATALOG_DIR = Path(__file__).parent / "catalog"
CATALOG_DIR.mkdir(parents=True, exist_ok=True)


def load_fx_hist_data(
    filename: str,
    currency: str,
    catalog_path: PathLike[str] | str,
) -> None:
    instruments = TestInstrumentProvider.default_fx_ccy(currency)
    wrangler = QuoteTickDataWrangler(instruments)

    df = CSVTickDataLoader.load(
        filename,
        index_col=0,
        datetime_format="%Y%m%d %H%M%S%f",
    )
    df.columns = ["bid_price", "ask_price", "size"]
    print(df)

    print("Preparing ticks...")
    ticks = wrangler.process(df)

    print("Writing data to catalog...")
    catalog = ParquetDataCatalog(catalog_path)
    catalog.write_data([instruments])
    catalog.write_data(ticks)

    print("Done")


def download(url: str) -> None:
    filename = url.rsplit("/", maxsplit=1)[1]

    with open(filename, "wb") as f:
        f.write(requests.get(url).content)


def main():
    download(
        "https://raw.githubusercontent.com/nautechsystems/nautilus_data/main/raw_data/fx_hist_data/DAT_ASCII_EURUSD_T_202001.csv.gz",
    )
    load_fx_hist_data(
        filename="DAT_ASCII_EURUSD_T_202001.csv.gz",
        currency="EURUSD",
        catalog_path=CATALOG_DIR,
    )


main()
