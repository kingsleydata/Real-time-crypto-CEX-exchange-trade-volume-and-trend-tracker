import pandas as pd
import requests
from google.cloud import bigquery
from datetime import datetime, timezone

# BigQuery config
project_id = 'collections-online-sales'
dataset_id = 'btc_historical_exchange_data'
table_id = 'daily_crypto_price_data_bronze_cointinuous_load_raw'
client = bigquery.Client(project=project_id)

# API base link
api_link = 'https://data-api.coindesk.com/spot/v1/historical/days'

# Markets
non_binance_markets = ['coinbase','bybit']
binance_market = 'binance'
#kucoin_market = 'kucoin'

# Coin pairs
coin_pairs = [
    "BTC-USDT", "BTC-USD", "BTC-USDC", "BTC-DAI", "BTC-USDE", "BTC-USDS", "BTC-USD1", "BTC-SUSDS", "BTC-USDTB", "BTC-USDTO", "BTC-FDUSD", "BTC-XAUT",
 "ETH-USDT", "ETH-USD", "ETH-USDC", "ETH-DAI", "ETH-USDE", "ETH-USDS", "ETH-USD1", "ETH-SUSDS", "ETH-USDTB", "ETH-USDTO", "ETH-FDUSD", "ETH-XAUT",
 "XRP-USDT", "XRP-USD", "XRP-USDC", "XRP-DAI", "XRP-USDE", "XRP-USDS", "XRP-USD1", "XRP-SUSDS", "XRP-USDTB", "XRP-USDTO", "XRP-FDUSD", "XRP-XAUT",
"USDT-USDT", "USDT-USD", "USDT-USDC", "USDT-DAI", "USDT-USDE", "USDT-USDS", "USDT-USD1", "USDT-SUSDS", "USDT-USDTB", "USDT-USDTO", "USDT-FDUSD", "USDT-XAUT",
    "BNB-USDT", "BNB-USD", "BNB-USDC", "BNB-DAI", "BNB-USDE", "BNB-USDS", "BNB-USD1", "BNB-SUSDS", "BNB-USDTB", "BNB-USDTO", "BNB-FDUSD", "BNB-XAUT",
"SOL-USDT", "SOL-USD", "SOL-USDC", "SOL-DAI", "SOL-USDE", "SOL-USDS", "SOL-USD1", "SOL-SUSDS", "SOL-USDTB", "SOL-USDTO", "SOL-FDUSD", "SOL-XAUT",
"USDC-USDT", "USDC-USD", "USDC-USDC", "USDC-DAI", "USDC-USDE", "USDC-USDS", "USDC-USD1", "USDC-SUSDS", "USDC-USDTB", "USDC-USDTO", "USDC-FDUSD", "USDC-XAUT"
]

def fetch_market_data(market, instrument):
    try:
        response = requests.get(
            api_link,
            params={
                "market": market,
                "instrument": instrument,
                "aggregate": 1,
                "fill": "true",
                "apply_mapping": "true",
                "response_format": "JSON",
                "limit": 96
            },
            headers={"Content-type": "application/json; charset=UTF-8"}
        )

        if response.status_code != 200:
            print(f" Failed to fetch data for {market} - {instrument}: {response.status_code}")
            return None

        json_data = response.json()
        df = pd.json_normalize(json_data, record_path='Data')
        df = df.fillna(0)

        for col in df.select_dtypes(include=['int']).columns:
            df[col] = df[col].astype(float)

        df['market_name'] = market
        df['coin_pair'] = instrument
        df['inserted_date'] = datetime.now(timezone.utc)

        print(f" Success: {market} - {instrument}")
        return df

    except Exception as e:
        print(f" Error fetching or processing {market} - {instrument}: {e}")
        return None

def load_data_to_bigquery(dataframes, mode, source_name):
    if not dataframes:
        print(f" No {source_name} data to load.")
        return

    final_df = pd.concat(dataframes, ignore_index=True)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=mode,
        autodetect=True
    )

    load_job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
    load_job.result()
    print(f" Loaded {load_job.output_rows} rows from {source_name} into {table_ref}")

def extract_and_load_to_bigquery():
    # Step 1: Non-Binance exchanges (overwrite)
    non_binance_data = []
    for market in non_binance_markets:
        for pair in coin_pairs:
            df = fetch_market_data(market, pair)
            if df is not None:
                non_binance_data.append(df)
    load_data_to_bigquery(non_binance_data, bigquery.WriteDisposition.WRITE_TRUNCATE, "Non-Binance Markets")

     #Step 2: Binance (append)
    binance_data = []
    for pair in coin_pairs:
        df = fetch_market_data(binance_market, pair)
        if df is not None:
            binance_data.append(df)
    load_data_to_bigquery(binance_data, bigquery.WriteDisposition.WRITE_APPEND, "Binance")

# Run the function
extract_and_load_to_bigquery()
