--Purpose of script
==========================================================================================================================
  --What this set of queries do?
  --These set of queries insert data from the bronze layer gotten from the API directly into silver layer table
  --after which the silver layer is dedplicated and validated.
==========================================================================================================================

--Checks
=========================================================================================================================
  --If table name "btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer" already exists in the database
  --this query will not run.
=========================================================================================================================



--CREATING SILVER LAYER
           CREATE TABLE IF NOT EXISTS btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer
          (UNIT STRING,
          TIMESTAMP INT64,
          TYPE STRING,
          MARKET STRING,
          INSTRUMENT STRING,
          MAPPED_INSTRUMENT STRING,
          BASE STRING,
          QUOTE STRING,
          BASE_ID FLOAT64,
          QUOTE_ID FLOAT64,
          OPEN NUMERIC(29,9),
          HIGH NUMERIC(29,9),
          LOW NUMERIC(29,9),
          CLOSE NUMERIC(29,9),
          FIRST_TRADE_TIMESTAMP INT64,
          LAST_TRADE_TIMESTAMP INT64,
          FIRST_TRADE_PRICE FLOAT64,
          HIGH_TRADE_PRICE FLOAT64,
         HIGH_TRADE_TIMESTAMP INT64,
         LOW_TRADE_PRICE FLOAT64,
         LOW_TRADE_TIMESTAMP INT64,
         LAST_TRADE_PRICE FLOAT64,
         TOTAL_TRADES INT64,
         TOTAL_TRADES_BUY INT64,
         TOTAL_TRADES_SELL INT64,
         TOTAL_TRADES_UNKNOWN INT64,
         VOLUME FLOAT64,
         QUOTE_VOLUME FLOAT64,
         VOLUME_BUY FLOAT64,
        QUOTE_VOLUME_BUY FLOAT64,
        VOLUME_SELL FLOAT64,
        QUOTE_VOLUME_SELL FLOAT64,
        VOLUME_UNKNOWN FLOAT64,
        QUOTE_VOLUME_UNKNOWN FLOAT64,
        COIN_PAIR STRING,
        INSERTED_DATE TIMESTAMP DEFAULT current_timestamp() not null);






                            
            --inserting into silver layer table  from raw bronze layer           

INSERT INTO btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer (
  UNIT, TIMESTAMP, TYPE, MARKET, INSTRUMENT, MAPPED_INSTRUMENT, BASE, QUOTE, BASE_ID, QUOTE_ID, OPEN,HIGH,
  LOW, CLOSE, FIRST_TRADE_TIMESTAMP, LAST_TRADE_TIMESTAMP,FIRST_TRADE_PRICE,HIGH_TRADE_PRICE,HIGH_TRADE_TIMESTAMP,LOW_TRADE_PRICE,
  LOW_TRADE_TIMESTAMP, LAST_TRADE_PRICE, TOTAL_TRADES, TOTAL_TRADES_BUY, TOTAL_TRADES_SELL, TOTAL_TRADES_UNKNOWN,
  VOLUME, QUOTE_VOLUME, VOLUME_BUY, QUOTE_VOLUME_BUY, VOLUME_SELL, QUOTE_VOLUME_SELL, VOLUME_UNKNOWN, QUOTE_VOLUME_UNKNOWN,
  COIN_PAIR
)
SELECT
  UNIT, CAST(TIMESTAMP AS INT64), TYPE, MARKET, INSTRUMENT, MAPPED_INSTRUMENT,BASE, QUOTE,
  BASE_ID, QUOTE_ID, CAST(OPEN AS NUMERIC), CAST(HIGH AS NUMERIC), CAST(LOW AS NUMERIC),
  CAST(CLOSE AS NUMERIC), CAST(FIRST_TRADE_TIMESTAMP as INT64), CAST(LAST_TRADE_TIMESTAMP AS INT64),
  FIRST_TRADE_PRICE, HIGH_TRADE_PRICE, CAST(HIGH_TRADE_TIMESTAMP AS INT64), LOW_TRADE_PRICE,
  CAST(LOW_TRADE_TIMESTAMP AS INT64),
  LAST_TRADE_PRICE,
  CAST(TOTAL_TRADES AS INT64),
  CAST(TOTAL_TRADES_BUY AS INT64),
  CAST(TOTAL_TRADES_SELL AS INT64),
  CAST(TOTAL_TRADES_UNKNOWN AS INT64),
  VOLUME,
  QUOTE_VOLUME,
  VOLUME_BUY,
  QUOTE_VOLUME_BUY,
  VOLUME_SELL,
  QUOTE_VOLUME_SELL,
  VOLUME_UNKNOWN,
  QUOTE_VOLUME_UNKNOWN,
  COIN_PAIR
FROM `collections-online-sales.btc_historical_exchange_data.crypto_historical_ohlcv_data_bronze_first_load_raw`;



--DEDUPLICATING SILVER LAYER
   CREATE OR REPLACE TABLE btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer AS   
                            SELECT * EXCEPT (RN) FROM (
                            SELECT *,
                            row_NUMBER() OVER (PARTITION BY TIMESTAMP,
                            FIRST_TRADE_TIMESTAMP,
                            LAST_TRADE_TIMESTAMP,
                            HIGH_TRADE_TIMESTAMP,
                            LOW_TRADE_TIMESTAMP, 
                            MARKET, INSTRUMENT
                            ORDER BY TIMESTAMP,
                            FIRST_TRADE_TIMESTAMP,
                            LAST_TRADE_TIMESTAMP,
                             HIGH_TRADE_TIMESTAMP,
                            LOW_TRADE_TIMESTAMP, 
                            MARKET, INSTRUMENT
                            ) AS RN
                            from btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer
                            ) A
                            WHERE RN = 1;





--SET NULL COLUMNS TO THE INSERT TIME WITH THIS QUERY 
                            UPDATE btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer SIL
                            SET inserted_date = current_timestamp()
                            where inserted_date is null;
