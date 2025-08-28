
--Purpose of script 
===================================================================================================================================
  --This SQL script is scheduled to run twice daily and it does 4 simple things:
  --1. Selects new data from te bronze table after scheduled python script has pulled data 
  --from the API and loaded the new data into the bronze table.

  --2. Deduplicates bronze table to get rid of duplicates.

  --3. Inserts only new and updated records into the silver table.

  --4. Deduplicates the silver table.
===================================================================================================================================

  --Deduplicating bronze table
  CREATE OR REPLACE TABLE `collections-online-sales.btc_historical_exchange_data.crypto_historical_ohlcv_data_bronze_continuous_load_raw` AS   
                            SELECT * EXCEPT (RN) FROM (
                            SELECT *,
                            row_NUMBER() OVER (PARTITION BY CAST(TIMESTAMP AS NUMERIC),
                            CAST(FIRST_TRADE_TIMESTAMP AS NUMERIC),
                            CAST(LAST_TRADE_TIMESTAMP AS NUMERIC),
                            CAST(HIGH_TRADE_TIMESTAMP AS NUMERIC),
                            CAST(LOW_TRADE_TIMESTAMP AS NUMERIC), 
                            MARKET, INSTRUMENT
                            ORDER BY TIMESTAMP,
                            FIRST_TRADE_TIMESTAMP,
                            LAST_TRADE_TIMESTAMP,
                             HIGH_TRADE_TIMESTAMP,
                            LOW_TRADE_TIMESTAMP, 
                            MARKET, INSTRUMENT
                            ) AS RN
                            from `collections-online-sales.btc_historical_exchange_data.crypto_historical_ohlcv_data_bronze_continuous_load_raw`
                            ) A
                            WHERE RN = 1;


--Inserting only new records into silver layer      
            INSERT INTO btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer (
  UNIT,
  TIMESTAMP,
  TYPE,
  MARKET,
  INSTRUMENT,
  MAPPED_INSTRUMENT,
  BASE,
  QUOTE,
  BASE_ID,
  QUOTE_ID,
  OPEN,
  HIGH,
  LOW,
  CLOSE,
  FIRST_TRADE_TIMESTAMP,
  LAST_TRADE_TIMESTAMP,
  FIRST_TRADE_PRICE,
  HIGH_TRADE_PRICE,
  HIGH_TRADE_TIMESTAMP,
  LOW_TRADE_PRICE,
  LOW_TRADE_TIMESTAMP,
  LAST_TRADE_PRICE,
  TOTAL_TRADES,
  TOTAL_TRADES_BUY,
  TOTAL_TRADES_SELL,
  TOTAL_TRADES_UNKNOWN,
  VOLUME,
  QUOTE_VOLUME,
  VOLUME_BUY,
  QUOTE_VOLUME_BUY,
  VOLUME_SELL,
  QUOTE_VOLUME_SELL,
  VOLUME_UNKNOWN,
  QUOTE_VOLUME_UNKNOWN,
  COIN_PAIR
)         
  SELECT
  CON.UNIT,
  CAST(CON.TIMESTAMP AS INT64),
  CON.TYPE,
  CON.MARKET,
  CON.INSTRUMENT,
  CON.MAPPED_INSTRUMENT,
  CON.BASE,
  CON.QUOTE,
  CON.BASE_ID,
  CON.QUOTE_ID,
  CAST(CON.OPEN AS NUMERIC),
 CAST(CON.HIGH AS NUMERIC),
 CAST(CON.LOW AS NUMERIC),
  CAST(CON.CLOSE AS NUMERIC),
  CAST(CON.FIRST_TRADE_TIMESTAMP as INT64),
  CAST(CON.LAST_TRADE_TIMESTAMP AS INT64),
  CON.FIRST_TRADE_PRICE,
  CON.HIGH_TRADE_PRICE,
  CAST(CON.HIGH_TRADE_TIMESTAMP AS INT64),
  CON.LOW_TRADE_PRICE,
  CAST(CON.LOW_TRADE_TIMESTAMP AS INT64),
  CON.LAST_TRADE_PRICE,
  CAST(CON.TOTAL_TRADES AS INT64),
  CAST(CON.TOTAL_TRADES_BUY AS INT64),
  CAST(CON.TOTAL_TRADES_SELL AS INT64),
  CAST(CON.TOTAL_TRADES_UNKNOWN AS INT64),
  CON.VOLUME,
  CON.QUOTE_VOLUME,
  CON.VOLUME_BUY,
  CON.QUOTE_VOLUME_BUY,
  CON.VOLUME_SELL,
  CON.QUOTE_VOLUME_SELL,
  CON.VOLUME_UNKNOWN,
  CON.QUOTE_VOLUME_UNKNOWN,
  CON.COIN_PAIR
                  FROM `collections-online-sales.btc_historical_exchange_data.crypto_historical_ohlcv_data_bronze_continuous_load_raw` CON
                  LEFT JOIN btc_historical_exchange_data.crypto_historical_ohlcv_data_silver_layer SIL
                  ON 
                            CAST(CON.TIMESTAMP AS NUMERIC) = SIL.TIMESTAMP
                            AND CAST(CON.FIRST_TRADE_TIMESTAMP AS NUMERIC) = SIL.FIRST_TRADE_TIMESTAMP
                            AND CAST(CON.LAST_TRADE_TIMESTAMP AS NUMERIC) = SIL.LAST_TRADE_TIMESTAMP
                            AND CAST(CON.HIGH_TRADE_TIMESTAMP AS NUMERIC) = SIL.HIGH_TRADE_TIMESTAMP
                            AND CAST(CON.LOW_TRADE_TIMESTAMP AS NUMERIC) = SIL.LOW_TRADE_TIMESTAMP 
                            AND CON.MARKET = SIL.MARKET 
                            AND CON.INSTRUMENT = SIL.INSTRUMENT
                  WHERE SIL.TIMESTAMP IS NULL;


--DEDUPLICATE THE SILVER TABLE AS USUAL 

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
                            WHERE RN = 1



