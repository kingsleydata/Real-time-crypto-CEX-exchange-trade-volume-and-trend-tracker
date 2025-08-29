# Real-time-crypto-CEX-exchange-trade-volume-and-trend-tracker

## Project overview
###
**Crypto never sleeps prices jump, exchanges battle for dominance, and volatility keeps everyone on their toes. This project was built to make sense of all that chaos by turning raw trading data into insights you can actually use. Instead of staring at endless numbers, you’ll get a clear picture of market trends, which exchanges are leading the pack, and how price movements play out over time. By pulling together trade volumes, exchange dominance, volatility patterns, and long-term moving averages, the project helps you understand not just what’s happening in crypto but why it matters.**

###  
**Why is it important to analyze exchange activity and market dynamics in the crypto space?**  

Crypto markets are fast, volatile, and heavily influenced by exchange performance. Understanding how exchanges move money, which ones dominate, and how price trends evolve is essential for anyone trying to make sense of this industry — from traders to analysts to businesses.  

This project was undertaken to transform raw trading and price data into **actionable insights** that support smarter decision-making in areas such as market trend analysis, exchange performance evaluation, and trading strategy development.  

**Project structure and architecture**

***The project involves extracting publicly available trade volume data from coin api, cleaning the data and loading it into Bigquery for continuous realtime analysis.***

***The project is scheduled using google cloud colab enterprise scheduler via CRON schedule language, the ETL script runs twice daily providing users near real time access to the
latest crypto price flunctuations, trade volume and trends.***

<img width="761" height="301" alt="Crypto ETL project drawio" src="https://github.com/user-attachments/assets/6018ef1e-60f7-424b-990d-f7e578f91952" />

**What was my analysis focused on?**  

My analysis focused on both **foundational exchange activity metrics** and **advanced market trend indicators**. Core KPIs included:  

* **Total trade volume across major exchanges**  
* **Exchange dominance** (by both trade volume and number of trades)  
* **Hourly price volatility**  

To gain deeper insights, the project also explored:  

* **Long-term moving averages** (50-day, 100-day, 200-day)  
* **Volatility patterns across exchanges**  
* **Exchange comparison metrics** to see who’s leading the pack and who’s lagging  

###### These metrics were used to assess exchange performance, identify short-term volatility risks, and highlight long-term market trends. The end result is a **comprehensive snapshot of crypto market dynamics** and a tool to guide trading, analysis, and strategy.  

---

## Dashboards  

#### Overview  

![Crypto_price_tracker_CEX dominance page 1 compressed](https://github.com/user-attachments/assets/6a0b8ad0-3bfa-40a1-baa1-7cfa411a2eb1)


[Dashboard link](https://lookerstudio.google.com/u/0/reporting/063f5e7d-05ef-4158-9bbc-424027dafb03/page/eRFUF)  

---


## Core Metrics and Their Implications  

#### Trade Volume Trends  

<img width="1087" height="255" alt="trade volume ss" src="https://github.com/user-attachments/assets/d78d21da-a180-4215-ab97-a8ed58b23a8b" />


###### Tracking trade volume reveals which exchanges are consistently handling the largest flows. Spikes in volume may indicate heightened market activity, new listings, or external events driving trading surges. Sustained growth often points to increased trust and adoption.  

---

#### Exchange Dominance  

<img width="1083" height="224" alt="stacked chart ss compressed" src="https://github.com/user-attachments/assets/fe89d0d0-aedd-42ca-9f45-7f861b7b5fb7" />



###### This metric highlights which exchanges hold the lion’s share of activity. Shifts in dominance could indicate traders moving to new platforms, regulatory changes, or competitive pricing strategies.  

---

#### Hourly Price Volatility  

![Crypto_price_tracker hourly volatility](https://github.com/user-attachments/assets/7fdcccf7-3533-4cac-87d0-9db17cdd64a3)


###### Crypto is notoriously volatile, but measuring **hourly swings** provides granular insight into when the market is most unpredictable. These patterns can inform risk management strategies for active traders.  

---

#### Moving Averages (50, 100, 200-Day)  

![Crypto_price_tracker_trends_page-3 compressed](https://github.com/user-attachments/assets/4a2b97e2-b101-4305-94f8-318476511943)

****SQL query to calculate simple moving average (50, 100 and 200 day)****

```sql
WITH FILTERED_DATA AS (
  SELECT
    MAPPED_INSTRUMENT,
    BASE,
    DATE(TIMESTAMP_SECONDS(CAST(TIMESTAMP AS INT64))) AS TRADE_DATE,
    CLOSE,
    MARKET,
    ROW_NUMBER() OVER (
      PARTITION BY MAPPED_INSTRUMENT,  DATE(TIMESTAMP_SECONDS(CAST(TIMESTAMP AS INT64)))
      ORDER BY  DATE(TIMESTAMP_SECONDS(CAST(TIMESTAMP AS INT64)))
    ) AS RN
  FROM btc_historical_exchange_data.daily_crypto_price_data_silver_layer
  WHERE
    MARKET = 'binance'
    AND MAPPED_INSTRUMENT IN ('BTC-USDT', 'ETH-USDT', 'XRP-USDT', 'BNB-USDT', 'SOL-USDT')
),
UNIQUE_DATES AS (
  SELECT *
  FROM FILTERED_DATA
  WHERE RN = 1
),
MOVING_AVERAGES AS (
  SELECT
    MAPPED_INSTRUMENT,
    BASE,
    TRADE_DATE,
    CLOSE,
    AVG(CLOSE) OVER (
      PARTITION BY MAPPED_INSTRUMENT
      ORDER BY TRADE_DATE
      ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
    ) AS MOVING_AVG_100_DAY
  FROM UNIQUE_DATES
)
SELECT *
FROM MOVING_AVERAGES
ORDER BY 3 DESC
```

###### Moving averages smooth out short-term noise and highlight long-term trends. Traders often use these levels to identify support/resistance zones or confirm bullish vs bearish market momentum.  

---

## Insights  

- Exchanges don’t just differ in volume — their **trading dominance tells a story** about trust, liquidity, and user preference.  
- Volatility isn’t constant; it clusters at certain times, offering opportunities (or risks) depending on strategy.  
- Long-term moving averages help separate **real trends from short-lived hype cycles**.  
- Tracking these metrics together provides a balanced view of **both the short-term rollercoaster and the long-term direction** of crypto markets.  

---

## Future Opportunities  

- Expand coverage to include **on-chain metrics** alongside exchange data.  
- Add **predictive modeling** for price volatility and trend forecasting.  
- Build **automated alerts** (so traders don’t have to babysit the dashboard).  
- Integrate **more exchanges and tokens** for wider market coverage.  

---




