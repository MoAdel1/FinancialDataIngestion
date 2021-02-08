# DataIngestion
This repo contains Databircks based integration jobs that fetches and store Forex data in a structured way from the Oanda V20 open source API.

Timeframes stored:
- M1 
- M5 
- M10 
- M15 
- M30

Pairs stored:
- EUR_USD 
- USD_JPY 
- GBP_USD
- USD_CAD 
- USD_CHF
- AUD_USD
- NZD_USD

The repo contains three main jobs:
- **seed:** first time dataframes creation where we fetch 10 years of data for the different timeframes and pairs starting from current date and moving backwards.
- **update:** a periodic weekly job scheduled in Databricks which fetches the weeks data and append it to the current existing dataframes.
- **rollback:** a job to revert back the data version in case of corupted entries. It should be noted that reverting is possible because we use a versioned delta table for storing of the data.

# References
- [Databricks](https://docs.databricks.com/)
- [OandaV20](https://developer.oanda.com/rest-live-v20/introduction/)






