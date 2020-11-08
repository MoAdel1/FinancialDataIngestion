# Databricks notebook source
# MAGIC %pip install oandapyV20==0.6.3

# COMMAND ----------

# code imports
import os
import datetime
import oandapyV20
import numpy as np
import pandas as pd
import databricks.koalas as ks
from oandapyV20.contrib.factories import InstrumentsCandlesFactory

# COMMAND ----------

# define instruments and frames

inst_list = ['EUR_USD', 'USD_JPY', 'GBP_USD', 'USD_CAD', 'USD_CHF', 'AUD_USD', 'NZD_USD']
frames = ['M1', 'M5', 'M10', 'M15', 'M30']

# COMMAND ----------

# functions

def table_names():
  names = list()
  for inst in inst_list:
    for frame in frames:
      name = '{}.{}_{}'.format('forex_data', inst.lower(), frame.lower())
      names.append(name)
  return names
  
def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '.' + k, v) for k, v in flatten_dict(value).items()]
        else:
            return [(key, value)]
    items = [item for k, v in d.items() for item in expand(k, v)]
    return dict(items)

def get_forex_data(inst, frame, from_date=None, days=3650, include_first=True):
    client = oandapyV20.API(access_token=os.environ.get('OANDA_TOKEN'))
    # time period used
    end_date = datetime.datetime.utcnow().replace(microsecond=0)
    start_date = (end_date - datetime.timedelta(days=days)).isoformat('T')+'Z' if \
                 from_date == None else pd.Timestamp(from_date).to_pydatetime().isoformat('T')+'Z'
    end_date = end_date.isoformat('T')+'Z'
    # define list to hold the candles data
    # define request parameters
    params = {'granularity':frame,
              'price':'AB',
              'from':start_date,
              'to':end_date}
    # fetch data from Oanda servers by a client request
    data = list()
    for r in InstrumentsCandlesFactory(instrument=inst,params=params):
        client.request(r)
        data.append(r.response['candles'])
    flat_list = [flatten_dict(item) for sublist in data for item in sublist]
    # remove duplicated records from the dataFrame
    candles_data = ks.DataFrame(data=flat_list)
    candles_data = candles_data.iloc[1:] if not include_first else candles_data
    candles_data = candles_data[candles_data['complete']==True]
    candles_data = candles_data.drop_duplicates('time',keep='last').reset_index(drop=True)
    candles_data = candles_data.astype({'ask.o': np.float, 
                                        'ask.h': np.float, 
                                        'ask.l': np.float, 
                                        'ask.c': np.float,
                                        'bid.o': np.float, 
                                        'bid.h': np.float, 
                                        'bid.l': np.float, 
                                        'bid.c': np.float,
                                        'volume': np.float})
    candles_data.rename(columns={'ask.o':'ask_open', 'ask.h':'ask_high', 'ask.l':'ask_low', 'ask.c':'ask_close',
                                 'bid.o':'bid_open', 'bid.h':'bid_high', 'bid.l':'bid_low', 'bid.c':'bid_close',
                                 'time':'utc_time'}, inplace=True)
    candles_data['utc_time'] = candles_data['utc_time'].apply(lambda x: datetime.datetime.strptime(x.split('.')[0]+'Z', 
                                                                                                   '%Y-%m-%dT%H:%M:%SZ'))
    candles_data = candles_data[['utc_time', 
                                 'ask_open', 'ask_high', 'ask_low', 'ask_close',
                                 'bid_open', 'bid_high', 'bid_low', 'bid_close',
                                 'volume']]
    candles_data.sort_values(by=['utc_time'], inplace=True, ascending=True)
    return candles_data