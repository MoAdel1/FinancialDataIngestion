# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update the tables

# COMMAND ----------

for inst in inst_list:
  for frame in frames:
    name = '{}.{}_{}'.format('forex_data', inst.lower(), frame.lower())
    data = ks.read_table(name=name)
    data.sort_values(by=['utc_time'], inplace=True, ascending=True)
    from_date = data.utc_time.iloc[-1:].values[0]
    try:
      new_data = get_forex_data(inst, frame, from_date=from_date)
    except:
      print('fetch data failed for {}'.format(name))
      continue
    if new_data.shape[0] == 0:
      print('empty frame for {}'.format(name))
      continue
    df = data.append(new_data, ignore_index = True)
    df = df.drop_duplicates('utc_time',keep='last').reset_index(drop=True)
    df.sort_values(by=['utc_time'], inplace=True, ascending=True)
    df.to_table(name='{}.{}_{}'.format('forex_data', inst.lower(), frame.lower()),
                   format='delta',
                   mode='overwrite')