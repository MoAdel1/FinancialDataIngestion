# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Create the Database

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS forex_data CASCADE;
# MAGIC CREATE DATABASE forex_data

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Fill the tables

# COMMAND ----------

# load the data and save it into tables
for inst in inst_list:
  for frame in frames:
    data = get_forex_data(inst, frame, from_date=None)
    data.to_table(name='{}.{}_{}'.format('forex_data', inst.lower(), frame.lower()),
                 format='delta',
                 mode='error')