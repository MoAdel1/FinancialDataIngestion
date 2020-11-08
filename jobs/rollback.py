# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

dbutils.widgets.text(name='Version', defaultValue='0')
dbutils.widgets.dropdown('Table', 'None', table_names() + ['None'])
#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reset table

# COMMAND ----------

version = int(dbutils.widgets.get('Version'))
table = str(dbutils.widgets.get('Table'))
database, table_name = table.split('.')[0], table.split('.')[1] 
old_version = ks.read_delta(path='dbfs:/user/hive/warehouse/{}.db/{}/'.format(database, table_name),
                            version=version)
old_version.to_table(name=table,
                     format='delta',
                     mode='overwrite')