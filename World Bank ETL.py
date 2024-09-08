# Databricks notebook source
# MAGIC %md
# MAGIC TODO: push to git, develop on PyCharm

# COMMAND ----------

# MAGIC %pip install wbdata
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import wbdata

# COMMAND ----------

wbdata.get_sources()

# COMMAND ----------

type(wbdata.get_indicators(source=1))

# COMMAND ----------

countries = [i['id'] for i in wbdata.get_countries(incomelevel='HIC')]

indicators = {"IC.BUS.EASE.XQ": "doing_business", "NY.GDP.PCAP.PP.KD": "gdppc"}

df = wbdata.get_dataframe(indicators, country=countries, parse_dates=True, freq='Y').reset_index()

sdf = spark.createDataFrame(df)

display(sdf)

# COMMAND ----------

df = wbdata.get_dataframe(indicators=wbdata.get_indicators(source=1), country=wbdata.get_countries(query='united'), parse_dates=True, freq='Y').reset_index()

sdf = spark.createDataFrame(df)

display(sdf)

# COMMAND ----------

print(type(wbdata.get_countries(incomelevel='HIC')))
print(wbdata.get_countries(incomelevel='HIC'))

print(type(wbdata.get_countries(incomelevel='HIC')[0]))
print(wbdata.get_countries(incomelevel='HIC')[0])
print(wbdata.get_countries(incomelevel='HIC')[1])

# COMMAND ----------


