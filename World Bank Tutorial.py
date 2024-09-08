# Databricks notebook source
# MAGIC %md
# MAGIC https://datahelpdesk.worldbank.org/knowledgebase/articles/889386
# MAGIC
# MAGIC Tutorial from docs: https://wbdata.readthedocs.io/en/stable/

# COMMAND ----------

# MAGIC %pip install wbdata

# COMMAND ----------

import wbdata

# COMMAND ----------

wbdata.get_sources()

# COMMAND ----------

# Details for Source #
wbdata.get_indicators(source=1)

# COMMAND ----------

wbdata.get_countries(query='united')

# COMMAND ----------

wbdata.get_data("IC.BUS.EASE.XQ", country="USA")

# COMMAND ----------

wbdata.get_data("IC.BUS.EASE.XQ", country=["USA", "GBR"], date=("2010", "2011"))

# COMMAND ----------

wbdata.get_indicators(query="gdp per capita", source=2)

# COMMAND ----------

wbdata.get_incomelevels()

# COMMAND ----------

countries = [i['id'] for i in wbdata.get_countries(incomelevel='HIC')]

indicators = {"IC.BUS.EASE.XQ": "doing_business", "NY.GDP.PCAP.PP.KD": "gdppc"}

df = wbdata.get_dataframe(indicators, country=countries, parse_dates=True)

df.describe()

# COMMAND ----------

df.sort_index().groupby('country').last().corr()

# COMMAND ----------


