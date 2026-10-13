# Databricks notebook source
# MAGIC %sql
# MAGIC WITH parsed AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         CAST(try_to_timestamp(`date`, 'yyyyMMdd') AS DATE) AS date_parsed
# MAGIC     FROM workspace.kkbox_raw.user_logs_v2
# MAGIC )
# MAGIC SELECT count(*)
# MAGIC FROM parsed
# MAGIC WHERE date_parsed >= DATE '2017-01-01'
# MAGIC   AND date_parsed < DATE '2018-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from workspace.kkbox_raw.transactions

# COMMAND ----------
