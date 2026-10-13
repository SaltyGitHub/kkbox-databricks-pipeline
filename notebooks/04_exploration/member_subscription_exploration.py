# Databricks notebook source
# DBTITLE 1,Could be cool but we don't have a mapping
# MAGIC %sql
# MAGIC select registration_channel_code, count(*)
# MAGIC from workspace.kkbox_silver.members
# MAGIC group by registration_channel_code
# MAGIC order by count(*) DESC
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select registration_date, count(*)
# MAGIC from workspace.kkbox_silver.members
# MAGIC group by registration_date
# MAGIC order by registration_date desc
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select city_code, count(*)
# MAGIC from workspace.kkbox_silver.members
# MAGIC group by city_code
# MAGIC order by count(*) DESC
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select plan_list_price_ntd, count(*), subscription_plan_days
# MAGIC from workspace.kkbox_silver.subscription_transactions
# MAGIC where transaction_date >= DATE '2017-01-01' and transaction_date < DATE '2017-02-01'
# MAGIC group by plan_list_price_ntd, subscription_plan_days
# MAGIC order by plan_list_price_ntd, subscription_plan_days desc
# MAGIC limit 1000

# COMMAND ----------

# DBTITLE 1,Subscription Type
# MAGIC %sql
# MAGIC WITH january_users AS (
# MAGIC   SELECT
# MAGIC     member_id,
# MAGIC     plan_list_price_ntd,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY member_id
# MAGIC       ORDER BY transaction_date DESC
# MAGIC     ) AS rn
# MAGIC   FROM workspace.kkbox_silver.subscription_transactions
# MAGIC   WHERE transaction_date >= DATE '2017-01-01'
# MAGIC     AND transaction_date < DATE '2017-02-01'
# MAGIC )
# MAGIC SELECT
# MAGIC   plan_list_price_ntd,
# MAGIC   COUNT(*) AS user_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_users
# MAGIC FROM january_users
# MAGIC WHERE rn = 1
# MAGIC GROUP BY plan_list_price_ntd
# MAGIC ORDER BY plan_list_price_ntd DESC;
