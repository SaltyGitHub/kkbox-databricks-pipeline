# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.is_churn,
# MAGIC     ROUND(AVG(t.amount_paid_ntd), 2) AS avg_amount_paid
# MAGIC FROM workspace.kkbox_silver.subscription_transactions t
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON t.member_id = c.msno
# MAGIC GROUP BY c.is_churn;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.plan_list_price_ntd,
# MAGIC     COUNT(*) AS transactions,
# MAGIC     ROUND(AVG(c.is_churn) * 100, 1) AS churn_rate
# MAGIC FROM workspace.kkbox_silver.subscription_transactions t
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON t.member_id = c.msno
# MAGIC GROUP BY t.plan_list_price_ntd
# MAGIC ORDER BY t.plan_list_price_ntd;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN t.amount_paid_ntd < t.plan_list_price_ntd THEN 'Discounted'
# MAGIC         ELSE 'Full Price'
# MAGIC     END AS price_type,
# MAGIC     COUNT(*) AS transactions,
# MAGIC     ROUND(AVG(c.is_churn) * 100, 1) AS churn_rate
# MAGIC FROM workspace.kkbox_silver.subscription_transactions t
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON t.member_id = c.msno
# MAGIC GROUP BY price_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('month', transaction_date) AS month,
# MAGIC     ROUND(AVG(amount_paid_ntd), 2) AS avg_amount_paid
# MAGIC FROM workspace.kkbox_silver.subscription_transactions
# MAGIC GROUP BY month
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.plan_list_price_ntd,
# MAGIC     COUNT(*) AS transactions,
# MAGIC     ROUND(AVG(c.is_churn) * 100, 1) AS churn_rate
# MAGIC FROM workspace.kkbox_silver.subscription_transactions t
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON t.member_id = c.msno
# MAGIC WHERE t.transaction_date >= '2017-01-01'
# MAGIC   AND t.transaction_date < '2017-02-01'
# MAGIC GROUP BY t.plan_list_price_ntd
# MAGIC ORDER BY t.plan_list_price_ntd;
