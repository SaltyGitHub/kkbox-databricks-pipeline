# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     member_id,
# MAGIC     COUNT(DISTINCT activity_date) AS active_days
# MAGIC FROM workspace.kkbox_silver.daily_listening_activity
# MAGIC GROUP BY member_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.member_id,
# MAGIC     t.membership_expiry_date,
# MAGIC     COUNT(DISTINCT a.activity_date) AS active_days_last_14
# MAGIC FROM workspace.kkbox_silver.daily_listening_activity a
# MAGIC JOIN workspace.kkbox_silver.subscription_transactions t
# MAGIC     ON a.member_id = t.member_id
# MAGIC WHERE a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                           AND t.membership_expiry_date
# MAGIC GROUP BY
# MAGIC     a.member_id,
# MAGIC     t.membership_expiry_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.member_id,
# MAGIC     CASE
# MAGIC         WHEN t.is_cancellation THEN 'Cancelled'
# MAGIC         WHEN t.is_auto_renew THEN 'Auto-renew'
# MAGIC         ELSE 'Manual'
# MAGIC     END AS renewal_type,
# MAGIC     COUNT(DISTINCT a.activity_date) AS active_days_last_14
# MAGIC FROM workspace.kkbox_silver.daily_listening_activity a
# MAGIC JOIN workspace.kkbox_silver.subscription_transactions t
# MAGIC     ON a.member_id = t.member_id
# MAGIC WHERE a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                           AND t.membership_expiry_date
# MAGIC GROUP BY
# MAGIC     a.member_id,
# MAGIC     renewal_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH engagement AS (
# MAGIC     SELECT
# MAGIC         a.member_id,
# MAGIC         t.membership_expiry_date,
# MAGIC         CASE
# MAGIC             WHEN t.is_cancellation THEN 'Cancelled'
# MAGIC             WHEN t.is_auto_renew THEN 'Auto-renew'
# MAGIC             ELSE 'Manual'
# MAGIC         END AS renewal_type,
# MAGIC         COUNT(DISTINCT a.activity_date) AS active_days
# MAGIC     FROM workspace.kkbox_silver.daily_listening_activity a
# MAGIC     JOIN workspace.kkbox_silver.subscription_transactions t
# MAGIC         ON a.member_id = t.member_id
# MAGIC     WHERE a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                               AND t.membership_expiry_date
# MAGIC     GROUP BY
# MAGIC         a.member_id,
# MAGIC         t.membership_expiry_date,
# MAGIC         renewal_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     renewal_type,
# MAGIC     active_days,
# MAGIC     COUNT(*) AS users
# MAGIC FROM engagement
# MAGIC GROUP BY renewal_type, active_days
# MAGIC ORDER BY renewal_type, active_days;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_tx AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY member_id
# MAGIC                ORDER BY transaction_date DESC
# MAGIC            ) AS rn
# MAGIC     FROM workspace.kkbox_silver.subscription_transactions
# MAGIC     WHERE membership_expiry_date BETWEEN '2017-03-01' AND '2017-03-31'
# MAGIC ),
# MAGIC
# MAGIC engagement AS (
# MAGIC     SELECT
# MAGIC         t.member_id,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN t.is_cancellation THEN 'Cancelled'
# MAGIC             WHEN t.is_auto_renew THEN 'Auto-renew'
# MAGIC             ELSE 'Manual'
# MAGIC         END AS renewal_type,
# MAGIC
# MAGIC         COUNT(DISTINCT a.activity_date) AS active_days
# MAGIC
# MAGIC     FROM latest_tx t
# MAGIC
# MAGIC     LEFT JOIN workspace.kkbox_silver.daily_listening_activity a
# MAGIC         ON t.member_id = a.member_id
# MAGIC         AND a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                                 AND t.membership_expiry_date
# MAGIC
# MAGIC     WHERE t.rn = 1
# MAGIC
# MAGIC     GROUP BY
# MAGIC         t.member_id,
# MAGIC         renewal_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     renewal_type,
# MAGIC     active_days,
# MAGIC     COUNT(DISTINCT member_id) AS users
# MAGIC FROM engagement
# MAGIC GROUP BY renewal_type, active_days
# MAGIC ORDER BY renewal_type, active_days;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_tx AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY member_id
# MAGIC                ORDER BY transaction_date DESC
# MAGIC            ) AS rn
# MAGIC     FROM workspace.kkbox_silver.subscription_transactions
# MAGIC     WHERE membership_expiry_date BETWEEN '2017-03-01' AND '2017-03-31'
# MAGIC ),
# MAGIC
# MAGIC engagement AS (
# MAGIC     SELECT
# MAGIC         t.member_id,
# MAGIC         CASE
# MAGIC             WHEN t.is_cancellation THEN 'Cancelled'
# MAGIC             WHEN t.is_auto_renew THEN 'Auto-renew'
# MAGIC             ELSE 'Manual'
# MAGIC         END AS renewal_type,
# MAGIC         COUNT(DISTINCT a.activity_date) AS active_days
# MAGIC     FROM latest_tx t
# MAGIC     LEFT JOIN workspace.kkbox_silver.daily_listening_activity a
# MAGIC         ON t.member_id = a.member_id
# MAGIC         AND a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                                 AND t.membership_expiry_date
# MAGIC     WHERE t.rn = 1
# MAGIC     GROUP BY t.member_id, renewal_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     e.renewal_type,
# MAGIC     e.active_days,
# MAGIC     COUNT(*) AS users,
# MAGIC     ROUND(AVG(c.is_churn) * 100, 1) AS churn_rate
# MAGIC FROM engagement e
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON e.member_id = c.msno
# MAGIC GROUP BY e.renewal_type, e.active_days
# MAGIC ORDER BY e.renewal_type, e.active_days;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_tx AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC                PARTITION BY member_id
# MAGIC                ORDER BY transaction_date DESC
# MAGIC            ) AS rn
# MAGIC     FROM workspace.kkbox_silver.subscription_transactions
# MAGIC     WHERE membership_expiry_date BETWEEN '2017-03-01' AND '2017-03-31'
# MAGIC ),
# MAGIC
# MAGIC engagement AS (
# MAGIC     SELECT
# MAGIC         t.member_id,
# MAGIC         CASE
# MAGIC             WHEN t.is_cancellation THEN 'Cancelled'
# MAGIC             WHEN t.is_auto_renew THEN 'Auto-renew'
# MAGIC             ELSE 'Manual'
# MAGIC         END AS renewal_type,
# MAGIC         COUNT(DISTINCT a.activity_date) AS active_days
# MAGIC     FROM latest_tx t
# MAGIC     LEFT JOIN workspace.kkbox_silver.daily_listening_activity a
# MAGIC         ON t.member_id = a.member_id
# MAGIC         AND a.activity_date BETWEEN DATE_SUB(t.membership_expiry_date, 13)
# MAGIC                                 AND t.membership_expiry_date
# MAGIC     WHERE t.rn = 1
# MAGIC     GROUP BY t.member_id, renewal_type
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     e.renewal_type,
# MAGIC     e.active_days,
# MAGIC     COUNT(*) AS users,
# MAGIC     ROUND(AVG(c.is_churn) * 100, 1) AS churn_rate
# MAGIC FROM engagement e
# MAGIC JOIN workspace.kkbox_raw.train_v2 c
# MAGIC     ON e.member_id = c.msno
# MAGIC GROUP BY e.renewal_type, e.active_days
# MAGIC ORDER BY e.renewal_type, e.active_days;
