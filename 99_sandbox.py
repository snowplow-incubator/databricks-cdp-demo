# Databricks notebook source
# MAGIC %md
# MAGIC Outline
# MAGIC * Start with overview: Composable CDP
# MAGIC   * Use Case Overview
# MAGIC   * Business Impact of Solution
# MAGIC   * Show the end: Single audience segment in hightouch (derived from ML) distributed to Braze and Saleforce
# MAGIC * Step 1: Load atomic events table (bronze) using Snowplow's RDB loader
# MAGIC   * Show schema of data
# MAGIC   * Show actual data
# MAGIC * Step 2: Create silver tables using Snowplow's DBT web package
# MAGIC   * Users, Sessions, Page Views
# MAGIC   * Anything we want to show for EDA? Easily parsing nested JSON?
# MAGIC * Step 3: Load Salesforce data for conversion events
# MAGIC * Step 4: Prepare data for propensity scoring
# MAGIC * Step 5: Train propensity to engage model
# MAGIC   * Include SHAP
# MAGIC * Step 6: Use propensity to engage model for inference
# MAGIC   * Persist table for Hightouch?
# MAGIC * Step 7: Create audience segment in hightouch
# MAGIC * Step 8: Activate audience segment (Braze + Salesforce)
# MAGIC * Step 9: Monitor campaign performance

# COMMAND ----------



# COMMAND ----------


