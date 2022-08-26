-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction
-- MAGIC 
-- MAGIC Adpoting a composable CDP means best-in-class products at each stage in the pipeline, from data creation to storage, modeling and activation.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/feature/move_media/assets/composable_cdp.png" width="80%">
-- MAGIC 
-- MAGIC In these notebooks we will be exploring behavioural data collected by Snowplow's Javascript tracker from Snowplow's own [website](https://snowplowanalytics.com/) stored in Databricks. We will then model this data to make it analytics ready using dbt. This dataset can then be used to more effectivley target customers using Hightouch to sync to 3rd party destinations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In this demo we show how we can build user segments based on a user's web beavioural data and sync these to Braze email subscription groups.
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/feature/move_media/assets/hightouch_syncs_braze.png" width="50%">
