-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Introduction
-- MAGIC 
-- MAGIC Adpoting a composable CDP means best-in-class products at each stage in the pipeline, from data creation to storage, modeling and activation.
-- MAGIC 
-- MAGIC <img src="files/images/composable_cdp.png" width="80%">
-- MAGIC 
-- MAGIC In these notebooks we will be exploring behavioural data collected by Snowplow's Javascript tracker from Snowplow's own [website](https://snowplowanalytics.com/). We will then model this data to make it analytics ready using dbt. The end goal will be to use this dataset to more effectivley target customers to book a demo with Snowplow's platform using Hightouch to sync to 3rd party destinations like Google Ads.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *{{show end result of Hightouch linked to Google Ads}}*
-- MAGIC 
-- MAGIC <img src="files/images/hightouch_syncs.png" width="50%">
