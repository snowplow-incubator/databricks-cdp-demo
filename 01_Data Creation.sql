-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 1. Data Creation using Snowplow to stay fully compliant with GDPR
-- MAGIC 
-- MAGIC Snowplow Analytics is an open-source enterprise data creation platform that enables data collection from multiple products for advanced data analytics and AI/ML solutions. 
-- MAGIC 
-- MAGIC **Snowplow allows you to:** 
-- MAGIC 
-- MAGIC - Create a rich granular behavioural data across your digital platform
-- MAGIC - Define your own version-controlled custom events and entity schema
-- MAGIC - Enchrich the data with various services (pseudonymization, geo, currency exchange, campaign attribution...)
-- MAGIC - Provides flexible identity stitching
-- MAGIC - Control the quality of your data
-- MAGIC - Own your data asset and infrastructure
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_BDP.png" width="40%">
-- MAGIC 
-- MAGIC 
-- MAGIC Accurate and compliant identification of users is the cornerstone of any CDP. With first-party user identifiers included with each event and in-stream privacy tooling, including PII pseudonymization, you have a complete, and compliant view of every customer interaction.
-- MAGIC 
-- MAGIC Using Snowplow’s private deployment model and native connector to Databricks, your unified event stream lands in real-time in the Delta Lake.
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources:** 
-- MAGIC - What is Snowplow Platform? https://snowplow.io/snowplow-bdp/
-- MAGIC - What is Data Creation? https://snowplow.io/what-is-data-creation/
-- MAGIC - What is behavioural data? https://snowplow.io/what-is-behavioral-data/
-- MAGIC - Try Snowplow Platform: https://snowplow.io/get-started/

-- COMMAND ----------

-- DBTITLE 1,What makes Snowplow data GDPR compliant? 
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/Snowplow_gdpr.png" width="900%" style="float: center"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.1 Creating the data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC So let's now dive into how the data created by Snowplow platform looks like, what are the main figures, number of events and unique users entering the demo website. 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import plotly.express as px
-- MAGIC 
-- MAGIC df = spark.sql(
-- MAGIC """
-- MAGIC select date(collector_tstamp) as Date, count(distinct domain_userid) as users, count(*) as events
-- MAGIC from snowplow_samples.snowplow.events 
-- MAGIC where app_id = 'website'
-- MAGIC group by 1
-- MAGIC order by 1
-- MAGIC """
-- MAGIC ).toPandas()
-- MAGIC 
-- MAGIC fig = px.bar(df, x="Date", y=["users", "events"], barmode='group',
-- MAGIC              labels={"variable": "Count"},
-- MAGIC              title='Unique Users and Events over Time')
-- MAGIC fig.show()

-- COMMAND ----------

-- DBTITLE 1,Apply additional tracking plugins to collect GDPR context
-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_consent.png" width="100%" style="float: center"/>

-- COMMAND ----------

-- DBTITLE 1,Apply Snowplow enrichments in stream
-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC **The following Enrichments write data into atomic.events table:**
-- MAGIC 
-- MAGIC - PII Pseudonymization
-- MAGIC - IP lookups 
-- MAGIC - Campaign attribution
-- MAGIC - referer-parser enrichment
-- MAGIC - IAB
-- MAGIC 
-- MAGIC 
-- MAGIC **Resources:**
-- MAGIC - The list of all available enchrichments: https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/
-- MAGIC - What is tracking event? https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/browser-tracker/browser-tracker-v3-reference/tracking-events
-- MAGIC - How to set up a JavaScript tracker to my Web Product: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/javascript-tracker/web-quick-start-guide/
-- MAGIC - How to set up a Mobile tracker for my MobileApp: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/mobile-trackers/installation-and-set-up/
-- MAGIC - All additional supported tracking methods - https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/snowplow-tracker-protocol/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.2 Set your Databricks Loader
-- MAGIC 
-- MAGIC 
-- MAGIC - Setting up Databricks Loader for Snowplow: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader-3-0-0/loading-transformed-data/databricks-loader
-- MAGIC - GIT Repo: https://github.com/snowplow/snowplow-rdb-loader
-- MAGIC 
-- MAGIC 
-- MAGIC Snowplow's data use **RECAP** framework to help customers create right data with the right quality:
-- MAGIC 
-- MAGIC # 
-- MAGIC 
-- MAGIC  
-- MAGIC - **Reliable** - Self healing architecture streams data with a predictable latency and without loss. Catalog exists to understand usability (quality and structure) of data that is being tracked.
-- MAGIC - **Explainable** - It is clear how the Data was Created (generated, enhanced and modelled). It is possible to validate that every stage in the processing was completed successfully
-- MAGIC - **Compliant** End to end private cloud deployment and privacy tooling such as PII pseudonymisation and cookieless tracking to enforce governance on the data and socialise it in a compliant manner. Each line of data is enriched with basis for capture so it is unambiguous how it can be used.
-- MAGIC - **Accurate** - All data validated up front against predefined schemas; data quality actively monitored and alerted on with extensive debugging, QA and reprocessing tooling
-- MAGIC - **Predictive** - Behavioral signal is the strongest predictor of intent, the richness of the data is key to this with over 130 properties appended to each event out of the box and an extensible entity model that matches our mental map for the actions taking place.
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC JSON Schema for an atomic canonical Snowplow event: https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1.3 Start exploring the data from atomic (BRONZE) table
-- MAGIC 
-- MAGIC Even the most granular (atomic) data are very valuable for initial analysis of the data. Getting number of unique users, where the users are coming from, which campaigns etc... 

-- COMMAND ----------

-- DBTITLE 1,Atomic data created by Snowplow [BRONZE TABLE]
select * from snowplow_samples.snowplow.events limit 10

-- COMMAND ----------

select 
  contexts_com_snowplowanalytics_snowplow_ua_parser_context_1.device_family[0] as device_family,
  contexts_com_snowplowanalytics_snowplow_ua_parser_context_1.useragent_family[0] as browser_family,
  count(distinct domain_sessionid) as number_of_sessions
from snowplow_samples.snowplow.events
where
  unstruct_event_com_snowplowanalytics_snowplow_link_click_1.target_url = 'https://console.snowplowanalytics.com/'
  and app_id = 'website'
group by 1,2
order by 3 desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's more now into further analysis and data modelling...  NEXT NOTEBOOK
