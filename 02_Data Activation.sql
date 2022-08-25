-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Hightouch
-- MAGIC 
-- MAGIC Hightouch syncs your data in Databricks to the tools that your business teams rely on.
-- MAGIC 
-- MAGIC In this notebook we will use Hightouch to create rule based audiences from our Gold tables and sync to various destinations like Google Ads.
-- MAGIC 
-- MAGIC We can easily connect to Hightouch from Databricks using [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945):
-- MAGIC 
-- MAGIC <img src="files/images/hightouch_partner_connect.png" width="27%">
-- MAGIC 
-- MAGIC Once setup we can see our Databricks Cluster in the Sources tab in Hightouch:
-- MAGIC 
-- MAGIC <img src="files/images/hightouch_sources.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Audiences
-- MAGIC 
-- MAGIC ### Awareness Users
-- MAGIC Create an Awareness Users audience based on the following rules:
-- MAGIC 
-- MAGIC - First time on website
-- MAGIC - Lower engagement
-- MAGIC - Landed on the Snowplow homepage
-- MAGIC - Seen within the last 30 days
-- MAGIC 
-- MAGIC This should be synced to Google Ads for retargeting.

-- COMMAND ----------

-- 1. Awareness users
select count(1) as number_of_users
from dbt_cloud_derived.snowplow_web_users
where
  sessions = 1
  and engaged_time_in_s between 0 and 60
  and first_page_url like '%snowplowanalytics.com/'
  and start_tstamp_date >= date_sub(current_date(), 30)
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Engagement Users
-- MAGIC Create an Engagement Users audience based on the following rules:
-- MAGIC 
-- MAGIC - Returned (2 sessions)
-- MAGIC - Had intent to convert within the last 30 days
-- MAGIC - Didn't convert
-- MAGIC 
-- MAGIC This should also be synced to Google Ads for retargeting.
-- MAGIC 
-- MAGIC To flag if a user had intent to convert we see if they had viewed one of the getting started pages on Snowplow's site. We can make this an Audience Event using this query:
-- MAGIC 
-- MAGIC ```sql
-- MAGIC select 
-- MAGIC   domain_userid,
-- MAGIC   page_view_id,
-- MAGIC   start_tstamp
-- MAGIC from dbt_cloud_derived.snowplow_web_page_views
-- MAGIC where page_urlpath like '/get-started/%'
-- MAGIC ```
-- MAGIC 
-- MAGIC We can add this as a filter when we build the audience:
-- MAGIC 
-- MAGIC <img src="files/images/hightouch_engagement_users_audience_builder.png" width="40%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Syncs
-- MAGIC 
-- MAGIC We can use the dbt Cloud extension to trigger syncs after the Snowplow web model job finishes. This will mean our audiences will always be up to date and match with the data in Databricks. 
-- MAGIC 
-- MAGIC <img src="files/images/hightouch_dbt_schedule.png" width="35%">
