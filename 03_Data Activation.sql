-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3. Audience Activation using Hightouch
-- MAGIC 
-- MAGIC Hightouch syncs your data in Databricks to the tools that your business teams rely on. In this notebook we will use Hightouch to create rule based audiences from our Gold Databricks tables so we can sync them to various destinations like Braze.

-- COMMAND ----------

-- DBTITLE 0,Untitled
-- MAGIC %md
-- MAGIC 
-- MAGIC ## 3.1. Connect Hightough and Databricks via PartnerConnect
-- MAGIC 
-- MAGIC We can easily connect to Hightouch from Databricks using [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945):
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_partner_connect.png" width="25%">
-- MAGIC 
-- MAGIC Once setup we can see our Databricks cluster in the [Sources](https://app.hightouch.com/snowplow-yzw4c/sources) tab in Hightouch:
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_sources.png" width="25%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.2. Create the audience cohort
-- MAGIC 
-- MAGIC Say our Marketing team want to target two user segments and execute marketing campaigns to accelerate conversion (create new MQLs) and engage new visitors with the product. 
-- MAGIC # 
-- MAGIC 1. **Awareness visitors**
-- MAGIC 
-- MAGIC First we create a parent model based on all our `snowplow_web_users` table for the audiences to be built off of (see [here](https://app.hightouch.com/snowplow-yzw4c/audiences/setup/parent-models/591471)). 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.1. Awareness Users
-- MAGIC Create an [Awareness Users](https://app.hightouch.com/snowplow-yzw4c/audiences/591472) audience based on the following rules:
-- MAGIC 
-- MAGIC - First time on website
-- MAGIC - Lower engagement (less than 60 secs engaged)
-- MAGIC - Landed on the Snowplow homepage
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_awareness_users_audience_builder.png" width="40%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.2. Awareness Audience
-- MAGIC Create an [Awareness Users](https://app.hightouch.com/snowplow-yzw4c/audiences/591474) audience based on the following rules:
-- MAGIC 
-- MAGIC - Returned (2 sessions)
-- MAGIC - Had intent to engage within the last 30 days
-- MAGIC 
-- MAGIC To flag if a user had intent to engage we see if they had viewed one of the [get-started](https://snowplowanalytics.com/get-started/) pages on Snowplow's website. We can make this an Audience Event using the following query based on our `snowplow_web_page_views` table:
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
-- MAGIC After creating the event (see set up [here](https://app.hightouch.com/snowplow-yzw4c/audiences/setup/events/591234)), we need to add a direct relationship between this and our *All Users* parent model.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_direct_relationship.png" width="35%">
-- MAGIC 
-- MAGIC We can now use this event as a filter when we build our Engagement Users audience:
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_engagement_users_audience_builder.png" width="35%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.3. Sync the created Audiences with Marketing channels
-- MAGIC 
-- MAGIC After setting up Braze as a [destination](https://app.hightouch.com/snowplow-yzw4c/destinations) in Hightouch, we can sync up our new audiences. In this case we want to sync these audiences to our *Awareness Users* and *Engagement Users* Braze subscription groups.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_configure_braze.png" width="40%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.4. Orchestrate an auto-update of audiences
-- MAGIC It is important that our audiences connected to third party tools like Braze are always up to date and in sync with our Snowplow web data in Databricks. 
-- MAGIC 
-- MAGIC We can ensure this by using the [dbt Cloud extension](https://app.hightouch.com/snowplow-yzw4c/extensions) to trigger syncs after the dbt Snowplow web model job finishes and our Gold tables are updated.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_dbt_schedule.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.5. Summary
-- MAGIC 
-- MAGIC We have now finished creating our audiences and have them synced up to Braze!
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_syncs_braze.png" width="35%">
-- MAGIC 
-- MAGIC Hightouch is now setup with all the data and events from Databricks to enable our teams to easily build new audiences based on Snowplow's rich behavioural data and sync to their needed destinations.
