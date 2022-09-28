-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 3. Audience Activation using Hightouch
-- MAGIC 
-- MAGIC Hightouch syncs your data in Databricks to the tools that your business teams rely on. In this notebook we will use Hightouch to create audiences from our Gold Databricks user tables so we can sync them to various destinations like Braze, Facebook and Salesforce.

-- COMMAND ----------

-- DBTITLE 0,Untitled
-- MAGIC %md
-- MAGIC 
-- MAGIC ## 3.1. Connect Hightough and Databricks via PartnerConnect
-- MAGIC 
-- MAGIC We can easily connect to Hightouch from Databricks using [Partner Connect](https://docs.databricks.com/integrations/partner-connect/reverse-etl.html).
-- MAGIC 1. Make sure your Databricks account, workspace, and the signed-in user all meet the [requirements](https://docs.databricks.com/integrations/partner-connect/index.html#requirements) for Partner Connect.
-- MAGIC 2. In the Databricks sidebar, click **Partner Connect**.
-- MAGIC 3. Find the Hightouch tile. If the tile has a check mark icon, stop here, as our workspace is already connected. Otherwise, follow the on-screen directions to finish creating the connection.
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

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### 3.2.1. Create a parent model
-- MAGIC 
-- MAGIC First we need to create a [parent model table](https://hightouch.com/docs/audiences/schema#the-parent-model-table) for the audiences to be built off of. This can be based on our out-of-the-box Snowplow modelled `snowplow_web_users` table or any custom gold user tables that have been built.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.2. Audience Builder
-- MAGIC Now we can build an audience using columns from our parent model. In this example we are targetting high propensity users in the awareness stage based on the following criteria:
-- MAGIC * First time on website
-- MAGIC * Did not arrive from a marketing campaign
-- MAGIC * **High** propensity score
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_audience_builder.png" width="30%">
-- MAGIC 
-- MAGIC You can see we have utilised a [related model](https://hightouch.com/docs/audiences/schema#other-objects) *User Propensity Scores*. This model is based on the table of propensity scores outputted by our predictive ML model. You can join other source tables like this to your user parent model.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.3. Add Hightouch Events (Optional)
-- MAGIC It can be useful to flag key user behavoir like adding a product to basket or filling out a form as a Hightouch [Event](https://hightouch.com/docs/audiences/schema#events). Similary to related models, these can then be joined onto the parent model to filter your audiences by.
-- MAGIC 
-- MAGIC For example, you may want to filter an audience by if or when the user had viewed a certain page on your website. You can make this an audience event using the following query on your `snowplow_web_page_views` table:
-- MAGIC 
-- MAGIC ```sql
-- MAGIC select 
-- MAGIC   domain_userid,
-- MAGIC   page_view_id,
-- MAGIC   start_tstamp
-- MAGIC from dbt_cloud_derived.snowplow_web_page_views
-- MAGIC where page_urlpath like '/get-started/%'
-- MAGIC ```
-- MAGIC Once you have created the event and added the relationship to our parent model, you can use it as an audience filter.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_example_event.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.4. Audience Splits (Optional)
-- MAGIC 
-- MAGIC Use [Audience Splits](https://hightouch.com/blog/audience-splits) to manage A/B and  multivariate testing across your channels. You can also add stratification variables to ensure that the randomized groups of users are distributed as desired.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_splits.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.3. Sync the created Audiences with Marketing channels
-- MAGIC 
-- MAGIC After setting up your required [destinations](https://hightouch.com/integrations) in Hightouch you can sync up your new audiences. 
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_syncs.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.4. Schedule your audience syncs
-- MAGIC 
-- MAGIC It is important that our audiences connected to these third party tools are always up to date and in sync with our Snowplow web data in Databricks. 
-- MAGIC 
-- MAGIC One way we can ensure this by using the [dbt Cloud extension](https://hightouch.com/docs/syncs/dbt-cloud) to trigger syncs after the dbt Snowplow web model job finishes and our derived tables are updated.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_dbt_schedule.png" width="30%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.5. Summary
-- MAGIC 
-- MAGIC We have now finished creating our audiences and have them synced up to your needed destinations!
-- MAGIC 
-- MAGIC Hightouch is now setup with all the data and events from Databricks to enable our teams to easily build new audiences based on Snowplow's rich behavioural data and sync to their needed destinations.
