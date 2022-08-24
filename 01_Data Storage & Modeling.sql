-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Snowplow web behavioural data
-- MAGIC 
-- MAGIC To get the most from advanced analytics and AI, you need rich, contextual data of the best quality. Snowplow can help you create granular behavioral data in your own Databricks lakehouse.
-- MAGIC 
-- MAGIC In this notebook we will be exploring behavioural data collected by Snowplow's Javascript tracker from Snowplow's own [website](https://snowplowanalytics.com/).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Atomic Events Table (Bronze)
-- MAGIC 
-- MAGIC All events are loaded using Snowplow's RDB loader into a single atomic events table backed by Databricks’ Delta tables. We call this a “Wide-row Table” – with one row per event, and one column for each type of entity/property and self-describing event.
-- MAGIC 
-- MAGIC [Go into more detail of how the data is loaded into Databricks. Snowplow pipeline diagram?]

-- COMMAND ----------

-- DBTITLE 1,atomic.events
select * from snowplow.events 
where app_id = 'website' and collector_tstamp_date = current_date() 
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For example we can query this table to examine users who are clicking the 'LOG IN' button on the site along with the page they're on, what device are they using, where in the world they are etc.

-- COMMAND ----------

-- DBTITLE 1,Examine Log In Events
select 
  domain_userid,
  event_name, 
  collector_tstamp,
  page_urlpath, 
  geo_country,
  geo_city,
  contexts_com_snowplowanalytics_snowplow_ua_parser_context_1.device_family[0] as device_family,
  contexts_com_snowplowanalytics_snowplow_ua_parser_context_1.useragent_family[0] as browser_family
from snowplow.events
where
  unstruct_event_com_snowplowanalytics_snowplow_link_click_1.target_url = 'https://console.snowplowanalytics.com/'
  and collector_tstamp_date >= date_sub(current_date(), 7) 
limit 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create derived tables using dbt (Gold - Analytics Ready)
-- MAGIC 
-- MAGIC Use Snowplow's dbt web package to transform and aggregate the raw web data into a set of derived tables straight out of the box:
-- MAGIC * `page_views`
-- MAGIC * `sessions`
-- MAGIC * `users`
-- MAGIC 
-- MAGIC The package processes all web events incrementally. It is not just constrained to page view events - any custom events you are tracking can also be incrementally processed. 
-- MAGIC 
-- MAGIC <img src="files/images/snowplow_web_model_dag.jpg" width="40%">
-- MAGIC 
-- MAGIC ### dbt Cloud using Partner Connect
-- MAGIC Easy setup yout dbt Cloud connection using Databricks' [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945).
-- MAGIC 
-- MAGIC ### Installing the snowplow_web dbt package
-- MAGIC 
-- MAGIC To include the package in your dbt project, include the following in your `packages.yml` file:
-- MAGIC 
-- MAGIC ```yaml
-- MAGIC packages:
-- MAGIC   - package: snowplow/snowplow_web
-- MAGIC     version: [">=0.9.0", "<0.10.0"]
-- MAGIC ```
-- MAGIC 
-- MAGIC Run `dbt deps` to install the package.
-- MAGIC 
-- MAGIC View the package on dbt's [package hub](https://hub.getdbt.com/snowplow/snowplow_web/latest/) or see the [dbt-snowplow-web GitHub repository](https://github.com/snowplow/dbt-snowplow-web) for more information.

-- COMMAND ----------

-- MAGIC %py
-- MAGIC slide_id = '1BZZhR_QyU8DhF4q_rEZo7tcgK1DGpvTWocdoEGwgp_0'
-- MAGIC slide_number = 'id.p1'
-- MAGIC  
-- MAGIC displayHTML(f'''
-- MAGIC <iframe
-- MAGIC   src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
-- MAGIC   frameborder="0"
-- MAGIC   width="75%"
-- MAGIC   height="600"
-- MAGIC ></iframe>
-- MAGIC ''')

-- COMMAND ----------

-- DBTITLE 1,snowplow_web_page_views
select * from dbt_cloud_derived.snowplow_web_page_views where start_tstamp_date >= date_sub(current_date(), 1) limit 10

-- COMMAND ----------

-- DBTITLE 1,snowplow_web_sessions
select * from dbt_cloud_derived.snowplow_web_sessions where start_tstamp_date >= date_sub(current_date(), 1) limit 10

-- COMMAND ----------

-- DBTITLE 1,snowplow_web_users
select * from dbt_cloud_derived.snowplow_web_users where start_tstamp_date >= date_sub(current_date(), 1) limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Exploration
-- MAGIC 
-- MAGIC First we look at what are the top blog posts on the website, which are most read and have the highest engagement. Also see which posts were a users first page and if any of those users returned for another session on the site. This analysis can be fed back to the content writers to review so future blog posts can be optimised to boost engagement. 

-- COMMAND ----------

-- DBTITLE 1,Examine Top Blog Posts
select 
  pv.page_title as blog_title, 
  count(*) as number_of_page_views,
  round(avg(pv.vertical_percentage_scrolled),1) as average_scroll_depth_percentage,
  -- round(avg(pv.engaged_time_in_s), 0) as average_engaged_time_in_s,
  round(count(distinct if(s.first_page_title = pv.page_title, domain_sessionid, null))/count(distinct domain_sessionid) * 100, 1) as entrance_page_percentage,
  round(count(distinct if(s.last_page_title = pv.page_title, domain_sessionid, null))/count(distinct domain_sessionid) * 100, 1) as exit_page_percentage
from dbt_cloud_derived.snowplow_web_page_views pv
join dbt_cloud_derived.snowplow_web_sessions s using(domain_sessionid)
where pv.page_urlpath like '/blog/%'
  and pv.start_tstamp_date between date_sub(current_date(), 7) and date_sub(current_date(), 1)
group by 1
order by 2 desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can also explore which users are most engaged with the website and have the most potential to convert into Snowplow customers. We can see how many times they have visited, first and last pages viewed and where they came from. If they have viewed any of the '[get-started](https://snowplowanalytics.com/get-started/)' pages where they can book a demo or go onto try Snowplow, we can flag this as them having intent to convert. These users may have a higher chance of converting to a Snowplow customer so it makes sense to target these users when running ad campaigns.

-- COMMAND ----------

-- DBTITLE 1,Examine Most Engaged Users
with intent_pages as (
  select domain_userid, True as has_intent
  from dbt_cloud_derived.snowplow_web_page_views
  where page_urlpath like '/get-started/%'
    and start_tstamp_date between date_sub(current_date(), 7) and date_sub(current_date(), 1)
  group by 1
)

select 
  u.domain_userid, 
  -- u.engaged_time_in_s, 
  u.page_views, u.sessions, 
  u.first_page_title, 
  u.last_page_title,
  u.referrer, 
  u.mkt_source,
  ifnull(i.has_intent, False) as has_intent_to_convert
from dbt_cloud_derived.snowplow_web_users u
left join intent_pages i using(domain_userid)
where u.start_tstamp_date between date_sub(current_date(), 7) and date_sub(current_date(), 1)
order by 2 desc, 3 desc, 4 desc
limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can examine if certain user properties have a relationship to a user's intent to convert, for example a user's operating system:

-- COMMAND ----------

-- MAGIC %py
-- MAGIC import plotly.express as px
-- MAGIC 
-- MAGIC df = spark.sql(
-- MAGIC   """
-- MAGIC   with intent_pages as (
-- MAGIC     select domain_sessionid, True as has_intent
-- MAGIC     from dbt_cloud_derived.snowplow_web_page_views
-- MAGIC     where page_urlpath like '/get-started/%'
-- MAGIC       and start_tstamp_date between date_sub(current_date(), 7) and date_sub(current_date(), 1)
-- MAGIC     group by 1
-- MAGIC   )
-- MAGIC   
-- MAGIC   select 
-- MAGIC     s.os_family,
-- MAGIC     ifnull(i.has_intent, False) as has_intent_to_convert,
-- MAGIC     count(1) as number_of_sessions
-- MAGIC   from dbt_cloud_derived.snowplow_web_sessions s
-- MAGIC   left join intent_pages i using(domain_sessionid)
-- MAGIC   where s.start_tstamp_date between date_sub(current_date(), 7) and date_sub(current_date(), 1)
-- MAGIC   group by 1, 2
-- MAGIC   order by 3 desc
-- MAGIC   """
-- MAGIC )
-- MAGIC df = df.toPandas()
-- MAGIC 
-- MAGIC fig = px.bar(df, x="os_family", y="number_of_sessions", color="has_intent_to_convert")
-- MAGIC fig.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Further data exploration using Databricks SQL
-- MAGIC 
-- MAGIC See DBSQL [Snowplow Website Insights](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/d98ec601-48c1-4f28-a06e-b8c75e118147-snowplow-website-insights?o=2894723222787945) dashboard to view some web analytics.
-- MAGIC 
-- MAGIC <img src="files/images/dashboard_screenshot_1.png" width="70%">
-- MAGIC 
-- MAGIC [provide more intresting insights in the dashboard]

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next step:
-- MAGIC Implement propoensity modeling with machine learning 
