# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/logo.png" width="80%">

# COMMAND ----------

# DBTITLE 1,Composable CDP Demo Overview
# MAGIC %md
# MAGIC * **Background:** Data practitioners across the globe visit Snowplow's website to learn how the leader in data creation helps organizations create and maintain high quality and privacy-compliant first party data. While some of these visitors request a demo instantly, others remain in the awareness stage of the marketing funnel for some time.
# MAGIC   * **Business Objective:** Convert prospects from awareness to engaged users (request for demo).
# MAGIC   * **Marketing Strategy:** Focus remarketing efforts on prospects with a hight propensity to convert.
# MAGIC   * **Audience Segmentation:** Rules-based audience segment and ML-based audience segment
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/ad_campaign_dashboard.png" width="70%" style="float:right"/>
# MAGIC 
# MAGIC [Link to Dashboard](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/3c27e852-ca5e-40fc-a701-f8c5e580ad19?o=2894723222787945)
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Composable CDP
# MAGIC %md
# MAGIC We will leverage a **Composable CDP** built with Snowplow, Databricks, and Hightouch to achieve our goal of converting website visitors into engaged prospects by remarketing to them through various marketing channels.
# MAGIC 
# MAGIC The components of standard CDP offerings can be classified into the following categories:
# MAGIC 
# MAGIC 
# MAGIC - **Data Collection** : CDPs are designed to collect customer events from a number of different sources (onsite, mobile applications and server-side) and append these activities to the customer profile. These events typically contain metadata to provide detailed context about the customer's specific digital interactions. Event collection is typically designed to support marketing use cases such as marketing automation.
# MAGIC 
# MAGIC - **Data Storage and Modeling**: CDPs provide a proprietary repository of data that aggregates and manages different sources of customer data collected from most of the business's SaaS and internal applications. The unified database is a 360 degree view about each customer and a central source of truth for the business. Most CDPs have out-of-the-box identity stitching functionality and tools to create custom traits on user profiles.
# MAGIC 
# MAGIC - **Data Activation**: CDPs offer the ability to build audience segments leveraging the data available in the platform. Thanks to a wide-array of pre-built integrations, these audiences and other customer data points are then able to be pushed both to and from various marketing channels.
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/composable_cdp.png" width="60%">

# COMMAND ----------

# DBTITLE 1,Composable CDP: Demo Workflow
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/composable-cdp/workflow2.png" width="80%" style="float: right" />
# MAGIC 
# MAGIC * **Step 1:** Create  compliant behavioural data using Snowplow
# MAGIC * **Step 2:** Apply advanced tracking with GDPR context and consent and enrichments features to ensure full compliance of the data and load data into DeltaLake
# MAGIC * **Step 3:** Create silver tables using Snowplow's DBT package
# MAGIC * **Step 4:** Perform exploratory data analysis using Databricks SQL
# MAGIC * **Step 5:** Create gold table for audience segmentation
# MAGIC * **Step 6:** Use Hightouch for rules-based remarketing
# MAGIC * **Step 7:** Train propensity to convert model using XGBoost and Mlflow
# MAGIC * **Step 8:** Use model to predict propensity to convert
# MAGIC * **Step 9:** Use Hightouch for ML-based remarketing
# MAGIC * **Step 10:** Monitor campaign performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create behavioural event data using Snowplow

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Snowplow is an open-source enterprise data creation platform that enables data collection from multiple products for advanced data analytics and AI/ML solutions. 
# MAGIC 
# MAGIC **Snowplow allows you to:** 
# MAGIC 
# MAGIC - Create a rich granular behavioural data across your digital platform
# MAGIC - Define your own version-controlled custom events and entity schema
# MAGIC - Enchrich the data with various services (pseudonymization, geo, currency exchange, campaign attribution...)
# MAGIC - Provides flexible identity stitching
# MAGIC - Control the quality of your data
# MAGIC - Own your data asset and infrastructure
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_pipeline.png" width="40%" style="float: left"/>
# MAGIC 
# MAGIC [Setting up the JavaScript tracker for web](https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/javascript-tracker/web-quick-start-guide/) | [All available enchrichments](https://docs.snowplow.io/docs/enriching-your-data/available-enrichments/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load atomic event data into Databricks using Snowplow's RDB loader

# COMMAND ----------

# MAGIC %md
# MAGIC Once your tracking is set up, all events are loaded in real-time using Snowplow's RDB loader into a single atomic events table backed by Databricks’ Delta tables. We call this a “Wide-row Table” – with one row per event, and one column for each type of entity/property and self-describing event.

# COMMAND ----------

# DBTITLE 1,View atomic event data
# MAGIC %sql
# MAGIC select * from snowplow_samples.snowplow.events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create silver tables using Snowplow's DBT package

# COMMAND ----------

# MAGIC %md
# MAGIC Snowplow's out-of-the-box dbt models aggregate the data to different levels of altitude, so data scientists can directly consume the right altitude for their model. For composable CDP use cases, we're typically making predictions at the user-level. However, it is valuable to be able to drill down into the session, page view and event level, to understand where the user is in their journey and how that is changing over time.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_web_model_dag.png" width="30%">
# MAGIC 
# MAGIC 
# MAGIC ### The dbt package will:
# MAGIC 
# MAGIC - Transforms and aggregates raw web event data collected from the Snowplow JavaScript tracker into a set of derived tables: page views, sessions and users.
# MAGIC - Derives a mapping between user identifiers, allowing for 'session stitching' and the development of a single customer view.
# MAGIC - Processes all web events incrementally. It is not just constrained to page view events - any custom events you are tracking will also be incrementally processed.
# MAGIC - Is designed in a modular manner, allowing you to easily integrate your own custom SQL into the incremental framework provided by the package.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### Steps to proceed:
# MAGIC 
# MAGIC **2.1. Use dbt Cloud using Partner Connect**
# MAGIC Easily setup yout dbt Cloud connection using Databricks' [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945).
# MAGIC 
# MAGIC **2.2. Install the snowplow_web dbt package**
# MAGIC To include the package in your dbt project, include the following in your `packages.yml` file:
# MAGIC 
# MAGIC ```yaml
# MAGIC packages:
# MAGIC   - package: snowplow/snowplow_web
# MAGIC     version: [">=0.9.0", "<0.10.0"]
# MAGIC ```
# MAGIC 
# MAGIC Run `dbt deps` to install the package.
# MAGIC 
# MAGIC [Snowplow dbt packages](https://hub.getdbt.com/snowplow/) | [Using dbt with Databricks (if not via Partner Connect)](https://github.com/databricks/dbt-databricks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What does the modelled data actually look like?
# MAGIC 
# MAGIC Below is a view of a single user shown through our users, sessions and page views derived tables:

# COMMAND ----------

# DBTITLE 1,View users silver table
# MAGIC %sql
# MAGIC select domain_userid, start_tstamp, end_tstamp, engaged_time_in_s, page_views,sessions, first_page_title, last_page_title,mkt_source, mkt_medium, mkt_campaign
# MAGIC from snowplow_samples.dbt_cloud_derived.snowplow_web_users
# MAGIC where domain_userid = '831e2935bc84857e21b6fe174e43cbef93572038990d323258cc2ca9ad7ce891';

# COMMAND ----------

# DBTITLE 1,View sessions silver table
# MAGIC %sql
# MAGIC select domain_sessionid, start_tstamp, engaged_time_in_s, page_views, first_page_title, last_page_title, mkt_source, mkt_medium, mkt_campaign, geo_country, geo_city, device_family, os_family
# MAGIC from snowplow_samples.dbt_cloud_derived.snowplow_web_sessions
# MAGIC where domain_userid = '831e2935bc84857e21b6fe174e43cbef93572038990d323258cc2ca9ad7ce891'
# MAGIC order by start_tstamp;

# COMMAND ----------

# DBTITLE 1,View page views silver table
# MAGIC %sql
# MAGIC select page_view_id, domain_sessionidx, start_tstamp, page_title, page_urlpath, engaged_time_in_s, absolute_time_in_s, vertical_percentage_scrolled, horizontal_percentage_scrolled, mkt_source, mkt_medium, mkt_campaign
# MAGIC from snowplow_samples.dbt_cloud_derived.snowplow_web_page_views
# MAGIC where domain_userid = '831e2935bc84857e21b6fe174e43cbef93572038990d323258cc2ca9ad7ce891'
# MAGIC order by start_tstamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Perform exploratory data analysis using Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/dashboard_screenshot_1.png" width="70%">
# MAGIC 
# MAGIC [Snowplow Website Insights](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/d98ec601-48c1-4f28-a06e-b8c75e118147-snowplow-website-insights?o=2894723222787945) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create gold table for audience segmentation

# COMMAND ----------

# DBTITLE 1,Construct gold table
# MAGIC %sql 
# MAGIC 
# MAGIC WITH pv AS (
# MAGIC   SELECT domain_userid, absolute_time_in_s, vertical_percentage_scrolled, geo_country, geo_region, br_lang, device_family, os_family,
# MAGIC     row_number() OVER (PARTITION BY domain_userid ORDER BY start_tstamp) AS rn
# MAGIC   FROM snowplow_samples.dbt_cloud_derived.snowplow_web_page_views
# MAGIC   WHERE page_view_in_session_index = 1 qualify rn = 1
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   u.domain_userid, u.first_page_title, u.refr_urlhost, lower(u.refr_medium) as refr_medium, lower(u.mkt_medium) as mkt_medium, 
# MAGIC   u.mkt_source, u.mkt_term, u.mkt_campaign, u.engaged_time_in_s, u.sessions, u.page_views,
# MAGIC   pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country, pv.geo_region, pv.br_lang, pv.device_family, pv.os_family,
# MAGIC   ifnull(c.converted, false) as converted_user,
# MAGIC   0.00 as propensity_prediction -- to be populated by our ML model later
# MAGIC FROM
# MAGIC   snowplow_samples.dbt_cloud_derived.snowplow_web_users u
# MAGIC   JOIN pv ON u.domain_userid = pv.domain_userid
# MAGIC   AND pv.rn = 1
# MAGIC   LEFT JOIN snowplow_samples.samples.converted_users c USING(domain_userid)

# COMMAND ----------

df = _sqldf.na.fill(0)

# COMMAND ----------

# DBTITLE 1,Persist gold table
_sqldf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("snowplow_samples.samples.snowplow_website_users_first_touch_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Use Hightouch for rule-based remarketing

# COMMAND ----------

# DBTITLE 1,Three steps to audience segmentation
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_rules-based.png" width="80%">
# MAGIC 
# MAGIC [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945) | [Hightouch Audiences](https://app.hightouch.com/snowplow-yzw4c/audiences) | [Hightouch Destinations](https://app.hightouch.com/snowplow-yzw4c/destinations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Train propensity to convert model using XGBoost and MLflow

# COMMAND ----------

# DBTITLE 1,Import libraries
import re
import mlflow
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score
from hyperopt import fmin, tpe, rand, hp, Trials, STATUS_OK, SparkTrials, space_eval
from mlflow.models.signature import infer_signature
from xgboost import XGBClassifier
from pyspark.sql.functions import col, struct, coalesce
from pyspark.sql.types import DoubleType, StringType
from hyperopt.pyll.base import scope

# COMMAND ----------

# DBTITLE 1,Create train and test data sets
df = spark.table("snowplow_samples.samples.snowplow_website_users_first_touch_gold").toPandas()
# Select columns we want to use for the model from our Gold user table
df = df[["engaged_time_in_s", "absolute_time_in_s", "vertical_percentage_scrolled", "refr_medium", "mkt_medium", "converted_user"]]
df = pd.get_dummies(df,columns=['refr_medium','mkt_medium'],dtype='int64')
features = list(df.columns[:-1])
X_train, X_test, y_train, y_test = train_test_split(df[features], df["converted_user"], test_size=0.33, random_state=55)

# COMMAND ----------

# DBTITLE 1,Define model evaluation for hyperopt
def evaluate_model(params):
  #instantiate model
  model = XGBClassifier(learning_rate=params["learning_rate"],
                            gamma=int(params["gamma"]),
                            reg_alpha=int(params["reg_alpha"]),
                            reg_lambda=int(params["reg_lambda"]),
                            max_depth=int(params["max_depth"]),
                            n_estimators=int(params["n_estimators"]),
                            min_child_weight = params["min_child_weight"], objective='reg:linear', early_stopping_rounds=50)
  
  #train
  model.fit(X_train, y_train)
  
  #predict
  y_prob = model.predict_proba(X_test)
  
  #score
  precision = average_precision_score(y_test, y_prob[:,1])
  mlflow.log_metric('avg_precision', precision)  # record actual metric with mlflow run
  
  # return results (negative precision as we minimize the function)
  return {'loss': -precision, 'status': STATUS_OK, 'model': model}

# COMMAND ----------

# DBTITLE 1,Define search space for hyperopt
# define hyperopt search space
search_space = {'max_depth': scope.int(hp.quniform('max_depth', 2, 8, 1)),
                'learning_rate': hp.loguniform('learning_rate', -3, 0),
                'gamma': hp.uniform('gamma', 0, 5),
                'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
                'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
                'min_child_weight': scope.int(hp.loguniform('min_child_weight', -1, 3)),
                'n_estimators':  scope.int(hp.quniform('n_estimators', 50, 200, 1))}

# COMMAND ----------

# DBTITLE 1,Perform evaluation to optimal hyperparameters
# perform evaluation
with mlflow.start_run(run_name='XGBClassifier') as run:
  trials = SparkTrials(parallelism=4)
  argmin = fmin(fn=evaluate_model, space=search_space, algo=tpe.suggest, max_evals=20, trials=trials)
  #log the best model information
  model = trials.best_trial['result']['model']
  signature = infer_signature(X_test, model.predict_proba(X_test))
  mlflow.sklearn.log_model(trials.best_trial['result']['model'], 'model', signature=signature, input_example=X_test.iloc[0].to_dict())
  #add hyperopt model params
  for p in argmin:
    mlflow.log_param(p, argmin[p])
  mlflow.log_metric("avg_precision", trials.best_trial['result']['loss'])
  run_id = run.info.run_id

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a version
model_registered = mlflow.register_model("runs:/"+run_id+"/model", "field_demos_ccdp")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_ccdp", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 7: Use model to predict propensity to convert
# MAGIC 
# MAGIC Now that our model is built and saved in MLFlow registry, we can load it to run our inferences at scale.

# COMMAND ----------

# DBTITLE 1,Load the model from registry as UDF
#                                 Stage/version
#                       Model name       |
#                           |            |
model_path = 'models:/field_demos_ccdp/Production'
predict_propensity = mlflow.pyfunc.spark_udf(spark, model_path, result_type='string')


# COMMAND ----------

# DBTITLE 1,Perform inference
model_features = predict_propensity.metadata.get_input_schema().input_names()

users_gold = spark.table('snowplow_samples.samples.snowplow_website_users_first_touch_gold')
df = pd.get_dummies(users_gold.toPandas(),columns=['refr_medium','mkt_medium'],dtype='int64')
df = spark.createDataFrame(df)
predictions = df.withColumn('propensity_prediction', predict_propensity(*model_features))

display(predictions.filter(predictions.propensity_prediction == True))

# COMMAND ----------

# DBTITLE 1,Save propensity to convert predictions
# Join the propensity prediction back into our gold user table
user_cols = list(users_gold.columns[:-1])
user_cols = ['u.' + col for col in user_cols]
users_gold = users_gold.alias('u').join(predictions.alias('p'), ['domain_userid'], how='left').select(*user_cols, coalesce('p.propensity_prediction', 'u.propensity_prediction').alias('propensity_prediction'))
users_gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("snowplow_samples.samples.snowplow_website_users_first_touch_gold") 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Use Hightouch for ML-based remarketing

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_ml-based.png" width="60%">
# MAGIC 
# MAGIC [Hightouch Audiences](https://app.hightouch.com/snowplow-yzw4c/audiences) | [Hightouch Destinations](https://app.hightouch.com/snowplow-yzw4c/destinations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Compare campaign performance: ML vs. Rule Based

# COMMAND ----------

# DBTITLE 1,Ingest return path data from Fivetran [optional]
# MAGIC %md
# MAGIC E.g. - Facebook, Google AdWords, Instagram, Snapchat, Youtube

# COMMAND ----------

# DBTITLE 1,Monitor campaign performance
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/ad_campaign_dashboard_hightouch.png" width="70%" style="float:right"/>
# MAGIC 
# MAGIC [Link to Dashboard](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/3c27e852-ca5e-40fc-a701-f8c5e580ad19?o=2894723222787945)
