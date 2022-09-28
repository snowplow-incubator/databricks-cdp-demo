# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Data Storage & Modeling using Databricks DeltaLake and MLFlow
# MAGIC 
# MAGIC In this notebook we will be modeling and exploring behavioural data collected by Snowplow's Javascript tracker from Snowplow's [snowplow.io](https://snowplow.io/) website in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Atomic Events BRONZE Table stored in DeltaLake
# MAGIC 
# MAGIC All events are loaded using Snowplow's RDB loader into a single atomic events table backed by Databricks’ Delta tables. We call this a “Wide-row Table” – with one row per event, and one column for each type of entity/property and self-describing event.

# COMMAND ----------

# DBTITLE 1,atomic.events
# MAGIC %sql
# MAGIC select * from snowplow_samples.snowplow.events limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Create SILVER tables using Snowplow's dbt package
# MAGIC 
# MAGIC Snowplow's out-of-the-box [snowplow-web dbt package](https://hub.getdbt.com/snowplow/snowplow_web/latest/) models aggregate the data to different levels of altitude, so data scientists can directly consume the right altitude for their model. For composable CDP use cases, we're typically making predictions at the user-level. However, it is valuable to be able to drill down into the session, page view and event level, to understand where the user is in their journey and how that is changing over time.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_web_model_dag.png" width="40%">
# MAGIC 
# MAGIC 
# MAGIC ### The dbt package:
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
# MAGIC Easily setup yout dbt Cloud connection using Databricks' [Partner Connect](https://www.databricks.com/blog/2022/04/14/launching-dbt-cloud-in-databricks-partner-connect.html).
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
# MAGIC 
# MAGIC **Resources:**
# MAGIC - Check out all Snowplow's DBT packages (for mobile, video, web): https://hub.getdbt.com/snowplow/
# MAGIC - How to connect to dbt core (if not via Partner Connect): https://docs.databricks.com/integrations/prep/dbt.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Gain insights from SILVER tables using Databricks SQL Dashboards
# MAGIC 
# MAGIC This example DBSQL [Snowplow Website Insights](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/d98ec601-48c1-4f28-a06e-b8c75e118147-snowplow-website-insights?o=2894723222787945) dashboard shows how you can gain insightfull web analytics building on top of Snowplow's dbt package modelled tables.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/dashboard_screenshot_1.png" width="70%">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.4 Create Propensity to Convert ML Model
# MAGIC  
# MAGIC This propensity to engage solution provides a very flexible way to identify who among your audience is most likely to actually engage with you, for example request a demo / sign up for a trial, purchase a first service/product, request an upgrade, accept an offer etc… 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.1. Configuration
# MAGIC 
# MAGIC Please use a cluster with **11.2 ML CPU** Runtime.

# COMMAND ----------

# DBTITLE 1,Import libraries
import re
import mlflow
import pandas as pd
from sklearn.model_selection import cross_val_score, RepeatedStratifiedKFold, train_test_split
from sklearn.metrics import average_precision_score, f1_score
from hyperopt import fmin, tpe, rand, hp, Trials, STATUS_OK, SparkTrials, space_eval
from mlflow.models.signature import infer_signature
from xgboost import XGBClassifier
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType
from hyperopt.pyll.base import scope
import pyspark.sql.functions as f 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.3. Create GOLD user features table from Snowplow derived tables
# MAGIC 
# MAGIC Primary features returned from the Snowplow dbt web model can be grouped into categories based on their origin:
# MAGIC 
# MAGIC * **Temporal** – created from first event timestamp: an hour of the day, day of the week.
# MAGIC * **Landing Page** – page title of the first URL, comes out of the box
# MAGIC * **Device** –  User Agent enrichment
# MAGIC * **Referral** – Referral enrichment
# MAGIC * **Marketing** –  Marketing campaign enrichment
# MAGIC * **Geographic** – IP lookup enrichment
# MAGIC * **Engagement** – Accumulated page ping events by dbt page view model
# MAGIC 
# MAGIC Explore and collect these user features in a new view or table based on their first website visit. Include a column for your conversion flag so we are ready to train and test our models. Conversion can be derived from a Snowplow tracked event or using other sources like Salesforce data. 
# MAGIC In this example we are joining onto a `converted_users` table, which contains a list of all users that have converted.

# COMMAND ----------

# DBTITLE 1,Construct GOLD table
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW gold_view AS (
# MAGIC   WITH pv AS (
# MAGIC     SELECT domain_userid, absolute_time_in_s, vertical_percentage_scrolled, geo_country, geo_region, br_lang, 
# MAGIC     device_family, os_family, row_number() OVER (PARTITION BY domain_userid ORDER BY start_tstamp) AS rn
# MAGIC     FROM snowplow_samples.dbt_cloud_derived.snowplow_web_page_views
# MAGIC     WHERE page_view_in_session_index = 1 
# MAGIC     QUALIFY rn = 1)
# MAGIC 
# MAGIC 
# MAGIC   SELECT u.domain_userid, u.first_page_title, u.refr_urlhost, lower(u.refr_medium) as refr_medium,
# MAGIC   lower(u.mkt_medium) as mkt_medium, u.mkt_source, u.mkt_term, u.mkt_campaign, u.engaged_time_in_s, 
# MAGIC   u.sessions, u.page_views, pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country,
# MAGIC   pv.geo_region, pv.br_lang, pv.device_family, pv.os_family, int(ifnull(c.converted, false)) as converted_user
# MAGIC   FROM snowplow_samples.dbt_cloud_derived.snowplow_web_users u
# MAGIC   JOIN pv ON u.domain_userid = pv.domain_userid
# MAGIC   AND pv.rn = 1
# MAGIC   LEFT JOIN snowplow_samples.samples.converted_users c USING(domain_userid)
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Persist GOLD table
df = spark.sql("select * from gold_view").na.fill(0)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("snowplow_samples.samples.snowplow_website_users_first_touch_gold")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4.4. Train model using XGBoost and MLflow
# MAGIC 
# MAGIC Here we have just selected a few of the features Snowplow behavioural data has to offer to train our model. You'd extend this with more and more columns as you start using ML to predict more and more types of user behavior, building out a richer view of each of your customers / users. 

# COMMAND ----------

# DBTITLE 1,Create train and test data sets
df = spark.table("snowplow_samples.samples.snowplow_website_users_first_touch_gold").toPandas()
# Select columns we want to use for the model from our Gold user table
df = df[["engaged_time_in_s", "absolute_time_in_s", "vertical_percentage_scrolled", "refr_medium", "mkt_medium", "converted_user"]]
df = pd.get_dummies(df,columns=['refr_medium','mkt_medium'],dtype='int64')
features = [i for i in list(df.columns) if i != 'converted_user']
X_train, X_test, y_train, y_test = train_test_split(df[features], df["converted_user"], test_size=0.33, random_state=55)

# COMMAND ----------

# DBTITLE 1,Define model evaluation for hyperopt
def evaluate_model(params):
  #instantiate model
  model = XGBClassifier(use_label_encoder=False,
                            learning_rate=params["learning_rate"],
                            gamma=int(params["gamma"]),
                            reg_alpha=int(params["reg_alpha"]),
                            reg_lambda=int(params["reg_lambda"]),
                            max_depth=int(params["max_depth"]),
                            n_estimators=int(params["n_estimators"]),
                            min_child_weight = params["min_child_weight"], objective='binary:logistic', early_stopping_rounds=50)
  
  #train
  model.fit(X_train, y_train)
  
  #predict
  y_prob = model.predict_proba(X_test)
  y_pred = model.predict(X_test)
  
  #score
  precision = average_precision_score(y_test, y_prob[:,1])
  f1 = f1_score(y_test, y_pred)
  
  mlflow.log_metric('avg_precision', precision)  # record actual metric with mlflow run
  mlflow.log_metric('avg_f1', f1)  # record actual metric with mlflow run
  
  # return results (negative precision as we minimize the function)
  return {'loss': -f1, 'status': STATUS_OK, 'model': model}

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
  signature = infer_signature(X_test, model.predict(X_test))
  mlflow.sklearn.log_model(trials.best_trial['result']['model'], 'model', signature=signature, input_example=X_test.iloc[0].to_dict())
  #add hyperopt model params
  for p in argmin:
    mlflow.log_param(p, argmin[p])
  mlflow.log_metric("avg_f1_score", trials.best_trial['result']['loss'])
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

# MAGIC %md
# MAGIC ### 2.4.5. Use model to predict propensity to convert
# MAGIC 
# MAGIC Now that our model is built and saved in MLFlow registry, we can load it to run our inferences at scale.
# MAGIC 
# MAGIC This can be done:
# MAGIC 
# MAGIC * In batch or streaming (ex: refresh every night)
# MAGIC   * Using a standard notebook job
# MAGIC   * Or as part of the DLT pipeline we built
# MAGIC * In real-time over a REST API, deploying Databricks serving capabilities
# MAGIC 
# MAGIC In the following cells, we'll focus on deploying the model in this notebook directly

# COMMAND ----------

# DBTITLE 1,Load the model from registry as UDF
#                                 Stage/version
#                       Model name       |
#                           |            |
model_path = 'models:/field_demos_ccdp/Production'
predict_propensity = mlflow.pyfunc.spark_udf(spark, model_path, result_type='float')


# COMMAND ----------

# DBTITLE 1,Perform inference
from pyspark.sql.functions import struct

model_features = predict_propensity.metadata.get_input_schema().input_names()

users_gold = spark.table('snowplow_samples.samples.snowplow_website_users_first_touch_gold')
df = pd.get_dummies(users_gold.toPandas(),columns=['refr_medium','mkt_medium'],dtype='int64')
df = spark.createDataFrame(df)

predictions = df.withColumn('propensity_prediction', predict_propensity(struct(*model_features)))

display(predictions)

# COMMAND ----------

display(predictions.groupBy('propensity_prediction').count())

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.5. Create a table of high propensity website visitors
# MAGIC 
# MAGIC We have utilised Snowplow's rich behavioural data in this model to generate accurate propensity to engage scores. These can now be used when activating our data to improve audience segmentation and maximise conversions.

# COMMAND ----------

high_propensity_web_users = predictions.select('domain_userid', 'propensity_prediction').where(predictions.propensity_prediction == 1)
high_propensity_web_users.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("snowplow_samples.samples.high_propensity_web_users") 
