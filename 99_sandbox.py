# Databricks notebook source
# MAGIC %md
# MAGIC ## Demo Overview
# MAGIC * **Background:** Everyday, data practicioners acrsos the globe visit Snowplow's website to learn how the leader in data creation helps organizations create and maintain high quality and privacy-compliant first party data. While some of these visitors request a demo instantly, others remain in the awareness stage of the marketing funnel for some time.
# MAGIC 
# MAGIC * **Business Objective:** Convert prospects from awareness to engaged users (request for demo).
# MAGIC 
# MAGIC * **Marketing Strategy:** Focus remarketing efforts on prospects with a hight propensity to convert.
# MAGIC 
# MAGIC * **Audience Segmentation:**
# MAGIC   * Heuristic-Driven
# MAGIC   * ML-Driven

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composable CDP
# MAGIC 
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

# MAGIC %md
# MAGIC ### Workflow Overview
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/composable_cdp_new.png" width="50%" style="float: right" />
# MAGIC 
# MAGIC Steps
# MAGIC * **Step 1.** Create of rich behavioural data from testing data product 
# MAGIC * **Step 2.** Apply enrichments features to enhance the data and select snowplow web dbt package
# MAGIC * **Step 3.** Run out-of-the-box DBT web package to model the raw data into AI and BI ready consumption
# MAGIC * **Step 4.** Select initial features to run the Propensity model and enrich the dataset with the Propensity score
# MAGIC * **Step 5.** Via Audience builder, select from the table HighPropensity table visitors for *Awareness* and *Engagement* camapaigns
# MAGIC * **Step 6.** Run personalised marketing campaigns for the selected cohorts
# MAGIC * **Step 7.** Measure the performance of the cohort and other web analytics performance indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load atomic event data into Databricks using Snowplow's RDB loader

# COMMAND ----------

# MAGIC %md
# MAGIC * Setting up Databricks Loader for Snowplow: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-rdb-loader-3-0-0/loading-transformed-data/databricks-loader
# MAGIC * GIT Repo: https://github.com/snowplow/snowplow-rdb-loader

# COMMAND ----------

# DBTITLE 1,View atomic event data
# MAGIC %sql
# MAGIC select * from snowplow_samples.snowplow.events limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create silver tables using Snowplow's DBT package

# COMMAND ----------

# MAGIC %md
# MAGIC From the query above we can see it is not easy for someone who doesn't know the atomic events table well to get the answers they need from the data. Querying the atomic events table also requires more compute so ends up being more expensive. We need to flatten these columns and aggregate the events into useful, analytics ready tables.
# MAGIC 
# MAGIC To do this we can use Snowplow's dbt web package to transform and aggregate the raw web data into a set of derived **Gold** tables straight out of the box:
# MAGIC * `page_views`
# MAGIC * `sessions`
# MAGIC * `users`
# MAGIC 
# MAGIC Latest version of the DBT Web Package: https://hub.getdbt.com/snowplow/snowplow_web/latest/
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/snowplow_web_model_dag.png" width="40%">
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
# MAGIC 
# MAGIC **Resources:**
# MAGIC - Check out all Snowplow's DBT packages (for mobile, video, web): https://hub.getdbt.com/snowplow/
# MAGIC - How to enable DBT for your Databricks project (if not via Partner Connect): https://github.com/databricks/dbt-databricks

# COMMAND ----------

# DBTITLE 1,View page views silver table
# MAGIC %sql

# COMMAND ----------

# DBTITLE 1,View sessions silver table


# COMMAND ----------

# DBTITLE 1,View users silver table


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Perform Exploratory Data Analysis using Databricks SQL

# COMMAND ----------

# MAGIC %md
# MAGIC See DBSQL [Snowplow Website Insights](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/d98ec601-48c1-4f28-a06e-b8c75e118147-snowplow-website-insights?o=2894723222787945) dashboard to view some web analytics built on top of Snowplow's derived tables.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/dashboard_screenshot_1.png" width="70%">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Use Hightouch for heuristic-driven remarketing

# COMMAND ----------

# DBTITLE 1,Configure Partner Connect
# MAGIC %md
# MAGIC 
# MAGIC ## Connect Hightough and Databricks via PartnerConnect
# MAGIC 
# MAGIC We can easily connect to Hightouch from Databricks using [Partner Connect](https://dbc-dcab5385-51e3.cloud.databricks.com/partnerconnect?o=2894723222787945):
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_partner_connect.png" width="25%">
# MAGIC 
# MAGIC Once setup we can see our Databricks cluster in the [Sources](https://app.hightouch.com/snowplow-yzw4c/sources) tab in Hightouch:
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_sources.png" width="25%">

# COMMAND ----------

# DBTITLE 1,Create model for ___ table


# COMMAND ----------

# DBTITLE 1,Create audience segment
# MAGIC %md
# MAGIC 
# MAGIC Create an [Engagement Users](https://app.hightouch.com/snowplow-yzw4c/audiences/591474) audience based on the following rules:
# MAGIC 
# MAGIC - Returned (2 sessions)
# MAGIC - Had intent to engage within the last 30 days
# MAGIC 
# MAGIC To flag if a user had intent to engage we see if they had viewed one of the [get-started](https://snowplowanalytics.com/get-started/) pages on Snowplow's website. We can make this an Audience Event using the following query based on our `snowplow_web_page_views` table:
# MAGIC 
# MAGIC ```sql
# MAGIC select 
# MAGIC   domain_userid,
# MAGIC   page_view_id,
# MAGIC   start_tstamp
# MAGIC from dbt_cloud_derived.snowplow_web_page_views
# MAGIC where page_urlpath like '/get-started/%'
# MAGIC ```
# MAGIC 
# MAGIC After creating the event (see set up [here](https://app.hightouch.com/snowplow-yzw4c/audiences/setup/events/591234)), we need to add a direct relationship between this and our *All Users* parent model.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_direct_relationship.png" width="35%">
# MAGIC 
# MAGIC We can now use this event as a filter when we build our Engagement Users audience:
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_engagement_users_audience_builder.png" width="35%">

# COMMAND ----------

# DBTITLE 1,Set up destination sync
# MAGIC %md
# MAGIC After setting up Braze as a [destination](https://app.hightouch.com/snowplow-yzw4c/destinations) in Hightouch, we can sync up our new audiences. In this case we want to sync these audiences to our *Awareness Users* and *Engagement Users* Braze subscription groups.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_configure_braze.png" width="40%">

# COMMAND ----------

# DBTITLE 1,Orchestrate an auto-update of audiences
# MAGIC %md
# MAGIC It is important that our audiences connected to third party tools like Braze are always up to date and in sync with our Snowplow web data in Databricks. 
# MAGIC 
# MAGIC We can ensure this by using the [dbt Cloud extension](https://app.hightouch.com/snowplow-yzw4c/extensions) to trigger syncs after the dbt Snowplow web model job finishes and our Gold tables are updated.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/hightouch_dbt_schedule.png" width="30%">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Train propensity to convert model using XGBoost and MLflow

# COMMAND ----------

# DBTITLE 1,Create train and test data sets
features =['device_w','device_connectiontype','device_devicetype','device_lat','imp_bidfloor']
 
df = spark.table('field_demos_media.rtb_dlt_bids_gold').toPandas()
X_train, X_test, y_train, y_test = train_test_split(df[features], df['in_view'], test_size=0.33, random_state=55)

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
model_registered = mlflow.register_model("runs:/"+run_id+"/model", "field_demos_rtb")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_rtb", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 6: Use model to predict propensity to convert
# MAGIC 
# MAGIC PLACEHOLDER: workflow image
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
# MAGIC In the following cell, we'll focus on deploying the model in this notebook directly

# COMMAND ----------

# DBTITLE 1,Load the model from registry as UDF
#                                 Stage/version
#                       Model name       |
#                           |            |
model_path = 'models:/field_demos_rtb/Production'
predict_in_view = mlflow.pyfunc.spark_udf(spark, model_path, result_type = DoubleType())


# COMMAND ----------

# DBTITLE 1,Perform inference
model_features = predict_in_view.metadata.get_input_schema().input_names()
new_df = spark.table('field_demos_media.rtb_dlt_bids_gold').select(*model_features)
display(
  new_df.withColumn('in_view_prediction', predict_in_view(*model_features)).filter(col('in_view_prediction') == 1)
)

# COMMAND ----------

# DBTITLE 1,Save propensity to convert predictions


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Use Hightouch for ML-driven remarketing

# COMMAND ----------

# DBTITLE 1,Create model for __ table


# COMMAND ----------

# DBTITLE 1,Create audience segment


# COMMAND ----------

# DBTITLE 1,Set up destination sync


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Compare campaign performance: Heuristic-driven vs. ML-driven

# COMMAND ----------


