# Databricks notebook source
# MAGIC %md 
# MAGIC # Propensity to Engage ML Model

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/composable_cdp_architecture.png" width="80%">

# COMMAND ----------

# MAGIC %md 
# MAGIC A propensity to engage solution provides a very flexible way to identify who among your audience is most likely to actually engage with you, for example request a demo / sign up for a trial, purchase a first service/product, request an upgrade, accept an offer etc… 
# MAGIC 
# MAGIC This specific demo explores the impact of measuring behaviour data during initial contact with the web-site. With the goal of predicting which users are going to fill out the demo request form based on their first visit on [snowplow.io](https://snowplow.io/) website.
# MAGIC 
# MAGIC Although first touch attribution is less common than final touch or multi touch approach. It is still a useful tool for companies that want to build brand awareness or have a short sales cycle. Impact of other visitor information is constrained to the geographic and time properties, due to limitations by platforms such as Google Ad Campaign. Other factors contributing to the conversion are omitted.
# MAGIC 
# MAGIC Snowplow web tracking and modelling provides engagement metrics out of the box like:
# MAGIC * How much page is scrolled in
# MAGIC * Time spent engaged with the page
# MAGIC * How long tab was open in browser

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Dataset
# MAGIC Primary features returned from the Snowplow dbt web model can be grouped into categories based on their origin:
# MAGIC 
# MAGIC * **Temporal** – created from first event timestamp: an hour of the day, day of the week.
# MAGIC * **Landing Page** – page title of the first URL, comes out of the box
# MAGIC * **Device** –  User Agent enrichment
# MAGIC * **Referral** – Referral enrichment
# MAGIC * **Marketing** –  Marketing campaign enrichment
# MAGIC * **Geographic** – IP lookup enrichment
# MAGIC * **Robot** – IAB enrichment
# MAGIC * **Engagement** – Accumulated page ping events by dbt page view model
# MAGIC 
# MAGIC Conversion events are taken from Salesforce, using different tracking methods. However, in practice Snowplow users could send a custom conversion event to avoid joining another data source. Read Snowplow's documentation about setting this up [here](https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/javascript-tracker/javascript-tracker-v3/tracking-events/).
# MAGIC 
# MAGIC Most recent data should not be considered for the performance as some users have not converted yet (the purchase cycle for Snowplow BDP can be long).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Selection and Performance
# MAGIC 
# MAGIC The following classifiers are analysed: Logistic Regression, SVM (with linear kernel) and Gradient Boosting Trees. Other methods were considered - Random Forest, Neural Networks and Sharpie, MCMC, etc. But were excluded for high computational overhead or lack of interpretability. The best suited model for our project was the LGBM.
# MAGIC 
# MAGIC **LightGBM** is a gradient boosting framework that uses tree based learning algorithms. It is designed to be distributed and efficient with the following advantages:
# MAGIC 
# MAGIC * Faster training speed and higher efficiency
# MAGIC * Lower memory usage
# MAGIC * Better accuracy
# MAGIC * Support of parallel, distributed, and GPU learning
# MAGIC * Capable of handling large-scale data

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHAP Analysis and Feature Importance of LightGBM Model:
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/lgbm_shap_analysis.png" width="50%">
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/lgbm_feature_importance.png" width="30%">
# MAGIC 
# MAGIC This shows the importance of the engagement metrics in predicting conversion and that engagement contributes to 25% of the overall model performance.
# MAGIC 
# MAGIC **Expainability is key:** example of trees from the pure LightGBM model
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/lgbm_example_tree.png" width="100%">
# MAGIC 
# MAGIC From this tree you can see the top two features are by far the most important drivers to drive conversion. For our project the inflection points are:
# MAGIC * **Engaged time** 15 seconds
# MAGIC * **Vertical scrolling** 67%

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Deploy Model for Inference and Prediction
# MAGIC Once we have deployed our model using MLflow we can start offline (batch and streaming) inference and online (real-time) serving.
# MAGIC 
# MAGIC In this example we use the model to predict on our `snowplow_web_users` table and return a propensity score for each user who has visited the site.

# COMMAND ----------

import mlflow
import pandas as pd

logged_model = 'runs:/ff37d71514dc4ddc813eec15db28ad82/model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
df = spark.sql("select * from dbt_cloud_derived.snowplow_web_user").toPandas()
df["propensity_score"] = loaded_model.predict(df)

# Add propensity deciles and save to table
df["propensity_decile"] = pd.qcut(df["propensity_score"], 10, labels=False)
df_spark = spark.createDataFrame(df[["domain_userid", "propensity_score", "propensity_decile"]])
df_spark.write.mode("overwrite").saveAsTable("default.snowplow_user_propensity_scores")

# COMMAND ----------

# DBTITLE 1,User Propensity Scores
# MAGIC %sql 
# MAGIC select * from default.snowplow_user_propensity_scores limit 20

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next Step:
# MAGIC 
# MAGIC We have utilised Snowplow's rich behavioural data in this model to generate accurate propensity to engage scores. These can now be used when activating our data to improve audience segmentation and maximise conversions.
