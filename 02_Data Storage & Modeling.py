# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Data Storage & Modeling using Databricks DeltaLake and MLFlow
# MAGIC 
# MAGIC In this notebook we will be modeling and exploring behavioural data collected by Snowplow's Javascript tracker from Snowplow's [snowplow.io](https://snowplow.io/) website in Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Atomic Events Table stored in DeltaLake storage (BRONZE)
# MAGIC 
# MAGIC All events are loaded using Snowplow's RDB loader into a single atomic events table backed by Databricks’ Delta tables. We call this a “Wide-row Table” – with one row per event, and one column for each type of entity/property and self-describing event.

# COMMAND ----------

# DBTITLE 1,atomic.events
# MAGIC %sql
# MAGIC select * from snowplow_samples.snowplow.events limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Create Derived Tables using dbt (Gold - Analytics Ready)
# MAGIC 
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

# MAGIC %md
# MAGIC ## 2.3 Exploratory Data Analysis of derived tables using Databricks SQL Dashboard
# MAGIC 
# MAGIC See DBSQL [Snowplow Website Insights](https://dbc-dcab5385-51e3.cloud.databricks.com/sql/dashboards/d98ec601-48c1-4f28-a06e-b8c75e118147-snowplow-website-insights?o=2894723222787945) dashboard to view some web analytics built on top of Snowplow's derived tables.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/dashboard_screenshot_1.png" width="70%">

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.4 Create Propensity to Convert ML Model creation

# COMMAND ----------

# MAGIC %md 
# MAGIC This propensity to engage solution provides a very flexible way to identify who among your audience is most likely to actually engage with you, for example request a demo / sign up for a trial, purchase a first service/product, request an upgrade, accept an offer etc… 
# MAGIC 
# MAGIC This specific demo explores the impact of measuring behaviour data during initial contact with the web-site [first touch]. With the goal of predicting which users are going to fill out the demo request form based on their first visit on the demo website.
# MAGIC 
# MAGIC Although first touch attribution is less common than final touch or multi touch approach. It is still a useful tool for companies that want to build brand awareness or have a short sales cycle. Impact of other visitor information is constrained to the geographic and time properties, due to limitations by platforms such as Google Ad Campaign. Other factors contributing to the conversion are omitted.
# MAGIC 
# MAGIC Snowplow web tracking and modelling provides engagement metrics out of the box like:
# MAGIC * How much page is scrolled in
# MAGIC * Time spent engaged with the page
# MAGIC * How long tab was open and active in browser
# MAGIC * and many more...

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2.4.1. Feature exploration from the data
# MAGIC Primary features returned from the Snowplow dbt web model can be grouped into categories based on their origin:
# MAGIC 
# MAGIC * **Temporal** – created from first event timestamp: an hour of the day, day of the week.
# MAGIC * **Landing Page** – page title of the first URL, comes out of the box
# MAGIC * **Device** –  User Agent enrichment
# MAGIC * **Referral** – Referral enrichment
# MAGIC * **Marketing** –  Marketing campaign enrichment
# MAGIC * **Geographic** – IP lookup enrichment
# MAGIC * **Engagement** – Accumulated page ping events by dbt page view model

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.2. Installing the MLFlow

# COMMAND ----------

# DBTITLE 1,Installing MLFlow
# MAGIC %pip install mlflow lightgbm imblearn

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.3. Get user features from Snowplow derived tables

# COMMAND ----------

# DBTITLE 1,Get user features from Snowplow derived tables
import pandas as pd
import lightgbm as lgb
from imblearn.over_sampling import SMOTENC

# Get list of a users features based on first touch model
df = spark.sql(
"""
-- Get additional features from the user's first page view
with pv as (select domain_userid, absolute_time_in_s, vertical_percentage_scrolled,
                   geo_country, geo_region, br_lang, device_family, os_family,
                   row_number() over (partition by domain_userid order by start_tstamp) as rn
            from snowplow_samples.dbt_cloud_derived.snowplow_web_page_views
            where page_view_in_session_index = 1
                        qualify rn = 1)
                        
select u.domain_userid, u.first_page_title, u.refr_urlhost, u.refr_medium,
       u.mkt_medium, u.mkt_source, u.mkt_term, u.mkt_campaign, u.engaged_time_in_s,
       pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country,
       pv.geo_region, pv.br_lang, pv.device_family, pv.os_family,
       ifnull(c.converted, false) as converted_user
from snowplow_samples.dbt_cloud_derived.snowplow_web_users u
     join pv on u.domain_userid = pv.domain_userid and pv.rn = 1
     left join snowplow_samples.samples.converted_users c using(domain_userid)
""").toPandas()

ref_cols = ["refr_urlhost", "refr_medium"]
mkt_cols = ["mkt_medium", "mkt_source", "mkt_term"]
geo_cols = ["geo_country", "geo_region", "br_lang"]
dev_cols = ["device_family", "os_family"]
url_cols = ["first_page_title"]
engagement_cols = ["engaged_time_in_s", "absolute_time_in_s", "vertical_percentage_scrolled"]

discrete_col = ref_cols + mkt_cols + geo_cols + dev_cols + url_cols
continues_col = engagement_cols

all_features = discrete_col + continues_col

# Input missing data
for col in discrete_col:
    df[col].fillna("N/A", inplace=True)
for col in continues_col:
    df[col].fillna(df[col].mean(), inplace=True)
    
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.4. Model Selection and Performance
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
# MAGIC 
# MAGIC 
# MAGIC **Resources:** 
# MAGIC - Full LightGBM Documentation: https://lightgbm.readthedocs.io/en/v3.3.2/
# MAGIC - Parallelized LightGBM with Spark migration tips: https://github.com/microsoft/SynapseML/issues/889

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.5. Reduce number of categorical features so `SMOTENC` doesn't run out of memory. 

# COMMAND ----------

# DBTITLE 1,Reduce number of categorical features so `SMOTENC` doesn't run out of memory. 
from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np

# Reduce number of categorical features so `SMOTENC` doesn't run out of memory. 
class TakeTopK(BaseEstimator, TransformerMixin):
    def __init__(self, k=20):
        self.largest_cat = {}
        self.k = k
        
    def fit(self, X, y=None):
        for col in discrete_col:
            self.largest_cat[col] = df[col].value_counts().nlargest(self.k).index
        return self
    
    def transform(self, X, y=None):
        Xt = pd.DataFrame()
        for col in discrete_col:
            Xt[col] = pd.Series(np.where(X[col].isin(self.largest_cat[col]), X[col], 'Other'), dtype='category')
        Xt[continues_col] = X[continues_col].astype(float)
        return Xt

# COMMAND ----------

# MAGIC %md
# MAGIC Oversampling the data set is required due to extreme class imbalance. `imbalanced-learn` SMOTENC was chosen because data contrains many categorical features.

# COMMAND ----------

cat_index = [ pd.Index(all_features).get_loc(col) for col in discrete_col ] 
df_train, df_test = df.iloc[:df.shape[0]//10*8,:],  df.iloc[df.shape[0]//10*8:,:]
smote_nc = SMOTENC(categorical_features=cat_index, k_neighbors=5,  random_state=0, n_jobs=-1)
topk = TakeTopK(50)
X_res, y_res = smote_nc.fit_resample(topk.fit_transform(df_train[all_features]), df_train.converted_user)

# COMMAND ----------

# MAGIC %md
# MAGIC **Resource:** 
# MAGIC - Learn more about SMOTENC: https://medium.com/analytics-vidhya/smote-nc-in-ml-categorization-models-fo-imbalanced-datasets-8adbdcf08c25

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.6. Run LightGBM Model

# COMMAND ----------

# DBTITLE 1,Run LightGBM Model
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay, fbeta_score
from sklearn.pipeline import Pipeline


pipeline = Pipeline([
    ('top_20', TakeTopK(50)),
    ('clf', lgb.LGBMModel(n_jobs=-1, metric='binary_logloss', objective='binary', min_child_samples= 10, n_estimators=100, num_leaves= 50, scale_pos_weight=2))    
])
pipeline.fit(X_res, y_res.astype(int))

y_pred = np.where(pipeline.predict(df_test[all_features]) > 0.5, 1, 0)
cm = confusion_matrix(df_test['converted_user'].fillna(False),   y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['not converted', 'converted'])
print(f"model score {fbeta_score(df_test['converted_user'].fillna(False),  y_pred, beta=2)}")
print(classification_report(df_test['converted_user'].fillna(False),   y_pred))
disp.plot()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.7. Store the model

# COMMAND ----------

# DBTITLE 1,Store the trained model in Databricks project
import mlflow

# Log model
mlflow.sklearn.log_model(pipeline, "sklearn_lgbm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4.8. Feature Importance for the model performance:
# MAGIC **Expainability is key:** We can see the importance of the engagement metrics in predicting conversion. 
# MAGIC 
# MAGIC **The presence of user behavioural metrics like 
# MAGIC  - Vertical Scrolling (on the pages)
# MAGIC  - Engaged time (on the pages)
# MAGIC  - Absolute time (spent on the website) 
# MAGIC  
# MAGIC  support the model performance from 40%**

# COMMAND ----------

lgb.plot_importance(pipeline.steps[1][1], max_num_features=15)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2.4.9. Run the Prediction
# MAGIC Once we have deployed our model using MLflow we can start offline (batch and streaming) inference and online (real-time) serving.
# MAGIC 
# MAGIC In this example we use the model to predict and return a propensity score for users who has visited the site.

# COMMAND ----------

# DBTITLE 0,Final table with High Propensity visitors for the activation
import mlflow
import pandas as pd

def p_label(x):
    """ Assign a propensity label based on propensity deciles
        Low is lowest 80% of scores, Medium 80-90% of scores, High is top 10% of scores
    """
    if x <= 7:
        l = "Low"
    elif x <= 8:
        l = "Medium"
    else:
        l = "High"
    return l


logged_model = 'runs:/d8faf9093a044ac5b6ee1c051698db16/sklearn_lgbm'

# Load model as a PyFuncModel
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on Snowplow users
df = spark.sql(
"""
-- Get additional features from the user's first page view
with pv as (select domain_userid, absolute_time_in_s, vertical_percentage_scrolled,
                   geo_country, geo_region, br_lang, device_family, os_family,
                   row_number() over (partition by domain_userid order by start_tstamp) as rn
            from snowplow_samples.dbt_cloud_derived.snowplow_web_page_views
            where page_view_in_session_index = 1
                        qualify rn = 1)
                        
select u.domain_userid, u.first_page_title, u.refr_urlhost, u.refr_medium,
       u.mkt_medium, u.mkt_source, u.mkt_term, u.mkt_campaign, u.engaged_time_in_s,
       pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country,
       pv.geo_region, pv.br_lang, pv.device_family, pv.os_family
from snowplow_samples.dbt_cloud_derived.snowplow_web_users u
     join pv on u.domain_userid = pv.domain_userid and pv.rn = 1
""").toPandas()

df["propensity_score"] = loaded_model.predict(df)

# Add propensity deciles and labels then save to table
df["propensity_decile"] = pd.qcut(df["propensity_score"], 10, labels=False)
df["propensity_label"] = [p_label(x) for x in df["propensity_decile"]]

df_spark = spark.createDataFrame(df[["domain_userid", "propensity_score", "propensity_decile", "propensity_label"]])
df_spark.write.mode("overwrite").saveAsTable("snowplow_samples.samples.snowplow_user_propensity_scores")
df.head()

# COMMAND ----------

# DBTITLE 1,User Propensity Score Distribution
import plotly.express as px

fig = px.histogram(df, x="propensity_score", color="propensity_label", nbins=100, log_y=True)
fig.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.5. Create a table of visitors with High Propensity to Engage
# MAGIC 
# MAGIC We have utilised Snowplow's rich behavioural data in this model to generate accurate propensity to engage scores. These can now be used when activating our data to improve audience segmentation and maximise conversions.

# COMMAND ----------

df[df["propensity_label"] == "High"].sort_values(by=["propensity_score"], ascending=False).head(10)
