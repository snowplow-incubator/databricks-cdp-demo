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

# MAGIC %pip install mlflow lightgbm imblearn

# COMMAND ----------

import pandas as pd
import lightgbm as lgb
from imblearn.over_sampling import SMOTENC

# Get list of a users features based on first touch model
df = spark.sql(
"""
-- Get additional features from the user's first page view
with pv as (select domain_userid, absolute_time_in_s, vertical_percentage_scrolled, geo_country,
                   geo_region, br_lang, spider_or_robot, operating_system_class, operating_system_name,
                   device_family, os_family, row_number() over (partition by domain_userid order by start_tstamp) as rn
            from dbt_cloud_derived.snowplow_web_page_views
            where page_view_in_session_index = 1 and start_tstamp_date >= '2022-08-30'
            qualify rn = 1)
select u.start_tstamp, u.domain_userid, u.first_page_title, u.first_page_urlhost, u.first_page_urlpath,
       u.refr_urlhost, u.refr_medium, u.refr_term, u.mkt_medium, u.mkt_source, u.mkt_term,
       u.mkt_campaign, u.mkt_content, u.mkt_network, u.engaged_time_in_s, dayofweek(u.start_tstamp) as day_of_week,  
       hour(u.start_tstamp) as hour, pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country,
       pv.geo_region, pv.br_lang, pv.spider_or_robot, pv.operating_system_class, pv.operating_system_name,
       pv.device_family, pv.os_family, ifnull(c.converted, false) as converted_user
from dbt_cloud_derived.snowplow_web_users u
     join pv on u.domain_userid = pv.domain_userid
     left join default.converted_users c using(domain_userid)
     where u.start_tstamp_date >= '2022-08-30'
""").toPandas()

ref_cols = ["refr_urlhost", "refr_medium", "refr_term"]
mkt_cols = ["mkt_medium", "mkt_source", "mkt_term", "mkt_campaign", "mkt_content", "mkt_network"]
geo_cols = ["geo_country", "geo_region", "br_lang"]
dev_cols = ["device_family", "os_family", "operating_system_class", "operating_system_name"]
url_cols = ["first_page_title"]
robot_cols = ["spider_or_robot"]
calendar_cols = ["day_of_week", "hour"]
engagement_cols = ["engaged_time_in_s", "absolute_time_in_s", "vertical_percentage_scrolled"]

discrete_col = ref_cols + mkt_cols + geo_cols + dev_cols +  calendar_cols + url_cols
continues_col = engagement_cols

all_features = discrete_col + continues_col

# Input missing data
for col in discrete_col:
    df[col].fillna("N/A", inplace=True)
for col in continues_col:
    df[col].fillna(df[col].mean(), inplace=True)

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
# MAGIC Reduce number of categorical features so `SMOTENC` doesn't run out of memory. 

# COMMAND ----------

from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np

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
# MAGIC Oversampling the data set is required due to extreme class imbalance - `0.00043`. `imbalanced-learn` SMOTENC was chosen because data contrains many categorical features.

# COMMAND ----------

cat_index = [ pd.Index(all_features).get_loc(col) for col in discrete_col ] 
df_train, df_test = df.iloc[:df.shape[0]//10*8,:],  df.iloc[df.shape[0]//10*8:,:]
smote_nc = SMOTENC(categorical_features=cat_index, k_neighbors=5,  random_state=0, n_jobs=-1)
topk = TakeTopK(50)
X_res, y_res = smote_nc.fit_resample(topk.fit_transform(df_train[all_features]), df_train.converted_user)

topk.transform(df_test[all_features]).head()

# COMMAND ----------

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

import mlflow
mlflow.sklearn.log_model(pipeline, "sklearn_lgbm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SHAP Analysis and Feature Importance of LightGBM Model:
# MAGIC **Expainability is key:**
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/lgbm_shap_analysis.png" width="50%">
# MAGIC <img src="https://raw.githubusercontent.com/snowplow-incubator/databricks-cdp-demo/main/assets/lgbm_feature_importance.png" width="30%">
# MAGIC 
# MAGIC This shows the importance of the engagement metrics in predicting conversion and that engagement contributes to 25% of the overall model performance.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Using our Model for Inference and Prediction
# MAGIC Once we have deployed our model using MLflow we can start offline (batch and streaming) inference and online (real-time) serving.
# MAGIC 
# MAGIC In this example we use the model to predict on our `snowplow_web_users` table and return a propensity score for each user who has visited the site.

# COMMAND ----------

import mlflow
import pandas as pd

logged_model = 'runs:/0c437c842261403dba6d923a1a9b8257/sklearn_lgbm'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
df = spark.sql(
"""
with pv as (select domain_userid, absolute_time_in_s, vertical_percentage_scrolled, geo_country,
                   geo_region, br_lang, spider_or_robot, operating_system_class, operating_system_name,
                   device_family, os_family, row_number() over (partition by domain_userid order by start_tstamp) as rn
            from dbt_cloud_derived.snowplow_web_page_views
            where page_view_in_session_index = 1 and start_tstamp_date >= '2022-08-30'
            qualify rn = 1)
select u.start_tstamp, u.domain_userid, u.first_page_title, u.first_page_urlhost, u.first_page_urlpath,
       u.refr_urlhost, u.refr_medium, u.refr_term, u.mkt_medium, u.mkt_source, u.mkt_term,
       u.mkt_campaign, u.mkt_content, u.mkt_network, u.engaged_time_in_s, dayofweek(u.start_tstamp) as day_of_week,  
       hour(u.start_tstamp) as hour, pv.absolute_time_in_s, pv.vertical_percentage_scrolled, pv.geo_country,
       pv.geo_region, pv.br_lang, pv.spider_or_robot, pv.operating_system_class, pv.operating_system_name,
       pv.device_family, pv.os_family
from dbt_cloud_derived.snowplow_web_users u
     join pv on u.domain_userid = pv.domain_userid
     where u.start_tstamp_date >= '2022-08-30'
"""
).toPandas()
df["propensity_score"] = loaded_model.predict(df)

# Add propensity deciles and save to table
df["propensity_decile"] = pd.qcut(df["propensity_score"], 10, labels=False)

df_spark = spark.createDataFrame(df[["domain_userid", "propensity_score", "propensity_decile"]])
df_spark.write.mode("overwrite").saveAsTable("default.snowplow_user_propensity_scores")

df.head()

# COMMAND ----------

# DBTITLE 1,User Propensity Scores
import plotly.express as px

fig = px.histogram(df, x="propensity_score", nbins=50)
fig.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next Step:
# MAGIC 
# MAGIC We have utilised Snowplow's rich behavioural data in this model to generate accurate propensity to engage scores. These can now be used when activating our data to improve audience segmentation and maximise conversions.
