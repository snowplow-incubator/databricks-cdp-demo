# Databricks notebook source
# MAGIC %pip install git+https://github.com/sllynn/spark-xgboost

# COMMAND ----------

import pandas as pd

from sparkxgb import XGBoostClassifier, XGBoostRegressor
from pprint import PrettyPrinter
 
from pyspark.sql.types import StringType
 
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
pp = PrettyPrinter()
 

train_sdf, test_sdf = (
  spark.createDataFrame(pd.read_csv("./sample_propensity_data.csv"))
  .randomSplit([0.8, 0.2])
)

# COMMAND ----------

scale_pos_weight = train_sdf.filter("label = 0").count() / train_sdf.filter("label = 1").count()

# COMMAND ----------

string_columns = [fld.name for fld in train_sdf.schema.fields if isinstance(fld.dataType, StringType)]
string_col_replacements = [fld + "_ix" for fld in string_columns]
string_column_map=list(zip(string_columns, string_col_replacements))
target = "label"
predictors = [fld.name for fld in train_sdf.schema.fields if not isinstance(fld.dataType, StringType)] + string_col_replacements
pp.pprint(
  dict(
    string_column_map=string_column_map,
    target_variable=target,
    predictor_variables=predictors
  )
)
 
si = [StringIndexer(inputCol=fld[0], outputCol=fld[1]) for fld in string_column_map]
va = VectorAssembler(inputCols=predictors, outputCol="features")
pipeline = Pipeline(stages=[*si, va])
fitted_pipeline = pipeline.fit(train_sdf.union(test_sdf))
 
train_sdf_prepared = fitted_pipeline.transform(train_sdf)
train_sdf_prepared.cache()
train_sdf_prepared.count()
 
test_sdf_prepared = fitted_pipeline.transform(test_sdf)
test_sdf_prepared.cache()
test_sdf_prepared.count()
 
xgbParams = dict(
  eta=0.1,
  maxDepth=2,
  missing=0.0,
  objective="binary:logistic",
  numRound=5,
  numWorkers=2,
  scalePosWeight=scale_pos_weight
)
 
xgb = (
  XGBoostClassifier(**xgbParams)
  .setFeaturesCol("features")
  .setLabelCol("label")
)
 
bce = BinaryClassificationEvaluator(
  rawPredictionCol="rawPrediction",
  labelCol="label"
)
 
param_grid = (
  ParamGridBuilder()
  .addGrid(xgb.eta, [1e-1, 1e-2, 1e-3])
  .addGrid(xgb.maxDepth, [2, 4, 8])
  .build()
)
 
cv = CrossValidator(
  estimator=xgb,
  estimatorParamMaps=param_grid,
  evaluator=bce,#mce,
  numFolds=5
)
 
import mlflow
import mlflow.spark
 
spark_model_name = "best_model_spark"
 
with mlflow.start_run():
  model = cv.fit(train_sdf_prepared)
  best_params = dict(
    eta_best=model.bestModel.getEta(),
    maxDepth_best=model.bestModel.getMaxDepth()
  )
  mlflow.log_params(best_params)
  
  mlflow.spark.log_model(fitted_pipeline, "featuriser")
  mlflow.spark.log_model(model, spark_model_name)
 
  metrics = dict(
    roc_test=bce.evaluate(model.transform(test_sdf_prepared))
  )
  mlflow.log_metrics(metrics)

# COMMAND ----------


