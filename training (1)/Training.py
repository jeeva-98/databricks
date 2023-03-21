# Databricks notebook source
import pandas as pd
import sklearn
import mlflow
import numpy as np

# COMMAND ----------

sql = "SELECT * FROM " +  "default.iris_data"
data= spark.sql(sql).toPandas()

# COMMAND ----------

data=data[0:100]

# COMMAND ----------

# Splitting the dataset into the Training set and Test set
X = data.iloc[:, [0,1,2, 3]].values
y = data.iloc[:, 4].values
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.25, random_state = 0)


# COMMAND ----------

# from sklearn.preprocessing import StandardScaler
# sc = StandardScaler()
# X_train = sc.fit_transform(X_train)
# X_test = sc.transform(X_test)

# COMMAND ----------

from sklearn.linear_model import LogisticRegression
classifier = LogisticRegression(random_state = 0, solver='lbfgs', multi_class='auto')
classifier.fit(X_train, y_train)

# COMMAND ----------

# Predicting the Test set results
y_pred = classifier.predict(X_test)
# Predict probabilities
probs_y=classifier.predict_proba(X_test)
### Print results 
probs_y = np.round(probs_y, 2)

# COMMAND ----------

from sklearn.metrics import confusion_matrix
cm = confusion_matrix(y_test, y_pred)
print(cm)

# COMMAND ----------

from sklearn import metrics
from sklearn.metrics import classification_report
print(metrics.classification_report(y_test, y_pred))

# COMMAND ----------

classificationReport = classification_report(y_test, y_pred, output_dict=True)
classificationReport

# COMMAND ----------

mlflow.start_run()


# COMMAND ----------

for key in classificationReport.keys():
  if key == 'accuracy':
    mlflow.log_metric(key, classificationReport[key])
    #print(key, classificationReport[key])
  else:
    for nested_key in classificationReport[key].keys():
      mlflow.log_metric(key + "_" + nested_key, classificationReport[key][nested_key])

# COMMAND ----------

input_example = {
    "sepal length (cm)": 5.1,
    "sepal width (cm)": 3.5,
    "petal length (cm)": 1.4,
    "petal width (cm)": 0.2,
}

# COMMAND ----------

model_name="irismodel"
mlflow.sklearn.log_model(classifier, artifact_path = model_name, registered_model_name=model_name,input_example=input_example)

# COMMAND ----------

mlflow.log_param("model_name_list", model_name)

# COMMAND ----------

classificationReport_df =pd.DataFrame.from_dict(classificationReport).transpose()
classificationReport_df

# COMMAND ----------

fn = 'classificationReport_df.csv'
classificationReport_df.to_csv(fn)
mlflow.log_artifact(fn)

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------



# COMMAND ----------


