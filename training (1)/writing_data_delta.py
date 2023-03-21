# Databricks notebook source
# MAGIC %fs cp "dbfs:/FileStore/tables/phonic_aquifer_381208_dd83862f218f.json" "file:/tmp/big.json"

# COMMAND ----------

# MAGIC %fs head "dbfs:/FileStore/tables/phonic_aquifer_381208_dd83862f218f.json"

# COMMAND ----------

cfp= "/tmp/big.json"
pn="phonic-aquifer-381208"
table="iris.iris_trail"

# COMMAND ----------

df=spark.read.format("bigquery").option("credentialsfile",cfp).option("parentProject",pn).option('table',table).load()

# COMMAND ----------

display(df)

# COMMAND ----------

#df1=spark.createDataFrame(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("iris_data")

# COMMAND ----------

# sql = "SELECT * FROM " +  "default.iris_data"
# data= spark.sql(sql).toPandas()

# COMMAND ----------

# queryjoborders_pni_sett_mv_vn

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs head "dbfs:/FileStore/tables/phonic_aquifer_381208_dd83862f218f.json"

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.gs.project.id", "phonic-aquifer-381208")
spark._jsc.hadoopConfiguration().set("fs.gs.system.bucket", "databricksbq123")
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", "iris-512@phonic-aquifer-381208.iam.gserviceaccount.com")
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id", "dd83862f218f64768822a6105d3009bc71697d45")
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDvPf1AkcZu2rW7\n7a9ggHEWLRn8G5WX7Mr8KMLTemQOkv8FXpexbf/WRIY3tOILrK2rH7597WB2q9YB\nYo0lglA8PtSA0GxJYpIlhMMy4lqBC/IUWoHq1fhNbPSNcZh89bQi+56OxB6hLMg4\nm72FncPE1DKSrS1bxJgK42K2CSNihNfvDHTFYlhYW8vmyVaguSfLQaboqZZOuIPM\ntDiTaGxj8NZaZz2bChwhuMu1CTNP3IzmABO5qabM+nZ9488Ymi+FtllGjCFNQ/JU\njnI5nAjzAnXeA/B0Vc9+o6v+tirgTtCoAfRC0otjw4AOx7nHGgfMknAzs8OT2y+l\nhP32mjgXAgMBAAECggEAFeCcTRBHjhGP9tx4YiG8taNwMd6v4lG0CjUMA9y4U6Db\nkzkgNAh0Ebr4bxWqahjjVLe1aryPSqrSChRiBeERx0CTnCfzDw7lgGrQktGiJxTZ\nkUTphnkEmMfy4RBXOm4Zh+1shW+nwWNf99U+En7nzRL+gO03LaIfqTc76puRUcvY\n0fBzRFMmiIpvrVPFEkloPMODOm9bQOPom9v8CqC0+wNUdSSx3og0Dfv1aMDCaXrE\nFZJ0XgpirZOkCUtvIvOv7nLPcPHj8Iol2DQcUecM+jY+0kQFBcUjwVE//e/UyYxL\nfvlH0+H4ZqInAiDpZNYFU7/5AjUx+VY4xoHVKEie2QKBgQD8WYoFgHAZcLJ+sn8A\nwAy+KolbKr1ryXo2qUCwExg+r9S4cfV5bUDLAo4izgOlV2gHLao+6xZCkwFjNsWY\nqYC4iX9Oe8QFMz7hZ5FSFtsOaMdUd9Cu6yTXzWHpN76o93hgO/szG/Nc5tSQJe7W\n+i9R1EIt4nqR5M2doXd1VedpUwKBgQDys+mAMdWL2byvgUoE+FxW6SKTKoaOjU9S\n6kVyQzJWIBV5xV4dvuQpFp9uDSkLgQ4QZ/ZnuwWrkIux4w5NvZ6z2bYLBCLCVf1y\nOAsoKng80yN2z3XhpXZx4oI/4shnJx1jrlT6FgHl4wt8sO9KL2dD4k08iveowTL8\nOHuV80dprQKBgGJaZ6ADUi2oLfmRikx5jb3kiEp/GvrSuQ5q4yp9Frr//vGwgNNF\n0LYPFeTc1LDxozsBAlSpfbCO6UCJDXSVw/SoqnSlal34fi6OFiaff9ta3S2/0RcL\n0P1HMD0cm/PxF/qyBhMCdtSjpv9TuYYSVrUW/IAzwD/ypG0mjI+v3pS/AoGBAK1x\nm1YsxvJZmXlHCRHCN6M4mqfXRS/MlrqnhxegI/HendpXDOpvB/jBHRB1wTZidR2O\nXk5FHkk8VEi8/fTapPbciDcZLon5D9W3bWm36BN3xCldG141dA7xE6n6NMNa9arN\n5BaQ8axK9GOYwnkepUqjCBVTLqff4cLTzqYbpJZFAoGASVncQFIy7uWSfSIALVjW\nd/wQm7L9fslzz90F52u2FlkGwlRwxbz05fY8f0eXbT/ADNjTUK/QIfmHmriragXi\nH1WcmZtCuBqk/Vjby+l844ELqcF7prXEE2NCZSeCY8tazIhHvwjmgzUuC62TuCOQ\nk8c+AZtqtAzMehqIuhYEXWU=\n-----END PRIVATE KEY-----\n")

# COMMAND ----------

def to_base64(creds):
    creds_bytes = creds.encode('ascii')
    base64_bytes = base64.b64encode(creds_bytes)
    return base64_bytes.decode('ascii')

# COMMAND ----------

cfp= {
  "type": "service_account",
  "project_id": "phonic-aquifer-381208",
  "private_key_id": "dd83862f218f64768822a6105d3009bc71697d45",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDvPf1AkcZu2rW7\n7a9ggHEWLRn8G5WX7Mr8KMLTemQOkv8FXpexbf/WRIY3tOILrK2rH7597WB2q9YB\nYo0lglA8PtSA0GxJYpIlhMMy4lqBC/IUWoHq1fhNbPSNcZh89bQi+56OxB6hLMg4\nm72FncPE1DKSrS1bxJgK42K2CSNihNfvDHTFYlhYW8vmyVaguSfLQaboqZZOuIPM\ntDiTaGxj8NZaZz2bChwhuMu1CTNP3IzmABO5qabM+nZ9488Ymi+FtllGjCFNQ/JU\njnI5nAjzAnXeA/B0Vc9+o6v+tirgTtCoAfRC0otjw4AOx7nHGgfMknAzs8OT2y+l\nhP32mjgXAgMBAAECggEAFeCcTRBHjhGP9tx4YiG8taNwMd6v4lG0CjUMA9y4U6Db\nkzkgNAh0Ebr4bxWqahjjVLe1aryPSqrSChRiBeERx0CTnCfzDw7lgGrQktGiJxTZ\nkUTphnkEmMfy4RBXOm4Zh+1shW+nwWNf99U+En7nzRL+gO03LaIfqTc76puRUcvY\n0fBzRFMmiIpvrVPFEkloPMODOm9bQOPom9v8CqC0+wNUdSSx3og0Dfv1aMDCaXrE\nFZJ0XgpirZOkCUtvIvOv7nLPcPHj8Iol2DQcUecM+jY+0kQFBcUjwVE//e/UyYxL\nfvlH0+H4ZqInAiDpZNYFU7/5AjUx+VY4xoHVKEie2QKBgQD8WYoFgHAZcLJ+sn8A\nwAy+KolbKr1ryXo2qUCwExg+r9S4cfV5bUDLAo4izgOlV2gHLao+6xZCkwFjNsWY\nqYC4iX9Oe8QFMz7hZ5FSFtsOaMdUd9Cu6yTXzWHpN76o93hgO/szG/Nc5tSQJe7W\n+i9R1EIt4nqR5M2doXd1VedpUwKBgQDys+mAMdWL2byvgUoE+FxW6SKTKoaOjU9S\n6kVyQzJWIBV5xV4dvuQpFp9uDSkLgQ4QZ/ZnuwWrkIux4w5NvZ6z2bYLBCLCVf1y\nOAsoKng80yN2z3XhpXZx4oI/4shnJx1jrlT6FgHl4wt8sO9KL2dD4k08iveowTL8\nOHuV80dprQKBgGJaZ6ADUi2oLfmRikx5jb3kiEp/GvrSuQ5q4yp9Frr//vGwgNNF\n0LYPFeTc1LDxozsBAlSpfbCO6UCJDXSVw/SoqnSlal34fi6OFiaff9ta3S2/0RcL\n0P1HMD0cm/PxF/qyBhMCdtSjpv9TuYYSVrUW/IAzwD/ypG0mjI+v3pS/AoGBAK1x\nm1YsxvJZmXlHCRHCN6M4mqfXRS/MlrqnhxegI/HendpXDOpvB/jBHRB1wTZidR2O\nXk5FHkk8VEi8/fTapPbciDcZLon5D9W3bWm36BN3xCldG141dA7xE6n6NMNa9arN\n5BaQ8axK9GOYwnkepUqjCBVTLqff4cLTzqYbpJZFAoGASVncQFIy7uWSfSIALVjW\nd/wQm7L9fslzz90F52u2FlkGwlRwxbz05fY8f0eXbT/ADNjTUK/QIfmHmriragXi\nH1WcmZtCuBqk/Vjby+l844ELqcF7prXEE2NCZSeCY8tazIhHvwjmgzUuC62TuCOQ\nk8c+AZtqtAzMehqIuhYEXWU=\n-----END PRIVATE KEY-----\n",
  "client_email": "iris-512@phonic-aquifer-381208.iam.gserviceaccount.com",
  "client_id": "108505242936156613680",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/iris-512%40phonic-aquifer-381208.iam.gserviceaccount.com"
}

# COMMAND ----------



# COMMAND ----------

df.write.format("bigquery").mode("append").option("writeMethod", "direct").option("parentProject", "phonic-aquifer-381208").option("temporaryGcsBucket", "databricksbq123").option("table", "iris.prediction").save()


# COMMAND ----------

table = "bigquery-public-data.samples.shakespeare"
df = spark.read.format("bigquery").option("table",table).load()
display(df.createOrReplaceTempView("shakespeare"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.show(3)

# COMMAND ----------

gs://spark-lib/bigquery/spark-3.3-bigquery-0.29.0-preview.jar

# COMMAND ----------


