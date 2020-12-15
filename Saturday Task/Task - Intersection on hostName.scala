// Databricks notebook source
//  /FileStore/tables/nasa_august-6.tsv
//  /FileStore/tables/nasa_july-6.tsv
// publish: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8861196314593728/4439048079168112/2851477278659892/latest.html

// COMMAND ----------

//Creating RDD
val julRdd = sc.textFile("/FileStore/tables/nasa_july-6.tsv")
val augRdd = sc.textFile("/FileStore/tables/nasa_august-6.tsv")

// COMMAND ----------

val julHost = julRdd.map(x => x.split("\t")(0))
val augHost = augRdd.map(x => x.split("\t")(0))

// COMMAND ----------

val interRdd = julHost.intersection(augHost)

// COMMAND ----------

def headRemover (line: String): Boolean = !(line.startsWith("host"))
interRdd.filter(x => headRemover(x)).count()
