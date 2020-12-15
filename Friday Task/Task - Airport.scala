// Databricks notebook source
// Airport Data = /FileStore/tables/airports.txt
// Publish: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8861196314593728/3834110251926788/2851477278659892/latest.html

// COMMAND ----------

// DBTITLE 1,Airport RDD Created
//Creating RDD
val airData = sc.textFile("/FileStore/tables/airports.txt")

// COMMAND ----------

// DBTITLE 1,Task 1: latitude > 40 or country = Island
val locatedData = airData.filter( line => {
  (line.split(",")(6) > "\"40\"") || (line.split(",")(3).contains("Island") )
})
locatedData.collect()

// COMMAND ----------

locatedData.saveAsTextFile("CountryIsland02.csv")

// COMMAND ----------

// DBTITLE 1,Task 2: Count Occurrence of timestamp when time = "Pacific/Port_Moresby" && altitude is even
val occurance = airData.filter(line => {
  (line.split(",")(11).contains("Pacific/Port_Moresby")) && ((line.split(",")(8).toInt) % 2 == 0)
})
occurance.count()
