// Databricks notebook source
// DBTITLE 1,Number Data File
// /FileStore/tables/numberData.csv

//publish : https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8861196314593728/956973817763519/2851477278659892/latest.html

// COMMAND ----------

// DBTITLE 1,Creating RDD
val numData = sc.textFile("/FileStore/tables/numberData.csv")
numData.take(3)

// COMMAND ----------

// DBTITLE 1,Removing Header
val header = numData.first
val noHead = numData.filter(x => x!=header)
noHead.take(3)

// COMMAND ----------

// DBTITLE 1,Filter data - > converting in int format
val intData = noHead.map(line => line.toInt)
intData.collect()

// COMMAND ----------

// DBTITLE 1,Prime Number Method
def isPrime(num:Int): Boolean = {
  var flag = true
  var x = 0
  if (num == 0 || num == 1)
    return false
  else
  {
    for (i <-2 until (num/2))
    {
      if (num % i == 0)
        flag = false
    }
  }
  return flag
}
//intData.collect()

val primeFiltered = intData.filter(line => isPrime(line))
primeFiltered.collect()

// COMMAND ----------

// DBTITLE 1,Sum
val sumData = primeFiltered.sum()
println("Total Sum : "+ sumData)
println ("Total prime numbers : "+primeFiltered.count())
