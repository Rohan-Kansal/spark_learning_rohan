// Databricks notebook source
//Task 1- avg number of friend for each age
//Task 2- max number of friend for each age
// file - /FileStore/tables/FriendsData.csv
// publish: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/8861196314593728/1979775830305268/2851477278659892/latest.html

// COMMAND ----------

// DBTITLE 1,Main Rdd created
val friendData = sc.textFile("/FileStore/tables/FriendsData.csv")
friendData.take(5)

// COMMAND ----------

// DBTITLE 1,Column Name Removed
val removedHeader = friendData.filter(x => !x.contains("Id")) // remove header column
removedHeader.take(3)

// COMMAND ----------

// DBTITLE 1,Key-Value Pair
val ageRdd = removedHeader.map(x => (x.split(",")(2).toInt, (1, x.split(",")(3).toFloat)) )
ageRdd.collect()

// COMMAND ----------

ageRdd.map(x=> x._1).take(10)

// COMMAND ----------

ageRdd.map(x=> x._2).take(10)

// COMMAND ----------

// DBTITLE 1,TASK 1 -----> Average number of friends for each Age
val reducedRdd = ageRdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2))
reducedRdd.take(10)

// COMMAND ----------

// DBTITLE 1,Cal Average
val avgRdd = reducedRdd.mapValues(data => data._2 / data._1)
avgRdd.collect()

// COMMAND ----------

// DBTITLE 1,Better Display Format
for ( (age, avgFriend) <- avgRdd.collect())
  println (age + " : " + avgFriend)

// COMMAND ----------

// DBTITLE 1,TASK 2 --> Max number of friends for each Age
val ageRdd2 = removedHeader.map(x => (x.split(",")(2), x.split(",")(3).toInt) )
val maxRdd = ageRdd2.max()(new Ordering[Tuple2[String, Int]]() {
  override def compare(x: (String, Int), y: (String, Int)): Int =
    Ordering[Int].compare(x._2, y._2)
})

// COMMAND ----------

// DBTITLE 1,Task2 --> 2nd Approach
val ageRdd3 = removedHeader.map(x => (x.split(",")(2).toInt, x.split(",")(3).toInt))
ageRdd3.take(3)

// COMMAND ----------

val maxFriend = ageRdd3.reduceByKey(math.max(_, _))
maxFriend.collect()

// COMMAND ----------

val eachAgeMax = maxFriend.sortByKey()
eachAgeMax.collect()

// COMMAND ----------

for ( (age, max) <- eachAgeMax.collect())
  println (age + " : " + max)

// COMMAND ----------

// DBTITLE 1,GroupByKey
val maxFriend2 = ageRdd3.groupByKey()
//maxFriend.collect()
val sorted = maxFriend.sortByKey()
sorted.collect()
