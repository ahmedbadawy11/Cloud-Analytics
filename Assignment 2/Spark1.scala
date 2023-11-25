// Databricks notebook source
val dbfsDirectoryPath = "/FileStore/tables/Text_Corpus/*.text"
val dataRDD = spark.sparkContext.textFile(dbfsDirectoryPath)

// COMMAND ----------

dataRDD.count()

// COMMAND ----------

// MAGIC %scala
// MAGIC val keyword = "antibiotics"
// MAGIC
// MAGIC val countOfAntibiotics = dataRDD
// MAGIC   .flatMap(line => line.split(" "))  
// MAGIC   .filter(word => word == keyword)  
// MAGIC   .count() 
// MAGIC
// MAGIC println(s"The word '$keyword' appears $countOfAntibiotics times.")

// COMMAND ----------

// MAGIC %scala
// MAGIC val keyword1 = "patient"
// MAGIC val keyword2 = "admitted"
// MAGIC
// MAGIC val countOfBothWords = dataRDD
// MAGIC   .map(line => (line, line.split(" "))) 
// MAGIC   .filter { case (line, words) => words.contains(keyword1) && words.contains(keyword2) } 
// MAGIC   .count()
// MAGIC
// MAGIC println(s"The words '$keyword1' and '$keyword2' appear together on the same line $countOfBothWords times.")
// MAGIC

// COMMAND ----------



// COMMAND ----------


