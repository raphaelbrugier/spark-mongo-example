package com.github.rbrugier

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object MongoSparkMain extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongozips")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "zips")) // 1)
  val zipDf = sc.loadFromMongoDB(readConfig).toDF() // 2)

  zipDf.printSchema() // 3)
  zipDf.show()

  // Query 1
  println( "States with Populations above 10 Million" )
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._ // 1)
  zipDf.groupBy("state")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count") // 2)
    .filter($"count" > 10000000)
    .show() // 3)

  // Query 2
  println( "Average City Population by State" )
  zipDf
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .groupBy("state")
    .avg("count")
    .withColumnRenamed("avg(count)", "avgCityPop")
    .show()

  // Query 3
  println( "Largest and Smallest Cities by State" )
  val popByCity = zipDf // 1)
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")

  val minMaxCities = popByCity.join(
    popByCity
      .groupBy("state")
      .agg(max("count") as "max_pop", min("count") as "min_pop") // 2)
      .withColumnRenamed("state", "r_state"),
    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop") // 3)
    )
    .drop($"r_state")
    .drop($"max_pop")
    .drop($"min_pop") // 4)
  minMaxCities.show()


  // SparkSQL:
  println( "SparkSQL" )
  zipDf.registerTempTable("zips") // 1)
  sqlContext.sql( // 2)
    """SELECT state, sum(pop) AS count
      FROM zips
      GROUP BY state
      HAVING sum(pop) > 10000000"""
  )
  .show()

  // Aggregation pipeline integration:
  println( "Aggregation pipeline integration" )
  zipDf
    .filter($"pop" > 0)
    .show()

  println( "RDD with Aggregation pipeline" )
  val mongoRDD = sc.loadFromMongoDB(readConfig) // 1)
  mongoRDD
    .withPipeline(List( // 2)
      Document.parse("""{ $group: { _id: "$state", totalPop: { $sum: "$pop" } } }"""),
      Document.parse("""{ $match: { totalPop: { $gte: 10000000 } } }""")
    ))
    .collect()
    .foreach(println)

  // Writing data in MongoDB:
  MongoSpark
    .write(minMaxCities)
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/test" )
    .option("collection","minMaxCities")
    .mode("overwrite")
    .save()
}
