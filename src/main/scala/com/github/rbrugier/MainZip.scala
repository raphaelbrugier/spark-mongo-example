package com.github.rbrugier

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document


object MainZip extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongozips")
    .setMaster("local[*]")
    .set("spark.sql.shuffle.partitions", "2")
    .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.zips")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val zipDf = sqlContext.read.mongo()

  println( "States with Populations above 10 Million")
  zipDf.groupBy("state")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .filter($"count" > 10000000)
//    .show()

  println( "Average City Population by State" )
  zipDf
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .groupBy("state").avg("count")
    .withColumnRenamed("avg(count)", "avgCityPop")
//    .show()

  println( "Largest and Smallest Cities by State" )
  // Dataframes:
  // Use a join to find the min and max, could return multiple min or max in case of ties
  val popByCity = zipDf
    .filter($"pop" > 0)
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")

  popByCity.join(
    popByCity
      .groupBy("state")
      .agg(max("count") as "max_pop", min("count") as "min_pop")
      .withColumnRenamed("state", "r_state"),
    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop")
    )
    .drop($"r_state")
    .drop($"max_pop")
    .drop($"min_pop")
//  .show()

  println( "Aggregation pipeline integration example")
  val mongoRDD = sc.loadFromMongoDB(ReadConfig(conf))
  val rdd = mongoRDD
    .withPipeline(List(Document.parse("""{ $match: {pop: { $gt:0 } } }" """)))
    .toDF()
    .registerTempTable("zips")

  val popByCity2 = sqlContext.sql(
    """SELECT state, city, sum(pop) AS count
      |FROM zips
      |GROUP BY state, city"""
      .stripMargin)

  val minMaxCities = popByCity2.join(
    popByCity2
      .groupBy("state")
      .agg(max("count") as "max_pop", min("count") as "min_pop")
      .withColumnRenamed("state", "r_state"),
    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop")
    )
    .drop($"r_state")
    .drop($"max_pop")
    .drop($"min_pop")

  MongoSpark
    .write(minMaxCities)
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/test" )
    .option("collection","minMaxCities")
    .mode("overwrite")
//    .save()
}
