package com.github.rbrugier

import com.mongodb.spark.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.{SparkConf, SparkContext}

object MainSteps extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongozips")
    .setMaster("local[*]")
    .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.zips") // 1)

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val zipDf = sqlContext.read.mongo() // 2)

  zipDf.printSchema() // 3)
  zipDf.show()

  // Query 1
  println( "States with Populations above 10 Million" )
  import sqlContext.implicits._ // 1)
  zipDf.groupBy("state")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .filter($"count" > 10000000)
    .show()

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

  popByCity.join(
    popByCity
      .groupBy("state")
      .agg(max("count") as "max_pop", min("count") as "min_pop") // 2)
      .withColumnRenamed("state", "r_state"),
    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop") // 3)
  )
    .drop($"r_state")
    .drop($"max_pop")
    .drop($"min_pop") // 4)
    .show()

  // SparkSQL:
  zipDf.registerTempTable("zips") // 1)
  sqlContext.sql( // 2)
    """SELECT state, sum(pop) AS count
      FROM zips
      GROUP BY state
      HAVING sum(pop) > 10000000"""
  )
  .show()
}
