package com.github.rbrugier

import com.mongodb.spark.sql._
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SQLContext
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

  println( "States with Populations above 10 Million" )
  zipDf.groupBy("state")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .filter(zipDf("count") > 10000000)
    .show()


}
