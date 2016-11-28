package com.github.rbrugier

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document

object MainNasa extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongotnasa")
    .setMaster("local[*]")
    .set("spark.sql.shuffle.partitions", "2")

    val sc = new SparkContext(conf)

  val readConfig = ReadConfig( Map("spark.mongodb.input.uri" -> "mongodb://127.0.0.1/nasa.eva"))
  val evaRdd = sc.loadFromMongoDB( readConfig = readConfig )
  println (evaRdd.count())
}
