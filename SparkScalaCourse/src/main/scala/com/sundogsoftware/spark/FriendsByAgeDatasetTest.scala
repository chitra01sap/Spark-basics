package com.sundogsoftware.spark

import org.apache.log4j
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, round}

object FriendsByAgeDatasetTest {

  final case class friends(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("datasetTest")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[friends]

    val ageAndFrends = dataset.select("age", "friends")

    ageAndFrends.groupBy("age").avg("friends").show()

    ageAndFrends.groupBy("age").avg("friends").sort("age").show()

    ageAndFrends.groupBy("age").agg(round(avg("friends"), 2)).sort("age").show()

    ageAndFrends
      .groupBy("age")
      .agg(round(avg("friends"), 2))
      .as("friends_age")
      .sort("age")
      .show()

  }

}
