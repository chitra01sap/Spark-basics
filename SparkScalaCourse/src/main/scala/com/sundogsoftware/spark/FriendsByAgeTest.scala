package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  * calculate the number of friends by age
  * sample data
  * id,name,age,friends
  * 0,Will,33,385
  * 1,Jean-Luc,26,2
  */
object FriendsByAgeTest {

  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String): (Int, Int) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)

    /**
      * 19,246
      * 20,220
      * 19,268
      * 20,72
      */
    val tupleOfFrends = rdd.mapValues(x => (x, 1)) // here key will remain same & value will get converted into tuple like(19,(246,1)),(20,(220,1)),(19,(268,1)) etc

    // (19,(246,1))  => (246,1)  => (x_1,x_2)
    val totalFrendsCount =
      tupleOfFrends.reduceByKey((x, y) => ((x._1 + y._1), (x._2 + y._2)))

    val averagesByAge = totalFrendsCount.mapValues(x => x._1 / x._2)
    val results = averagesByAge.collect()
    results.sorted.foreach(println)

  }
}
