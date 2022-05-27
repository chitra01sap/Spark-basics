package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.max

object MaxTemperature {

  def parseLine(line: String): (String, String, Float) = {
    val data = line.split(",")
    val stationId = data(0)
    val tempType = data(2)
    val temp = data(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, tempType, temp)

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkContext = new SparkContext("local[*]", "MaxTemp")

    val data = sparkContext.textFile("data/1800.csv")

    val lines = data.map(parseLine)

    val maxTemp = lines.filter(x => x._2 == "TMAX")

    val tupledat = maxTemp.map(x => (x._1, x._3.toFloat))

    val minTempsByStation = tupledat.reduceByKey((x, y) => max(x, y))

    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station maximum temperature: $formattedTemp")
    }

  }
}
