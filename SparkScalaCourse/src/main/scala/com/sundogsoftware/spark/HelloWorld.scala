package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]): Unit = {

    //Logger.getLogger("org").setLevel(Level.ERROR)

    //  val sc = new SparkContext("local[*]", "HelloWorld")

    //val lines = sc.textFile("data/ml-100k/u.data")
    //val numLines = lines.count()

    // println("Hello world! The u.data file has " + numLines + " lines.")

    // sc.stop()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "HelloWorld")
    val lines = sc.textFile("data/ml-100k/u.data")
    val count = lines.count()
    println("hello world! the u.data file has" + count + "lines")
    sc.stop()
  }
}
/*
1. read the count of lines from file
2. word count from the lines - https://github.com/barrelsofdata/spark-wordcount/blob/base/src/test/scala/com/barrelsofdata/sparkexamples/WordCountTest.scala
    https://barrelsofdata.com/spark-word-count-with-unit-tests/
3.Create sample data
Load sample data
View a DataSet
Process and visualize the Dataset

Spark basic transformation opertaion

https://www.obstkel.com/spark-scala-examples
https://databricks.com/spark/getting-started-with-apache-spark/datasets
https://docs.databricks.com/getting-started/spark/datasets.html?_ga=2.166849086.124764298.1643094476-2015950404.1642399645#dataset-notebook
https://www.analyticsvidhya.com/blog/2020/12/big-data-with-spark-and-scala/
 */
