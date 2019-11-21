package edu.stanford.cs246

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.SparkContext._

object WordCountScala {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    // Load the data
    val data = sc.textFile(args(0))

    // Parse the data into words
    val words = data.flatMap(line => line.split("[^\\w]+"))
        .map(word => word.toUpperCase)
        .filter(word => !word.isEmpty)

    // Convert each word into a tuple of its first letter and 1
    val pairs = words.map(word => (word.charAt(0), 1))

    // Sum the counts for each character and save them to disk
    pairs.reduceByKey((c1, c2) => c1 + c2).sortByKey().saveAsTextFile(args(1))

    sc.stop()
  }
}
