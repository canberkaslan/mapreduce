package edu.stanford.cs246;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCountJava {
  public static void main(String[] args) throws Exception {
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load the data
    JavaRDD<String> data = sc.textFile(args[0]);

    // Parse the data into words
    JavaRDD<String> words =
          data.flatMap(line -> Arrays.asList(line.split("[^\\w]+")).iterator())
          .map(word -> word.toUpperCase())
          .filter(word -> !word.isEmpty());

    // Convert each word into a tuple of its first letter and 1
    JavaPairRDD<Character, Integer> pairs =
        words.mapToPair(word -> new Tuple2<Character, Integer>(word.charAt(0), 1));

    // Sum the counts for each character and save them to disk
    pairs.reduceByKey((c1, c2) -> c1 + c2).sortByKey().saveAsTextFile(args[1]);
  }
}
