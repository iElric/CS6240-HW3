package FollowerCount

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object FollowerRDDF {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nFollowerCount.FollowerRDDF <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("FollowerRDDF")
    val sc = new SparkContext(conf)

    // read the text file and output a RDD of strings
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => line.split(",")(1))
      // map to a pairRDD
      .map(userId => (userId, 1))
      // merge the values for each key
      .foldByKey(0)((count1, count2) => count1 + count2)
      .sortBy(_._2, ascending = false)
    // Output the counting file
    counts.saveAsTextFile(args(1))
    println(counts.toDebugString)
    logger.info("Debug string:")
    logger.info(counts.toDebugString)
  }
}