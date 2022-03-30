package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object WordCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    
    var linesrdd = sc.textFile(args(0))
    
    val followercount = linesrdd.map(line => (line.split(",")(1))).filter(userid => userid.toInt % 100 == 0).map(result => (result,1)).reduceByKey(_ + _)
    // splits each line by (",").It filters all the user_ids that are divisible by 100.It then maps those user_ids and then aggregates the count on each user_id
    println(followercount.toDebugString)

    followercount.saveAsTextFile(args(1))

   
  }
}