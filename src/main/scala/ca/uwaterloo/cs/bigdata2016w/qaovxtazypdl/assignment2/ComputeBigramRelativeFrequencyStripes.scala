package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment2

import util._
import scala.collection.mutable.{HashMap}

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def getStripes(line: String): Iterator[(String, HashMap[String, Float])] = {
    val tokens = tokenize(line)
    if (tokens.length < 2) return List().iterator

    tokens.sliding(2).map(slidingPair => {
      val hmap = new HashMap[String, Float]()
      hmap.put(slidingPair(1), 1)
      (slidingPair(0), hmap)
    })
  }

  def reduceStripe(s1: HashMap[String, Float], s2: HashMap[String, Float]): HashMap[String, Float] = {
    if (s1.size > s2.size) {
      s2.foreach{case(key, value) => s1.put(key, s1.getOrElse(key, 0f) + value)}
      s1
    } else {
      s1.foreach{case(key, value) => s2.put(key, s2.getOrElse(key, 0f) + value)}
      s2
    }
  }

  def mapReducedStripes(stripe: (String, HashMap[String, Float])): (String, HashMap[String, Float]) = {
    var sum : Float = 0
    stripe._2.foreach(sum += _._2)
    stripe._2.foreach(x => stripe._2.put(x._1, x._2 / sum))
    stripe
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    textFile.flatMap(getStripes(_))
      .reduceByKey(reduceStripe(_, _), args.reducers())
      .map(mapReducedStripes(_))
      .saveAsTextFile(args.output())
  }
}
