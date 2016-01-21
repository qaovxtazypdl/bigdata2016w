package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment2

import util.Tokenizer
import scala.collection.mutable.{HashMap}

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def getStripes(line: String): Iterator[(String, HashMap[String, Int])] = {
    val list = tokenize(line)
    val listOfStripes = new HashMap[String, HashMap[String, Int]]()

    if (list.length < 2) return listOfStripes.iterator

    for(i <- 1 until list.length){
      if (!listOfStripes.contains(list(i-1))) listOfStripes.put(list(i-1), new HashMap[String, Int]() { override def default(key: String) = 0 })

      listOfStripes(list(i-1)).put(list(i), listOfStripes(list(i-1))(list(i)) + 1)
    }

    listOfStripes.iterator
  }

  def reduceStripe(s1: HashMap[String, Int], s2: HashMap[String, Int]): HashMap[String, Int] = {
    val result = new HashMap[String, Int]() { override def default(key: String) = 0 } ++= s1
    s2.foreach{case(key, value) => result.put(key, result(key) + value)}

    result
  }

  def mapReducedStripes(stripe: (String, HashMap[String, Int])): (String, HashMap[String, Float]) = {
    var sum : Float = 0
    val bigramRelativeFreqs = new HashMap[String, Float]()
    stripe._2.foreach(sum += _._2)
    stripe._2.foreach(x => bigramRelativeFreqs.put(x._1, x._2 / sum))
    (stripe._1, bigramRelativeFreqs)
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
