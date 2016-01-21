package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment2

import util.Tokenizer
import scala.collection.mutable.{HashMap}

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def getStripes(line: String): Iterator[(String, HashMap[String, Float])] = {
    val list = tokenize(line)
    val listOfStripes = new HashMap[String, HashMap[String, Float]]()

    if (list.length < 2) return listOfStripes.iterator

    for(i <- 1 until list.length){
      if (!listOfStripes.contains(list(i-1))) listOfStripes.put(list(i-1), new HashMap[String, Float]() { override def default(key: String) = 0 })

      listOfStripes(list(i-1)).put(list(i), listOfStripes(list(i-1))(list(i)) + 1)
    }

    listOfStripes.iterator
  }

  def reduceStripe(s1: HashMap[String, Float], s2: HashMap[String, Float]): HashMap[String, Float] = {
    val result = new HashMap[String, Float]() { override def default(key: String) = 0 } ++= s1
    s2.foreach{case(key, value) => result.put(key, result(key) + value)}

    result
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
