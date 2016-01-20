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

  // stripe: (string, HashMap[string, int])

  def getStripes(line: String): Iterator[(String, HashMap[String, Int])] = {
    val list = tokenize(line)
    val listOfStripes = new HashMap[String, HashMap[String, Int]]() {
      override def default(key: String) = new HashMap[String, Int]() {
        override def default(key: String) = 0
      }
    }

    if (list.length < 2) return listOfStripes.iterator

    for(i <- 1 until list.length){
      listOfStripes(list(i-1))(list(i)) = listOfStripes(list(i-1))(list(i)) + 1
    }

    println(listOfStripes.mkString(" "))

    return listOfStripes.iterator
  }

  def reduceStripe(s1: HashMap[String, Int], s2: HashMap[String, Int]): HashMap[String, Int] = {
    s1.foreach{case(key, value) => s2(key) = s2(key) + value}
    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@REDokay" + s1.toString());
    s1
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

    println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" + textFile.flatMap(getStripes(_)).count())

    //textFile
      //.flatMap(getStripes(_))
      //.reduceByKey(reduceStripe(_, _))
      //.take(10)
      //.saveAsTextFile(args.output())
  }
}
