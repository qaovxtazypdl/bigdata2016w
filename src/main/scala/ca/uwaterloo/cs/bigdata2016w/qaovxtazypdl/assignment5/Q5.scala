package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import util.Tokenizer
import util._
import scala.collection.mutable.{HashMap}

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.rogach.scallop._

class ConfQ5(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfQ5(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())

    val conf = new SparkConf().setAppName("A5Q5")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input())
  }
}
