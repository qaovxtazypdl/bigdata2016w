package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment2

import util.Tokenizer
import scala.collection.mutable.{HashMap}

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class BigramPartitioner(partitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = {
    val modulus = key.asInstanceOf[(String, String)]._1.hashCode % partitions
    if (modulus < 0) modulus + partitions else modulus
  }

  def numPartitions(): Int = {partitions}
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def getPairs(line: String): Iterator[((String, String), Int)] = {
    val tokens = tokenize(line)
    if (tokens.length < 2) return List().iterator

    tokens.sliding(2).flatMap(slidingPair => List(
      ( (slidingPair(0), slidingPair(1)) , 1),
      ( (slidingPair(0), "*"), 1)
    )).toList.iterator
  }

  def processPartition(partitionIndex: Int, partitionData: Iterator[((String, String), Int)]): Iterator[((String, String), Float)] = {
    val marginalCounts = new HashMap[String, Int]()
    partitionData.foreach(pair => {
      if (pair._1._2 equals "*") {
        marginalCounts.put(pair._1._1, pair._2)
      }
    })

    partitionData.flatMap(pair => {
      if (!(pair._1._2 equals "*")) {
        return List((pair._1, pair._2 / marginalCounts(pair._1._1).toFloat)).iterator
      } else {
        return List().iterator
      }
    })
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
    textFile
      .flatMap(getPairs(_))
      .reduceByKey(_ + _, args.reducers())
      .repartitionAndSortWithinPartitions(new BigramPartitioner(args.reducers()))
      .mapPartitionsWithIndex(processPartition(_, _))
      .saveAsTextFile(args.output())
  }
}
