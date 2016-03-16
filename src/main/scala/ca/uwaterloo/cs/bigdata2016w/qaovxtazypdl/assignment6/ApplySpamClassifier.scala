package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6

import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.Map

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def classify(feat : (Int, Iterable[(String, String, Array[Int])]), w : Map[Int, Double]): Iterable[(String,String,Double,String)] = {
    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // For each instance...
    feat._2.map(x => {
      val isSpam = x._2 equals "spam"
      val features = x._3

      val score = spamminess(features)
      val classification = if (score > 0) "spam" else "ham"
      (x._1, x._2, score, classification)
    })
  }

  def main(argv: Array[String]) {
    var input = ""
    var model = ""
    var output = ""

    for (i <- 0 to argv.length-1) {
      if (argv(i) equals "--input") {
        input = argv(i+1)
      } else if (argv(i) equals "--model") {
        model = argv(i+1)
      } else if (argv(i) equals "--output") {
        output = argv(i+1)
      }
    }

    println("Input: " + input)
    println("Model: " + model)
    println("Output: " + output)

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(output)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelRDD = sc.textFile(model + "/part-00000")
      .map(line => {
        val tokens = line.drop(1).dropRight(1).split(',')
        (tokens(0).toInt,tokens(1).toDouble)
      })
    val modelMap = sc.broadcast(modelRDD.collectAsMap())

    sc.textFile(input)
      .map(line => {
        //clueweb09-en0008-75-37022 spam 387908 697162 426572 161118 688171 43992 908749 126841
        val tokens = line.split(' ')
        val docid = tokens(0)
        val isSpam = tokens(1)
        val features = tokens.drop(2).map(_.toInt)
        (0, (docid, isSpam, features))
      })
      .groupByKey(1)
      .flatMap(x => {
        classify((x._1, x._2), modelMap.value)
      })
      .saveAsTextFile(output)
  }
}
