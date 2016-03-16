package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6

import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.Map

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def classify(feat : (Int, Iterable[(String, String, Array[Int])]), w1 : Map[Int, Double], w2 : Map[Int, Double], w3 : Map[Int, Double], method: String): Iterable[(String,String,Double,String)] = {
    // Scores a document based on its list of features.
    val isAveraging = method equals "average"

    def spamminess(features: Array[Int]) : Double = {
      var score = 0d

      var classifierScore = 0d
      features.foreach(f => if (w1.contains(f)) classifierScore += w1(f))
      score += (if (isAveraging) classifierScore/3.0 else (if (classifierScore > 0) 1 else -1))

      classifierScore = 0d
      features.foreach(f => if (w2.contains(f)) classifierScore += w2(f))
      score += (if (isAveraging) classifierScore/3.0 else (if (classifierScore > 0) 1 else -1))

      classifierScore = 0d
      features.foreach(f => if (w3.contains(f)) classifierScore += w3(f))
      score += (if (isAveraging) classifierScore/3.0 else (if (classifierScore > 0) 1 else -1))

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
    var method = ""

    for (i <- 0 to argv.length-1) {
      if (argv(i) equals "--input") {
        input = argv(i+1)
      } else if (argv(i) equals "--model") {
        model = argv(i+1)
      } else if (argv(i) equals "--output") {
        output = argv(i+1)
      } else if (argv(i) equals "--method") {
        method = argv(i+1)
      }
    }

    println("Input: " + input)
    println("Model: " + model)
    println("Output: " + output)
    println("Method: " + method)

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(output)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model1RDD = sc.textFile(model + "/part-00000")
      .map(line => {
        val tokens = line.drop(1).dropRight(1).split(',')
        (tokens(0).toInt,tokens(1).toDouble)
      })
    val model1Map = sc.broadcast(model1RDD.collectAsMap())

    val model2RDD = sc.textFile(model + "/part-00001")
      .map(line => {
        val tokens = line.drop(1).dropRight(1).split(',')
        (tokens(0).toInt,tokens(1).toDouble)
      })
    val model2Map = sc.broadcast(model2RDD.collectAsMap())

    val model3RDD = sc.textFile(model + "/part-00002")
      .map(line => {
        val tokens = line.drop(1).dropRight(1).split(',')
        (tokens(0).toInt,tokens(1).toDouble)
      })
    val model3Map = sc.broadcast(model3RDD.collectAsMap())

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
        classify((x._1, x._2), model1Map.value, model2Map.value, model3Map.value, method)
      })
      .saveAsTextFile(output)
  }
}
