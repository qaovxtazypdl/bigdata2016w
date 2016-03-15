package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment6

import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.Map
import scala.math._

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def classify(feat : (Int, Iterable[(String, Int, Array[Int])])): List[(Int, Double)] = {
    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]) : Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    // This is the main learner:
    val delta = 0.002

    // For each instance...
    feat._2.foreach(x => {
      val isSpam = x._2
      val features = x._3

      // Update the weights as follows:
      val score = spamminess(features)
      val prob = 1.0 / (1 + exp(-score))
      features.foreach(f => {
        if (w.contains(f)) {
          w(f) += (isSpam - prob) * delta
        } else {
          w(f) = (isSpam - prob) * delta
        }
      })
    })

    w.toList
  }

  def main(argv: Array[String]) {
    var input = ""
    var model = ""

    for (i <- 0 to argv.length-1) {
      if (argv(i) equals "--input") {
        input = argv(i+1)
      } else if (argv(i) equals "--model") {
        model = argv(i+1)
      }
    }

    println("Input: " + input)
    println("Model: " + model)

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    val outputDir = new Path(model)
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(input)

    textFile.map(line => {
      //clueweb09-en0008-75-37022 spam 387908 697162 426572 161118 688171 43992 908749 126841
      val tokens = line.split(' ')
      val docid = tokens(0)
      val isSpam = if (tokens(1) equals "spam") 1 else 0
      val features = tokens.drop(2).map(_.toInt)
      (0, (docid, isSpam, features))
    }).groupByKey(1)
    // Then run the trainer...
    .flatMap(classify).saveAsTextFile(model)
  }
}
