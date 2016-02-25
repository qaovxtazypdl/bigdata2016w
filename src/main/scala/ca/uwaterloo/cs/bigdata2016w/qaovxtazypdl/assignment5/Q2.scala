package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def getTupleFromString(line : String): (String, String) = {
    val (key, value) = line.split('|').splitAt(1)
    (key(0), value.mkString("|"))
  }

  def main(argv: Array[String]) {
    var input = ""
    var date = ""

    for (i <- 0 to argv.length-1) {
      if (argv(i) equals "--input") {
        input = argv(i+1)
      } else if (argv(i) equals "--date") {
        date = argv(i+1)
      }
    }

    println("Input: " + input)
    println("Date: " + date)

    val conf = new SparkConf().setAppName("A5Q2")
    val sc = new SparkContext(conf)

    /*
    select o_clerk, o_orderkey from lineitem, orders
    where
      l_orderkey = o_orderkey and
      l_shipdate = 'YYYY-MM-DD'
    order by o_orderkey asc limit 20;
    */

    val lineItemTuples = sc
      .textFile(input + "/lineitem.tbl")
      .filter(_.split('|')(10).equals(date))
      .map(getTupleFromString)
    val orderTuples = sc
      .textFile(input + "/orders.tbl")
      .map(getTupleFromString)

    lineItemTuples.cogroup(orderTuples)
      .filter(_._2._1.size != 0)
      .flatMap(data => data._2._2.map(x => (x.split('|')(5), data._1)).toList)
      .sortBy(_._2)
      .take(20)
      .map(println)
  }
}
