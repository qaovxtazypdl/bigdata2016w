package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q2 {
  val log = Logger.getLogger(getClass().getName())

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
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), "")
      })

    val orderTuples = sc
      .textFile(input + "/orders.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(6))
      })

    lineItemTuples.cogroup(orderTuples)
      .flatMap(data => {
        data._2._1.flatMap(lineItem => {
          data._2._2.map(orderItem => (orderItem, data._1))
        })
      })
      .sortBy(_._2.toInt)
      .take(20)
      .foreach(println)
  }
}
