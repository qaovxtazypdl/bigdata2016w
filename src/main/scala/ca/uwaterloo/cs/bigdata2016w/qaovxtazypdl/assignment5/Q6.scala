package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q6 {
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

    val conf = new SparkConf().setAppName("A5Q6")
    val sc = new SparkContext(conf)

    /*
    select
      l_returnflag,
      l_linestatus,
      sum(l_quantity) as sum_qty,
      sum(l_extendedprice) as sum_base_price,
      sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
      sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
      avg(l_quantity) as avg_qty,
      avg(l_extendedprice) as avg_price,
      avg(l_discount) as avg_disc,
      count(*) as count_order
    from lineitem
    where
      l_shipdate = 'YYYY-MM-DD'
    group by l_returnflag, l_linestatus;
    */

    //(returnflag, linestatus) => (quantity, extendedprice, discount, tax)
    /*
    val lineItems = sc
      .textFile(input + "/lineitem.tbl")
      .filter(_.split('|')(10).startsWith(date))
      .map(line => {
        val tokens = line.split('|')
        ((tokens(8), tokens(9)), (tokens(4).toFloat, tokens(5).toFloat, tokens(6).toFloat, tokens(7).toFloat))
      })
      .groupByKey()
      .map(keyValuePair => {
        val count = keyValuePair._2.size
        val sums = Array(0.0f,0.0f,0.0f,0.0f,0.0f) //quantity extended disc charge discount
        keyValuePair._2.foreach(tuple => {
          sums(0) += tuple._1
          sums(1) += tuple._2
          sums(2) += tuple._2 * (1-tuple._3)
          sums(3) += tuple._2 * (1-tuple._3) * (1+tuple._4)
          sums(4) += tuple._3
        })
        (keyValuePair._1._1, keyValuePair._1._2, sums(0), sums(1), sums(2), sums(3), sums(0)/count, sums(1)/count, sums(4)/count, count)
      })
      .collect()
      .foreach(println)
      */


    //(returnflag, linestatus) => (quantity, extendedprice, discount, tax, discount)
    val lineItems = sc
        .textFile(input + "/lineitem.tbl")
        .filter(_.split('|')(10).startsWith(date))
        .map(line => {
          val tokens = line.split('|')

          val extended = tokens(5).toFloat
          val discount = tokens(6).toFloat

          ((tokens(8), tokens(9)), (tokens(4).toFloat, extended, extended*(1-discount), extended*(1-discount)*(1+tokens(7).toFloat), discount))
        })
        .groupByKey()
        .map(keyValuePair => {
          val count = keyValuePair._2.size
          val sums = Array(0.0f,0.0f,0.0f,0.0f,0.0f) //quantity extended disc charge discount
          keyValuePair._2.foreach(tuple => {
            sums(0) += tuple._1
            sums(1) += tuple._2
            sums(2) += tuple._3
            sums(3) += tuple._4
            sums(4) += tuple._5
          })
          (keyValuePair._1._1, keyValuePair._1._2, sums(0), sums(1), sums(2), sums(3), sums(0)/count, sums(1)/count, sums(4)/count, count)
        })
        .collect()
        .foreach(println)
  }
}
