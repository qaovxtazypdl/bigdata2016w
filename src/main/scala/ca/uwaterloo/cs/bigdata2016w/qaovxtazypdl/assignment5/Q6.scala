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

    //(returnflag, linestatus) => (quantity, extendedprice, discount, tax, discount)
    val lineItems = sc
        .textFile(input + "/lineitem.tbl")
        //push down filter
        .filter(_.split('|')(10).startsWith(date))
        //then compute relevant aggeregate entries
        .map(line => {
          val tokens = line.split('|')
          val extended = tokens(5).toFloat
          val discount = tokens(6).toFloat
          ((tokens(8), tokens(9)), (tokens(4).toFloat, extended, extended*(1-discount), extended*(1-discount)*(1+tokens(7).toFloat), discount))
        })
        //group by the aggregate key
        .groupByKey()
        //do the aggregation
        .map(keyValuePair => {
          val count = keyValuePair._2.size
          val sums = Array(0.0f,0.0f,0.0f,0.0f,0.0f) //quantity extended disc charge discount

          val arr = keyValuePair._2.toIterator
          for (i <- count % 8 until count by 8) {
            val l1 = arr.next()
            val l2 = arr.next()
            val l3 = arr.next()
            val l4 = arr.next()
            val l5 = arr.next()
            val l6 = arr.next()
            val l7 = arr.next()
            val l8 = arr.next()


            sums(0) += l1._1
            sums(1) += l1._1
            sums(2) += l1._1
            sums(3) += l1._1
            sums(4) += l1._2
            sums(0) += l2._1
            sums(1) += l2._1
            sums(2) += l2._1
            sums(3) += l2._1
            sums(4) += l2._2
            sums(0) += l3._1
            sums(1) += l3._1
            sums(2) += l3._1
            sums(3) += l3._1
            sums(4) += l3._2
            sums(0) += l4._1
            sums(1) += l4._1
            sums(2) += l4._1
            sums(3) += l4._1
            sums(4) += l4._2
            sums(0) += l5._1
            sums(1) += l5._1
            sums(2) += l5._1
            sums(3) += l5._1
            sums(4) += l5._2
            sums(0) += l6._1
            sums(1) += l6._1
            sums(2) += l6._1
            sums(3) += l6._1
            sums(4) += l6._2
            sums(0) += l7._1
            sums(1) += l7._1
            sums(2) += l7._1
            sums(3) += l7._1
            sums(4) += l7._2
            sums(0) += l8._1
            sums(1) += l8._1
            sums(2) += l8._1
            sums(3) += l8._1
            sums(4) += l8._2
          }
          while (arr.hasNext) {
            val el = arr.next()
            sums(0) += el._1
            sums(1) += el._2
            sums(2) += el._3
            sums(3) += el._4
            sums(4) += el._5
          }

/*
(running time is actually about the same...)
          keyValuePair._2.foreach(tuple => {
            sums(0) += tuple._1
            sums(1) += tuple._2
            sums(2) += tuple._3
            sums(3) += tuple._4
            sums(4) += tuple._5
          })
*/

          (keyValuePair._1._1, keyValuePair._1._2, sums(0), sums(1), sums(2), sums(3), sums(0)/count, sums(1)/count, sums(4)/count, count)
        })
        .collect()
        .foreach(println)
  }
}
