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
        //pipeline filter and select
        .flatMap(line => {
          val tokens = line.split('|')
          val extended = tokens(5).toFloat
          val discount = tokens(6).toFloat
          if (tokens(10).startsWith(date)) //intermediate aggregate columns
            List(((tokens(8), tokens(9)), Array(tokens(4).toFloat, extended, extended*(1-discount), extended*(1-discount)*(1+tokens(7).toFloat), discount, 1)))
          else List()
        })
        .reduceByKey((acc, lst) => {
          acc(0) += lst(0)
          acc(1) += lst(1)
          acc(2) += lst(2)
          acc(3) += lst(3)
          acc(4) += lst(4)
          acc(5) += lst(5)
          acc
        })
        //very low amount of tuples being mapped => efficient aggregation
        .collect()
        .foreach(v => println((v._1._1, v._1._2, v._2(0), v._2(1), v._2(2), v._2(3), v._2(0)/v._2(5), v._2(1)/v._2(5), v._2(4)/v._2(5), v._2(5))))
  }
}
