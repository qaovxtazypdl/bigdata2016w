package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object What {
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

    val conf = new SparkConf().setAppName("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
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
      .filter(_.split('|')(10).startsWith(date))
      .map(line => line.split('|')(4).toFloat)
      .reduce(_+_)
    println(lineItems)
  }
}
