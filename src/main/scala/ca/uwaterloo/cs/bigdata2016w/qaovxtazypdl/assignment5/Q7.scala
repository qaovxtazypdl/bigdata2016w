package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.Ordering._


object Q7 {
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

    val conf = new SparkConf().setAppName("A5Q7")
    val sc = new SparkContext(conf)

    /*
    select
      c_name,
      l_orderkey,
      sum(l_extendedprice*(1-l_discount)) as revenue,
      o_orderdate,
      o_shippriority
    from customer, orders, lineitem
    where
      c_custkey = o_custkey and
      l_orderkey = o_orderkey and
      o_orderdate < "YYYY-MM-DD" and
      l_shipdate > "YYYY-MM-DD"
    group by
      c_name,
      l_orderkey,
      o_orderdate,
      o_shippriority
    order by
      revenue desc
      limit 10;
    */

    //(orderkey, (discount, extendedprce))
    val lineItems = sc
      .textFile(input + "/lineitem.tbl")
      .filter(_.split('|')(10) > date)
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), (tokens(6).toFloat, tokens(5).toFloat))
      })

    //(orderkey ,orderdate, shippriority, custkey)
    val orders = sc
      .textFile(input + "/orders.tbl")
      .filter(_.split('|')(4) < date)
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(4), tokens(7), tokens(1))
      })

    //(custkey, name)
    val customers = sc
      .textFile(input + "/customer.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(1))
      })

    //customers : custKey => listof(name)
    //(orderkey ,orderdate, shippriority, custkey)
    //join orders in => (orderkey, (orderdate, shippriority, name)) on custkey
    //customer key one to many to orders, fits in mem -> hashjoin
    val customerMap = sc.broadcast(customers.collectAsMap())
    val orderCustomers = orders
      .filter(item => customerMap.value.getOrElse(item._4, None) != None)
      .map(item => (item._1, (item._2, item._3, customerMap.value.getOrElse(item._4, None).asInstanceOf[String])))

    //join lineitem in on orderkey
    //lineitem: (orderkey, (discount, extendedprce))
    //ordercustomers: (orderkey, (orderdate, shippriority, name))
    //neither table guaranteed to fit in memory -> cogroup reduce side join
    //join result: (name, orderkey, orderdate, shippriority) => price*discount
    lineItems
      .cogroup(orderCustomers)
      .flatMap(data => {
        data._2._1.flatMap(lineItemEntry => {
          data._2._2.map(orderItemEntry => ((orderItemEntry._3, data._1, orderItemEntry._1, orderItemEntry._2), lineItemEntry._2 * (1-lineItemEntry._1)))
        })
      })
      .groupByKey()
      .map(keyIterable => (keyIterable._1._1, keyIterable._1._2, keyIterable._2.sum, keyIterable._1._3, keyIterable._1._4))
      .takeOrdered(10)(Ordering.by(-_._3))
      .foreach(println)
  }
}
