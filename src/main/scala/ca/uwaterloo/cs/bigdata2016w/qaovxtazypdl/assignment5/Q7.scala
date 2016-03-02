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
      .flatMap(line => {
        val tokens = line.split('|')
        if (tokens(10) > date)
          List((tokens(0).toInt, tokens(5).toDouble * (1-tokens(6).toDouble)))
        else List()
      })

    //(custkey, name)
    val customers = sc
      .textFile(input + "/customer.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0).toInt, tokens(1))
      })

    //customers : custKey => listof(name)
    //(orderkey ,orderdate, shippriority, custkey)
    //join orders in => (orderkey, (orderdate, shippriority, name)) on custkey
    //customer key one to many to orders, fits in mem -> hashjoin
    val customerMap = sc.broadcast(customers.collectAsMap())
    val orderCustomers = sc
      .textFile(input + "/orders.tbl")
      .flatMap(item => {
        val tokens = item.split('|')
        val itemdate = tokens(4)
        val tok4 = tokens(1).toInt
        val cmap = customerMap.value
        if (cmap.contains(tok4) && itemdate < date) List((tokens(0).toInt, (itemdate, tokens(7).toInt, cmap(tok4)))) else List()
      })

    //join lineitem in on orderkey
    //lineitem: (orderkey, (discount, extendedprce))
    //ordercustomers: (orderkey, (orderdate, shippriority, name))
    //neither table guaranteed to fit in memory -> cogroup reduce side join
    //join result: (name, orderkey, orderdate, shippriority) => price*discount
    sc
      .textFile(input + "/lineitem.tbl")
      .flatMap(line => {
        val tokens = line.split('|')
        if (tokens(10) > date)
          List((tokens(0).toInt, tokens(5).toDouble * (1-tokens(6).toDouble)))
        else List()
      })
      .cogroup(orderCustomers)
      .flatMap(data => {
        data._2._2.map(orderItemEntry => {
          (orderItemEntry._3, data._1, data._2._1.sum, orderItemEntry._1, orderItemEntry._2)
        })
      })
      .sortBy(-_._3)
      .take(10)
      .foreach(println)
  }
}
