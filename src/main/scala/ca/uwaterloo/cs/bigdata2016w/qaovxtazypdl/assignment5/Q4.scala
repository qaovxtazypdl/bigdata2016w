package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q4 {
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

    val conf = new SparkConf().setAppName("A5Q4")
    val sc = new SparkContext(conf)

    /*
    select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
    where
      l_orderkey = o_orderkey and
      o_custkey = c_custkey and
      c_nationkey = n_nationkey and
      l_shipdate = 'YYYY-MM-DD'
    group by n_nationkey, n_name
    order by n_nationkey asc;
    */

    //(orderkey, /)
    val lineItems = sc
      .textFile(input + "/lineitem.tbl")
      .filter(_.split('|')(10).startsWith(date))
      .map(line => {
        val tokens = line.split('|')
        (tokens(0).toInt, "")
      })

    //(orderkey, custkey)
    val orders = sc
      .textFile(input + "/orders.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0).toInt, tokens(1).toInt)
      })

    //(custkey, nationkey)
    val customers = sc
      .textFile(input + "/customer.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0).toInt, tokens(3).toInt)
      })

    //(nationkey, name)
    val nation = sc
      .textFile(input + "/nation.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0).toInt, tokens(1))
      })

    //nation: nationkey => listof name
    //join customers in => (custKey, (nationkey, name)) on nationkey
    //nationkey is one to many on customers -> hashjoin
    val nationMap = sc.broadcast(nation.collectAsMap())
    val customerNation = customers
      .filter(item => nationMap.value.getOrElse(item._2, None) != None)
      .map(item => (item._1, (item._2, nationMap.value.getOrElse(item._2, None).asInstanceOf[String])))

    //customernation : custKey => listof(nationkey, name)
    //join orders in => (orderkey, (nationkey, name)) on custkey
    //custkey is one to many on orders -> hashjoin
    val customerNationMap = sc.broadcast(customerNation.collectAsMap())
    val orderNations = orders
      .filter(item => customerNationMap.value.getOrElse(item._2, None) != None)
      .map(item => (item._1, customerNationMap.value.getOrElse(item._2, None).asInstanceOf[(Int, String)]))

    //neither result guaranteed to fit in memory - use reduce-side cogroup join
    //join lineitem in on orderkey
    lineItems
      .cogroup(orderNations)
      //cartesian join on elements of same key
      .flatMap(data => {
        data._2._1.flatMap(custNationItem => {
          data._2._2.map(lineItem => (lineItem, None))
        })
      })
      //aggregate
      .groupByKey()
      .map(keyIterable => (keyIterable._1._1, keyIterable._1._2, keyIterable._2.size))
      .sortBy(_._1.toInt)
      .collect()
      .foreach(println)
  }
}
