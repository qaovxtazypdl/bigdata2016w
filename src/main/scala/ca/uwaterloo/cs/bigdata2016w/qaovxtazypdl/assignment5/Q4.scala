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
      .flatMap(line => {
        val tokens = line.split('|')
        if (tokens(10).startsWith(date)) List((tokens(0).toInt, "")) else List()
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
      .flatMap(item => {
        val nmap = nationMap.value
        if (nmap.contains(item._2)) List((item._1, (item._2, nmap(item._2)))) else List()
      })

    //customernation : custKey => listof(nationkey, name)
    //join orders in => (orderkey, (nationkey, name)) on custkey
    //custkey is one to many on orders -> hashjoin
    val customerNationMap = sc.broadcast(customerNation.collectAsMap())
    val orderNations = orders
      .filter(item => customerNationMap.value.contains(item._2))
      .flatMap(item => {
        val cnmap = customerNationMap.value
        if (cnmap.contains(item._2)) List((item._1, cnmap(item._2))) else List()
      })

    //neither result guaranteed to fit in memory - use reduce-side cogroup join
    //join lineitem in on orderkey
    lineItems
      .cogroup(orderNations)
      //cartesian join on elements of same key
      .flatMap(data => {
        data._2._1.flatMap(lineItem => {
          data._2._2.map(custNationItem => (custNationItem, 1))
        })
      })
      //aggregate
      .reduceByKey(_+_)
      .collect()
      .sortBy(_._1._1)
      .foreach(x => println((x._1._1, x._1._2, x._2)))
  }
}
