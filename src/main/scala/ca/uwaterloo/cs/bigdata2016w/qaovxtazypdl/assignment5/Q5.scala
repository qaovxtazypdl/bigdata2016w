package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    var input = ""

    for (i <- 0 to argv.length-1) {
      if (argv(i) equals "--input") {
        input = argv(i+1)
      }
    }

    println("Input: " + input)

    val conf = new SparkConf().setAppName("A5Q5")
    val sc = new SparkContext(conf)

    //(orderkey, shipdate)
    val lineItems = sc
      .textFile(input + "/lineitem.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(10).substring(0, 7))
      })

    //(orderkey, custkey)
    val orders = sc
      .textFile(input + "/orders.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(1))
      })

    //(custkey, None)
    val customersCAN = sc
      .textFile(input + "/customer.tbl")
      .filter(line => line.split('|')(3).toInt == 3)
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), (tokens(3)))
      })

    val customersCANMap = sc.broadcast(customersCAN.collectAsMap())

    //customernation : custKey => listof(nationkey, name)
    //join orders in => (orderkey, (nationkey, name)) on custkey
    val ordersCAN = orders
      .flatMap(item => {
        val result = customersCANMap.value.getOrElse(item._2, None)
        if (result eq None) {
          List()
        } else {
          List((item._1, result.asInstanceOf[String]))
        }
      })

    //neither result guaranteed to fit in memory - use cogroup
    //join lineitem in on orderkey
    val itemsCAN = lineItems
      .cogroup(ordersCAN)
      .flatMap(data => {
        data._2._1.flatMap(month => {
          data._2._2.map(lineItem => ((lineItem, month), None))
        })
      })
      .groupByKey()
      .map(keyIterable => {
        val dateString = keyIterable._1._2.substring(5) + "-" + keyIterable._1._2.substring(0,4)
        (dateString, keyIterable._2.size)
      })



    //(custkey, None)
    val customersUSA = sc
      .textFile(input + "/customer.tbl")
      .filter(line => line.split('|')(3).toInt == 24)
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), (tokens(3)))
      })

    val customersUSAMap = sc.broadcast(customersUSA.collectAsMap())

    //customernation : custKey => listof(nationkey, name)
    //join orders in => (orderkey, (nationkey, name)) on custkey
    val ordersUSA = orders
      .flatMap(item => {
        val result = customersUSAMap.value.getOrElse(item._2, None)
        if (result eq None) {
          List()
        } else {
          List((item._1, result.asInstanceOf[String]))
        }
      })

    //neither result guaranteed to fit in memory - use cogroup
    //join lineitem in on orderkey
    val itemsUSA = lineItems
      .cogroup(ordersUSA)
      .flatMap(data => {
        data._2._1.flatMap(month => {
          data._2._2.map(lineItem => ((lineItem, month), None))
        })
      })
      .groupByKey()
      .map(keyIterable => {
        val dateString = keyIterable._1._2.substring(5) + "-" + keyIterable._1._2.substring(0,4)
        (dateString, keyIterable._2.size)
      })

    val data = itemsUSA
      .cogroup(itemsCAN)
      .flatMap(data => {
        data._2._1.flatMap(usaCount => {
          data._2._2.map(canCount => (data._1, usaCount, canCount))
        })
      })
      .sortBy(item => (item._1.substring(3), item._1.substring(0,2)))
      .collect()

    println("(MONTH, UNITED STATES, CANADA)")
    data.foreach(println)
  }
}
