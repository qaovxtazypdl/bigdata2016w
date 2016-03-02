package ca.uwaterloo.cs.bigdata2016w.qaovxtazypdl.assignment5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Q3 {
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

    val conf = new SparkConf().setAppName("A5Q3")
    val sc = new SparkContext(conf)

    /*
    select l_orderkey, p_name, s_name from lineitem, part, supplier
    where
      l_partkey = p_partkey and
      l_suppkey = s_suppkey and
      l_shipdate = 'YYYY-MM-DD'
    order by l_orderkey asc limit 20;
    */

    //(suppkey, partkey, orderkey)
    val lineItems = sc
      .textFile(input + "/lineitem.tbl")
      .filter(_.split('|')(10).startsWith(date))
      .map(line => {
        val tokens = line.split('|')
        (tokens(2), tokens(1), tokens(0))
      })

    //(suppkey, name)
    val suppliers = sc
      .textFile(input + "/supplier.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(1))
      })

    //(partkey, name)
    val parts = sc
      .textFile(input + "/part.tbl")
      .map(line => {
        val tokens = line.split('|')
        (tokens(0), tokens(1))
      })

    //broadcast suppliers
    val supplierEntries = sc.broadcast(suppliers.collectAsMap())

    //join lineitems into suppliers (partkey, (orderkey, s_name))
    val lineSuppliers = lineItems
      .filter(item => supplierEntries.value.getOrElse(item._1, None) != None)
      .map(item => (item._2, (item._3, supplierEntries.value.getOrElse(item._1, None).asInstanceOf[String])))

    val lineSupplierEntries = sc.broadcast(lineSuppliers.collectAsMap())

    //join parts in (orderkey, p_name, s_name)
    parts
      .filter(item => lineSupplierEntries.value.getOrElse(item._1, None) != None)
      .map(item => {
        val resultTuple = lineSupplierEntries.value.getOrElse(item._1, None).asInstanceOf[(String, String)]
        (resultTuple._1, item._2, resultTuple._2)
      })
      .sortBy(_._1.toInt)
      .take(20)
      .map(println)
  }
}
