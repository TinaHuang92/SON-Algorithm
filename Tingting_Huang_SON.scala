import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.util.control.Breaks._
import collection.mutable.{HashMap, ListBuffer}
import scala.collection.mutable
import scala.collection.immutable
import scala.collection.mutable.{Set => mutSet}
import scala.util.Random

import java.io._

import org.apache.spark.sql._
import scala.util.control.Breaks._

/*
 written by Tingting Huang 02/08/2018
 */

object Tingting_Huang_SON {

  def main (args : Array[String]){

    val start_time = System.currentTimeMillis()

    // local[*] Run Spark locally with as many worker threads as logical cores on your machine.
    val conf = new SparkConf().setAppName("Tingting_Huang_SON").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val case_number = args(0).toInt
    //val case_number = 2
    val support_threshold = args(2).toDouble
    //val support_threshold = 1500.0
    val csv = sc.textFile(args(1)).cache()
    //val csv = sc.textFile("./books.csv").cache()   // original file

    class SimpleCSVHeader(header:Array[String])
      extends Serializable{
      val index = header.zipWithIndex.toMap
      def apply(array:Array[String],
                key:String)
      :String =array(index(key))
    }

    val data = csv.map(line => line.split(",").map(elem=>elem.trim))
    val header = new SimpleCSVHeader(data.take(1)(0))
    // 取出第一行来创建header
    val rows = data.filter(line => header(line,"reviewerID") != "reviewerID")//.collect() // 去掉header
    //val reviewer = rows.map(row => header(row,"reviewerID")).collect()
    //val product = rows.map(row => header(row,"reviewerID") -> header(row,"productID").toString).collect()
    //val product = rows.map(row => header(row,"productID") -> header(row,"reviewerID").toString).groupByKey().collect()
    //println(reviewer)
    //product.foreach( x => println(x) )


    val partitions = rows.getNumPartitions

    var basketsRDD : RDD[List[String]] = null

    if (case_number == 1) {
      val RowRDD = rows.map(row => header(row,"reviewerID") -> header(row,"productID").toString)
        .groupBy(_._1).mapValues(_.map(_._2)(collection.breakOut).toList.distinct).map(k => k._2)//.reduceByKey(_ ++ _).map(x => x._2.toString).map(x => x.toList.sorted)
      basketsRDD = RowRDD
    }

    else if (case_number == 2) {
      val RowRDD = rows.map(row => header(row,"productID") -> header(row,"reviewerID").toString)
        .groupBy(_._1).mapValues(_.map(_._2)(collection.breakOut).toList.distinct).map(k => k._2)
      basketsRDD = RowRDD
    }

    //baskets.foreach( x => println(x) )


    // 1st map phase
    val Map_1 = basketsRDD.mapPartitions(line => Apriori_Alg(line, support_threshold/partitions))
      .sortByKey().map{ case (x, y) => y }.flatMap( x => {for (item <- x) yield (item, (item, 1))})

    // 1st reduce phase
    val Reduce_1 = Map_1.reduceByKey( (x: (Set[String], Int), y: (Set[String], Int)) => (x._1, 1) ).map{ case (x, y) => y }
    val candidatePair = Reduce_1.map(y => y._1).collect().toList
    val broadcastVar = sc.broadcast(candidatePair)

    // 2st map phase
    val Map_2 = basketsRDD.flatMap( baskets =>
      { var occurrence = ListBuffer.empty[Tuple2[Set[String], Int]]
        for (occur <- broadcastVar.value) {
          if (occur.subsetOf(baskets.toSet)) {
            occurrence += Tuple2(occur, 1)
          } else -1
        }
        occurrence.toList
      } )

    // 2st reduce phase
    val Reduce_2 = Map_2.reduceByKey( (x, y) => x+y )

    val frequentItemsets = Reduce_2.filter(x => (x._2 >= support_threshold)).map{ case (x, y) => (x.size, List(x.toList.sorted)) }
        .reduceByKey( (x: List[List[String]], y: List[List[String]]) => x ++: y ).coalesce(1)
          .map{ case (x, y) => (x, Sort_function(y)) }.sortBy(_._1)
          .map{ case (x, y) => {
            var res = ListBuffer.empty[String]
            for (item <- y) {
              res += item.map( x => "'" + x + "'").mkString("(",",",")")
            }
            res.toList
          } }


    val pw = new PrintWriter(new File("Tingting_Huang_SON_" + args(1).toString + ".case"+ args(0) + "-" + args(2) +".txt" ))
    //val pw = new PrintWriter(new File("result.txt" ))
    val output = frequentItemsets.map( item => item.mkString(",") ).collect()
    for (line <- output) {
      pw.write(line + "\n"+"\n")
    }
    pw.close

    val end_time = System.currentTimeMillis()
    println("Total Execution Time: " + (end_time - start_time)/1000 + " secs")

  }

  //https://stackoverflow.com/questions/3137918/how-do-i-sort-a-collection-of-lists-in-lexicographic-order-in-scala
  //sort a collection of Lists in lexicographic order in Scala
  def Sort_function[A](coll: Seq[Iterable[A]])(implicit ordering: Ordering[A]) = coll.sorted


  def Apriori_Alg(chunk: Iterator[List[String]], support : Double) : Iterator[(Int, mutable.Set[Set[String]])] = {
    var singletons_map = HashMap.empty[String, Int]
    var itemsets = ListBuffer.empty[Set[String]]
    var freqItemset = mutable.Set[Set[String]]()
    var res = HashMap.empty[Int, mutable.Set[Set[String]]]

    //var k = 1
    while (chunk.hasNext) {
      val itemset = chunk.next()
      itemsets += itemset.toSet
      for (singleton <- itemset) {
        if (singletons_map.contains(singleton)) {
          singletons_map(singleton) += 1
        }
        else {
          singletons_map += (singleton -> 1)
        }
      }
    }

    for (singleton <- singletons_map) {
      if (singleton._2 >= support) {
        freqItemset += Set(singleton._1)
      }
    }

    var k = 2
    if (!res.contains(k-1)) {
      res += (k-1 -> freqItemset)
    }

    while(freqItemset.isEmpty == false) {
      //freqItemset = Generate_ktuple(freqItemset,k)
      val candidate_sets = countOccurence(Generate_ktuple(freqItemset,k), itemsets, support)
      if (candidate_sets.isEmpty == false) {
        res += (k -> candidate_sets)
      }
      freqItemset = candidate_sets.clone()
      k = k + 1
    }
    return res.iterator
  }



  def Generate_ktuple(itemset : mutable.Set[Set[String]], k : Int) : mutable.Set[Set[String]] = {
    var singletons = mutSet.empty[String]
    for (item <- itemset) {
      for (i <- item){
        singletons += i
      }
    }
    var itemset_combination = mutSet.empty[Set[String]]
    for (item <- itemset) {
      for (singleton <- singletons) {
        val newItem = mutSet() ++ item
        newItem += singleton
        if (newItem.size == k) {
          val examine_list = newItem.subsets(k-1)
          var flag = true

          if (k > 2) {
            for (subset <- examine_list) {
              if (!itemset.contains(subset.toSet)){
                flag = false
              } else -1
            }
          }

          if(flag) {
            itemset_combination += newItem.toSet
          } else -1
        } else -1
      }
    }
    return itemset_combination
  }


  def countOccurence(candidate_itemset: mutable.Set[Set[String]], baskets: ListBuffer[Set[String]], support: Double) : mutable.Set[Set[String]] = {
    var itemsets = ListBuffer.empty[Set[String]]
    for (itemset <- baskets) {
      itemsets += itemset//.toSet
    }
    var map = mutable.HashMap.empty[Set[String], Int]
    for (item <- candidate_itemset){
      for (itemset <- itemsets) {
        if (item.subsetOf(itemset)) {
          if (map.contains(item)) {
            map(item) += 1
          }
          else {
            map += (item -> 1)
          }
        } else -1
      }
    }
    var occurence_set = mutable.Set[Set[String]]()
    for (item <- map) {
      if (item._2 >= support) {
        occurence_set += item._1
      } else -1
    }
    return occurence_set
  }


}

