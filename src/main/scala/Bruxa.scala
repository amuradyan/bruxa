/**
  * Created by spectrum on Jun, 2018
  */

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

import scala.collection.mutable.ListBuffer
import scala.math.BigInt

case class DBLPEdge(srcId: Int, dstId: Int, attr: Long)

case class DBLPEntry(val title: String, val authors: String, val year: BigInt) {
  def isValid = {
    title != null && title.nonEmpty && authors != null && authors.nonEmpty && authors != "[]" && year != null
  }
}

class Bruxa

object Bruxa {
  private val conf = ConfigFactory.load()
  private val datafile = conf.getString("app.datafile")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local[4]").
      appName("Bruxa").
      config("spark.app.id", "Bruxa").
      getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    val dblpDF = spark.read.json(datafile).as[DBLPEntry]

    val edgeArray = dblpDF.filter {
      _.isValid
    }.map {
      row => {
        val gEdges = new ListBuffer[DBLPEdge]()

        val edges = row.authors.
          replace("[", "").
          replace("]", "").
          split(", ")

        if (edges.length > 1) {
          val combinations = edges.combinations(2).map { case Array(a, b) => (a, b) }.toList

          combinations foreach {
            c => {
              gEdges += DBLPEdge(c._1.hashCode, c._2.hashCode, edges.length)
            }
          }
        }

        gEdges.toList
      }
    }.filter {
      _.length > 0
    }.select(explode($"value").as("edges")).
      select($"edges.srcId", $"edges.dstId", $"edges.attr").as[Edge[Long]].
      collect()

    val edgesRDD = sc.parallelize(edgeArray)
    val graph = Graph.fromEdges(edgesRDD, 1L)
    val vertices = graph.vertices.map(_._1).collect()
    val res = ShortestPaths.run(graph, vertices)

    res.edges.toDF().show(false)
  }
}
