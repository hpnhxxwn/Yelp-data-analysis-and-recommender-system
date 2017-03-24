/**
  * Created by hpnhxxwn on 2017/3/22.
  */
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
import scala.util.MurmurHash
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession


object YelpUserBusinessGraphAnalysis {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    // ====================================================================================
    val raw_nodes = sc.textFile("/Users/sundeepblue/Bootcamp/allweek/week9/capstone/data/yelp_data/userid_businessid_together.csv")
    println(raw_nodes.take(10).mkString("\n"))

    val raw_edges = sc.textFile("/Users/sundeepblue/Bootcamp/allweek/week9/capstone/data/yelp_data/userid_bizid_star_tuple_for_graph.csv")
    println(raw_edges.take(10).mkString("\n"))

    // ====================================================================================
    val vertexRDD: RDD[(Long, (String, Int))] = raw_nodes.map {
      line =>
        (line.toLong, ("node" + line, 28))
    }

    val edgeRDD: RDD[Edge[Int]] = raw_edges.map{ line =>
      val fields = line.split(",")
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toInt)
    }


    // ====================================================================================
    println(vertexRDD.take(10).mkString("\n"))
    println(edgeRDD.take(10).mkString("\n"))

    // ====================================================================================
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    graph.vertices.filter { case (id, (name, age)) => id < 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$id, $name is $age years old")
    }

    // ====================================================================================
    // long time to to run, but works!
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} rated ${triplet.dstAttr._1}")
    }

    // ====================================================================================
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    // Define a class to more clearly model the user property
    // User could mean a user, or a business
    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    // Create a user Graph
    val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }
    // Fill in the degree information
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }

    for ((id, property) <- userGraph.vertices.collect) {
      if (id < 1000000) {
        println(s"User $id is called ${property.name} and is rated by ${property.inDeg} people.")
      }
    }

    // ======================== connected components ======================================
    val goodGraph = userGraph.subgraph(vpred = (id, user) => user.inDeg > 0)
    val file = "/Users/sundeepblue/businessid_to_indegree.csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

    for ((id, user) <- goodGraph.vertices.collect) {
      println(s"User(or business) $id is rated by ${user.inDeg} people")
      writer.write(s"$id,${user.inDeg}\n")
    }
    writer.close()

    // compute the connected components
    val cc = goodGraph.connectedComponents

    // display the component id of each user
    goodGraph.vertices.leftJoin(cc.vertices) {
      case (id, user, comp) => s"${user.name} is in component ${comp.get}"
    }.collect.foreach{ case (id, str) => println(str) }

    // https://github.com/dougneedham/Cloudera-Data-Scientist-Challenge-3/tree/master/problem3
    // slides http://www.slideshare.net/DougNeedham1/apache-spark-graphx-highlights
    // https://stanford.edu/~rezab/sparkclass/slides/ankur_graphx.pdf

    // ====================================================================================

    spark.stop()
  }
}
// scalastyle:on println

