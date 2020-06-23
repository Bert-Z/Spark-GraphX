package lab

import org.apache.spark.graphx.{GraphLoader, VertexId}
import org.apache.spark.sql.SparkSession


object Wiki {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      val usage =
        """Usage: Analytics <taskType> <file>
          |[other options] Supported 'taskType' as follows:
          |pagerank    Compute PageRank
          |sssp        Compute Single Source Shortest Path using Pregel""".stripMargin
      System.err.println(usage)
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)

    taskType match {
      case "pagerank" =>

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        // Creates a SparkSession.
        val spark = SparkSession
          .builder
          .appName(s"${this.getClass.getSimpleName}")
          .config("spark.master","local")
          .getOrCreate()
        val sc = spark.sparkContext
        val graph = GraphLoader.edgeListFile(sc, fname)
        // Run PageRank
        val ranks = graph.pageRank(0.0001).vertices
        // Print the result
        println(ranks.collect().mkString("\n"))
        // $example off$
        spark.stop()

      case "sssp" =>

        println("======================================")
        println("|   Single Source Shortest Path      |")
        println("======================================")

        // Creates a SparkSession.
        val spark = SparkSession
          .builder
          .appName(s"${this.getClass.getSimpleName}")
          .config("spark.master","local")
          .getOrCreate()
        val sc = spark.sparkContext

        // $example on$
        // A graph with edge attributes containing distances
        val graph = GraphLoader.edgeListFile(sc, fname)
        val sourceId: VertexId = 42 // The ultimate source
        // Initialize the graph such that all vertices except the root have distance infinity.
        val initialGraph = graph.mapVertices((id, _) =>
          if (id == sourceId) 0.0 else Double.PositiveInfinity)
        val sssp = initialGraph.pregel(Double.PositiveInfinity)(
          (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
          triplet => {  // Send Message
            if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
              Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
            } else {
              Iterator.empty
            }
          },
          (a, b) => math.min(a, b) // Merge Message
        )
        println(sssp.vertices.collect.mkString("\n"))
        // $example off$

        spark.stop()
      case _ =>
        println("Invalid task type.")
    }
  }
}
