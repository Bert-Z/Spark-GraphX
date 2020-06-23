package lab

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.storage.StorageLevel

object Google {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      val usage =
        """Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions>
          |[other options] Supported 'taskType' as follows:
          |pagerank    Compute PageRank
          |sssp        Compute Single Source Shortest Path using Pregel""".stripMargin
      System.err.println(usage)
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException(s"Invalid argument: $arg")
      }
    }
    val options = mutable.Map(optionsList: _*)

    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val spark = SparkSession
          .builder
          .appName(s"${this.getClass.getSimpleName}")
          .config("spark.master","local")
          .getOrCreate()
        val sc = spark.sparkContext

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println(s"GRAPHX: Number of vertices ${graph.vertices.count}")
        println(s"GRAPHX: Number of edges ${graph.edges.count}")

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println(s"GRAPHX: Total rank: ${pr.map(_._2).reduce(_ + _)}")

        if (!outFname.isEmpty) {
          println(s"Saving pageranks of pages to $outFname")
          pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        }

        sc.stop()

      case "sssp" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException(s"Invalid option: $opt")
        }

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
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))
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
        sc.stop()
      case _ =>
        println("Invalid task type.")
    }
  }
}