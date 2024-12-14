package searchengine

import org.apache.spark.sql.{SparkSession}
import org.apache.log4j._
import org.apache.log4j.varia.NullAppender

object Comparison {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("Task-1")
      // .master("spark://spark-master:7077")
      .master("local[*]")
      .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://root:example@localhost:27017"
      )
      .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://root:example@localhost:27017"
      )
      .getOrCreate()

    val sc = spark.sparkContext

    def formatTime(nanos: Long): String = f"${nanos / 1e6}%.2f ms"

    def formatResults(results: Map[String, List[Int]]): String = {
      results
        .map { case (doc, pos) => s"($doc, [${pos.mkString(", ")}])" }
        .mkString("\n")
    }

    // Spark RDD
    val invertedIndexRDD = sc.textFile("output/wholeInvertedIndex.txt")
    val rddAnalyzer = new RddTextAnalyzer(invertedIndexRDD)
    // Mongo
    val mongoAnalyze = new MongodbAnalyzer(spark)

    val queries = List("play soccer")
    val headers =
      Seq("Query", "RDD Time", "Mongo Time", "Mongo Result", "RDD Result")

    val rows = queries.map { query =>
      val startRDD = System.nanoTime()
      val rddResults = rddAnalyzer.searchQuery(query)
      val rddTime = System.nanoTime() - startRDD

      val startMongo = System.nanoTime()
      val mongoResults = mongoAnalyze.searchQuery(query)
      val mongoTime = System.nanoTime() - startMongo

      Seq(
        query,
        formatTime(rddTime),
        formatTime(mongoTime),
        formatResults(mongoResults),
        formatResults(rddResults)
      )
    }

    println(TableFormatter.formatTable(headers, rows))

    // Stop Spark
    spark.stop()
  }
}

// Table formatter
object TableFormatter {

  def formatTable(headers: Seq[String], rows: Seq[Seq[String]]): String = {
    val columnWidths = (headers +: rows).transpose.map(_.map(_.length).max)
    val border = columnWidths.map("-" * _).mkString("+", "+", "+")
    val formatRow = (row: Seq[String]) =>
      row
        .zip(columnWidths)
        .map { case (cell, width) =>
          cell.padTo(width, ' ')
        }
        .mkString("|", "|", "|")

    val formattedTable =
      (Seq(border, formatRow(headers), border) ++ rows.map(formatRow) :+ border)
        .mkString("\n")
    formattedTable
  }

}
