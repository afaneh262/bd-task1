package searchengine

import org.apache.spark.sql.{SparkSession}
import org.apache.log4j._
import org.apache.log4j.varia.NullAppender
import TextUtils._
import org.apache.log4j.{Level, Logger}

object Comparison {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    Logger.getRootLogger.setLevel(Level.OFF)

    // Initialize Spark Session
    val spark = SparkSession
      .builder()
      .appName("Task-1")
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

    def trimText(text: String): String = {
      if (text.length > 50) text.take(50 - 3) + "..." else text
    }

    def formatTime(nanos: Long): String = f"${nanos / 1e6}%.2f ms"

    def formatResults(results: Map[String, List[Int]]): String = {
      results
        .map { case (doc, pos) => s"($doc, [${pos.mkString(", ")}])" }
        .mkString(" ")
    }

    // Spark RDD
    val invertedIndexRDD = sc.textFile("output/wholeInvertedIndex.txt")
    val rddAnalyzer = new RddTextAnalyzer(invertedIndexRDD)
    // Mongo
    val mongoAnalyze = new MongodbAnalyzer(spark)

    val queries = List(
      "play soccer",
      "soccer play",
      "play football",
      "play cricket",
      "soccer",
      "Tobias Gregson Shows What He Can Do",
      "Alkali Plain",
      "Avenging Angels",
      "flourishing distilleries",
      "who was usually very late in the mornings",
      "mornings who was usually very late in the",
      "The first thing that put us out was that advertisement",
      "Penang lawyer",
      "Your experience has been a most entertaining one"
    )

    queries.foreach { query =>
      val formattedText = formatText(query)
      println("=====================================")
      println("=====================================")
      println("Query: " + trimText(query))
      if (formattedText.length > 0) {
        val startRDD = System.nanoTime()
        val rddResults = rddAnalyzer.searchQuery(formattedText)
        val rddTime = System.nanoTime() - startRDD

        val startMongo = System.nanoTime()
        val mongoResults = mongoAnalyze.searchQuery(formattedText)
        val mongoTime = System.nanoTime() - startMongo
        println("RDD time: " + formatTime(rddTime))
        println("Mongo time: " + formatTime(mongoTime))
        println("RDD Result: " + formatResults(rddResults.collect().toMap))
        println("Mongo Result: " + formatResults(mongoResults))
      } else {
        println("hit invalid qiuery")
      }
    }

    // Stop Spark
    spark.stop()
  }
}
