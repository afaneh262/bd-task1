package searchengine

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.log4j.varia.NullAppender

import org.apache.log4j.{Level, Logger}
import TextUtils._

object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    Logger.getRootLogger.setLevel(Level.ERROR)

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

    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    val filesPath = "data/*.txt"
    val documents: RDD[(String, String)] = sc.wholeTextFiles(filesPath)

    val invertedIndex: RDD[(String, (Int, List[(String, List[Int])]))] =
      documents
        .flatMap { case (docPath, content) =>
          val docName = docPath.split("/").last
          content.split("\\s+").zipWithIndex.collect { case (word, pos) =>
            val validationResult = isValidWord(word)
            if (validationResult.isValid) {
              Some((validationResult.result, (docName, pos)))
            } else {
              None
            }
          }
        }
        .flatMap(x => x)
        .groupByKey()
        .mapValues { occurrences =>
          val docWithPositions = occurrences
            .groupBy(_._1)
            .map { case (docName, positions) =>
              (docName, positions.map(_._2).toList.sorted)
            }
            .toList
          (docWithPositions.size, docWithPositions.sortBy(_._1))
        }
        .sortByKey()

    invertedIndex
      .map { case (word, (count, docs)) =>
        val docsFormatted = docs
          .map { case (doc, positions) =>
            s"($doc, [${positions.mkString(", ")}])"
          }
          .mkString(", ")
        s"$word, $count, [$docsFormatted]"
      }
      .saveAsTextFile("output/wholeInvertedIndex.txt")

    val mongodbAnalyzer = new MongodbAnalyzer(spark)
    mongodbAnalyzer.processAndSave(invertedIndex)

    // Stop Spark
    spark.stop()
  }
}
