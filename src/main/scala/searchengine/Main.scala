package searchengine

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.log4j.varia.NullAppender

import java.io.PrintWriter
import org.apache.log4j.{Level, Logger}

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
    val stopWords = Set(
      "a",
      "an",
      "the",
      "and",
      "or",
      "but",
      "if",
      "in",
      "on",
      "with",
      "as",
      "by",
      "for",
      "to",
      "of",
      "at",
      "from",
      "up",
      "down",
      "out",
      "over",
      "under",
      "again",
      "further",
      "then",
      "once"
    )

    val invertedIndex: RDD[(String, (Int, List[(String, List[Int])]))] =
      documents
        .flatMap { case (docPath, content) =>
          val docName = docPath.split("/").last
          content.split("\\s+").zipWithIndex.collect { case (word, pos) =>
            val validationResult = isValidWord(word, stopWords)
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

    val content = invertedIndex
      .map { case (word, (count, docs)) =>
        val docsFormatted = docs
          .map { case (doc, positions) =>
            s"($doc, [${positions.mkString(", ")}])"
          }
          .mkString(", ")
        s"$word, $count, [$docsFormatted]"
      }
      .collect()
      .mkString("\n")

    new PrintWriter("output/wholeInvertedIndex.txt") {
      write(content)
      close()
    }

    val mongodbAnalyzer = new MongodbAnalyzer(spark)
    mongodbAnalyzer.processAndSave(invertedIndex)

    // Stop Spark
    spark.stop()
  }

  case class ValidationResult(isValid: Boolean, result: String)

  def isValidWord(word: String, stopWords: Set[String]): ValidationResult = {
    val normalizedWord = normalizeWord(word)
    val stem = stemWord(normalizedWord)
    ValidationResult(stem.length > 2 && !stopWords.contains(stem), stem)
  }

  def normalizeWord(word: String): String = {
    word.replaceAll("[^a-zA-Z]", "").toLowerCase
  }

  def stemWord(word: String): String = {
    var stem = word.toLowerCase
    return stem

    if (stem.endsWith("sses")) {
      stem = stem.dropRight(2)
    } else if (stem.endsWith("ies")) {
      stem = stem.dropRight(3) + "y"
    } else if (stem.endsWith("ss")) {
      stem = stem
    } else if (stem.endsWith("s")) {
      stem = stem.dropRight(1)
    }

    if (stem.endsWith("eed")) {
      if (stem.dropRight(3).exists(isVowel)) {
        stem = stem.dropRight(1)
      }
    } else if (
      (stem.endsWith("ed") || stem
        .endsWith("ing")) && stem.dropRight(2).exists(isVowel)
    ) {
      stem = stem.dropRight(2)
      if (stem.endsWith("at") || stem.endsWith("bl") || stem.endsWith("iz")) {
        stem = stem + "e"
      } else if (
        endsWithDoubleConsonant(stem) && !Set('l', 's', 'z').contains(stem.last)
      ) {
        stem = stem.dropRight(1)
      } else if (isShortWord(stem)) {
        stem = stem + "e"
      }
    }

    if (stem.endsWith("y") && stem.dropRight(1).exists(isVowel)) {
      stem = stem.dropRight(1) + "i"
    }

    stem
  }

  def isVowel(c: Char): Boolean = "aeiou".contains(c)

  def endsWithDoubleConsonant(word: String): Boolean = {
    word.length > 1 && word.last == word(word.length - 2) && !isVowel(word.last)
  }

  def isShortWord(word: String): Boolean = {
    word.length == 2 && isVowel(word.head) && !isVowel(word.last)
  }
}
