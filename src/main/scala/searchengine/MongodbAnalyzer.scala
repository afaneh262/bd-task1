package searchengine

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

class MongodbAnalyzer(
    spark: SparkSession
) {

  def processAndSave(
      invertedIndexRDD: RDD[(String, (Int, List[(String, List[Int])]))]
  ): Unit = {
    val postingSchema = StructType(
      Seq(
        StructField("docId", StringType, false),
        StructField("positions", ArrayType(IntegerType, false), false)
      )
    )

    val schema = StructType(
      Seq(
        StructField("word", StringType, false),
        StructField("docFrequency", IntegerType, false),
        StructField("postings", ArrayType(postingSchema, false), false)
      )
    )

    val df = spark.createDataFrame(
      invertedIndexRDD.map { case (word, (docFreq, postings)) =>
        Row(
          word,
          docFreq,
          postings.map { case (docId, positions) =>
            Row(docId, positions.toSeq)
          }.toSeq
        )
      },
      schema
    )

    val mongoDF = df
      .withColumn("_id", col("word"))
      .drop("word")

    mongoDF.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", "wholeInvertedIndex")
      .option("collection", "words")
      .save()
  }

  def searchQuery(searchPhrase: String): Map[String, List[Int]] = {
    val searchTerms = searchPhrase.split(" ").map(_.trim).filter(_.nonEmpty)
    val searchTermsArray = searchTerms.mkString("[\"", "\", \"", "\"]")

    val wordsDF = spark.read
      .format("mongodb")
      .option("database", "wholeInvertedIndex")
      .option("collection", "words")
      .option(
        "pipeline",
        s"""[{ "$$match": { "_id": { "$$in": $searchTermsArray } } }]"""
      )
      .load()
      
    case class Posting(docId: String, positions: Seq[Int]) extends Serializable
    case class WordDoc(_id: String, docFrequency: Long, postings: Seq[Posting])
        extends Serializable

    if (searchTerms.size == 1) {
      wordsDF
        .collect()
        .flatMap { row =>
          val postings = row.getSeq[Row](row.fieldIndex("postings"))
          postings.map { posting =>
            val docId = posting.getString(posting.fieldIndex("docId"))
            val positions = posting.getSeq[Int](posting.fieldIndex("positions"))
            docId -> positions.toList
          }
        }
        .toMap
    } else {
      val results = wordsDF.collect().flatMap { row =>
        val word = row.getString(row.fieldIndex("_id"))
        val postings = row.getSeq[Row](row.fieldIndex("postings"))
        postings.map { posting =>
          val docId = posting.getString(posting.fieldIndex("docId"))
          val positions = posting.getSeq[Int](posting.fieldIndex("positions"))
          (docId, word, positions)
        }
      }

      results.groupBy(_._1).flatMap { case (docId, entries) =>
        val wordPositions = entries.map { case (_, word, positions) =>
          (word, positions)
        }.toMap

        if (wordPositions.keySet == searchTerms.toSet) {
          val firstWordPositions = wordPositions(searchTerms.head).toSet
          val matches = firstWordPositions
            .filter { startPos =>
              searchTerms.zipWithIndex.forall {
                case (word, idx) => {
                  wordPositions(word).contains(startPos + idx)
                }
              }
            }
            .toSeq
            .sorted

          if (matches.nonEmpty) {
            Some(docId -> matches.toList)
          } else {
            None
          }
        } else {
          None
        }
      }
    }
  }
}
