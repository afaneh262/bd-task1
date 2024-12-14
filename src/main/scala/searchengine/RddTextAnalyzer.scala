package searchengine

import org.apache.spark.rdd.RDD

case class Position(
    documentName: String,
    positions: List[Int]
)

case class WordOccurrence(
    word: String,
    count: Int,
    occurrences: List[Position]
)

class RddTextAnalyzer(inputRDD: RDD[String]) {
  private val wordMap: Map[String, WordOccurrence] = parseInput(inputRDD)

  private def parseInput(rdd: RDD[String]): Map[String, WordOccurrence] = {
    val wordOccurrences: Array[(String, WordOccurrence)] = rdd
      .map { line =>
        val mainParts: Array[String] = line.split(",", 3).map(_.trim)
        val word: String = mainParts(0)
        val count: Int = mainParts(1).toInt

        val positionRegex = """\(([^,]+),\s*\[([\d,\s]+)\]\)""".r
        val matches = positionRegex.findAllMatchIn(mainParts(2))

        val positions: List[Position] = matches.map { matchResult =>
          val documentName: String = matchResult.group(1).trim.replace("\"", "")
          val positionNumbers: List[Int] = matchResult
            .group(2)
            .split(",")
            .map(pos => pos.trim.toInt)
            .toList

          Position(documentName, positionNumbers)
        }.toList

        (word, WordOccurrence(word, count, positions))
      }
      .collect()

    wordOccurrences.toMap
  }

  def searchQuery(searchPhrase: String): Map[String, List[Int]] = {
    val wordsToFind: Array[String] =
      searchPhrase.split(" ").map(_.trim).filter(_.nonEmpty)

    val allWordsExist: Boolean =
      wordsToFind.forall(word => wordMap.contains(word))
      
    if (wordsToFind.isEmpty || !allWordsExist) {
      return Map.empty[String, List[Int]]
    }

    if (wordsToFind.length == 1) {
      val word = wordsToFind(0)
      return wordMap(word).occurrences.map { position =>
        position.documentName -> position.positions
      }.toMap
    }

    val result = scala.collection.mutable.Map[String, List[Int]]()

    val wordPositions: List[List[Position]] = wordsToFind
      .map(word => wordMap(word).occurrences)
      .toList

    val documentsWithAllWords: List[String] = {
      val allDocumentNames: List[List[String]] = wordPositions.map {
        positions =>
          positions.map(pos => pos.documentName)
      }
      allDocumentNames.reduce { (doc1, doc2) =>
        doc1.intersect(doc2)
      }
    }

    for (documentName <- documentsWithAllWords) {
      val positionsPerWord: Array[List[Int]] = wordsToFind.map { word =>
        val positionForDoc: Option[Position] = wordMap(word).occurrences
          .find(pos => pos.documentName == documentName)

        positionForDoc match {
          case Some(position) => position.positions
          case None           => List.empty[Int]
        }
      }

      val consecutivePositions: List[Int] = positionsPerWord(0).filter {
        startPos =>
          val hasAllConsecutiveWords: Boolean =
            (1 until wordsToFind.length).forall { wordIndex =>
              positionsPerWord(wordIndex).contains(startPos + wordIndex)
            }
          hasAllConsecutiveWords
      }

      if (consecutivePositions.nonEmpty) {
        result(documentName) = consecutivePositions
      }
    }

    result.toMap
  }
}
