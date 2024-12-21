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

  private def getMatchedRdd(
      words: Array[String],
      rdd: RDD[String]
  ): RDD[(String, WordOccurrence)] = {
    rdd
      .filter { line =>
        val mainParts: Array[String] = line.split(",", 3).map(_.trim)
        val word: String = mainParts(0)
        words.contains(word)
      }
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
  }

  def searchQuery(searchPhrase: String): RDD[(String, List[Int])] = {
    val wordsToFind: Array[String] =
      searchPhrase.split(" ").map(_.trim).filter(_.nonEmpty)

    if (wordsToFind.isEmpty) {
      return inputRDD.context.emptyRDD[(String, List[Int])]
    }

    val wordOccurrencesRDD: RDD[(String, WordOccurrence)] =
      getMatchedRdd(wordsToFind, inputRDD)

    if (wordOccurrencesRDD.isEmpty()) {
      return inputRDD.context.emptyRDD[(String, List[Int])]
    }

    if (wordsToFind.length == 1) {
      return wordOccurrencesRDD
        .filter(_._1 == wordsToFind(0))
        .flatMap { case (_, wordOcc) =>
          wordOcc.occurrences.map(pos => (pos.documentName, pos.positions))
        }
    }

    val wordPositionsRDD: RDD[(String, (String, List[Int]))] =
      wordOccurrencesRDD
        .filter(entry => wordsToFind.contains(entry._1))
        .flatMap { case (word, wordOcc) =>
          wordOcc.occurrences.map(pos =>
            (pos.documentName, (word, pos.positions))
          )
        }

    val documentGroupedRDD: RDD[(String, Iterable[(String, List[Int])])] =
      wordPositionsRDD.groupByKey()

    documentGroupedRDD
      .flatMap { case (documentName, wordPositions) =>
        val positionsMap = wordPositions.toMap

        if (wordsToFind.forall(positionsMap.contains)) {
          val startPositions = positionsMap(wordsToFind(0))

          val consecutivePositions = startPositions.filter { startPos =>
            val hasAllConsecutiveWords = (1 until wordsToFind.length).forall {
              wordIndex =>
                positionsMap.get(wordsToFind(wordIndex)) match {
                  case Some(positions) =>
                    positions.contains(startPos + wordIndex)
                  case None => false
                }
            }
            hasAllConsecutiveWords
          }

          if (consecutivePositions.nonEmpty) {
            Some((documentName, consecutivePositions))
          } else {
            None
          }
        } else {
          None
        }
      }
  }
}
