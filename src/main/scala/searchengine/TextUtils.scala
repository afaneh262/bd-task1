package searchengine

object TextUtils {
  case class ValidationResult(isValid: Boolean, result: String)

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

  def isValidWord(word: String): ValidationResult = {
    val normalizedWord = normalizeWord(word)
    val stem = stemWord(normalizedWord)
    ValidationResult(stem.length > 2 && !stopWords.contains(stem), stem)
  }

  def normalizeWord(word: String): String = {
    word.replaceAll("[^a-zA-Z\\s]", "").toLowerCase
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
