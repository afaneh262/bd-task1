package searchengine

object TextUtils {
  def formatText(text: String): String = {
    text
      .split("\\s+")
      .map(word => normalizeWord(word))
      .mkString(" ")
  }

  def normalizeWord(word: String): String = {
    word.replaceAll("[^a-zA-Z\\s]", "").toLowerCase
  }
}
