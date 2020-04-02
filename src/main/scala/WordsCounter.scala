import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}

class WordsCounter(text: RDD[String], val stopWords: Array[String]) extends Serializable {

  private def splitWords (words: String): Array[String] = {
    words
      .split("[\\W+]")
      .filter(word => !word.equals(""))
  }

  private def sanitize(word: String): String = {
    StringUtils.stripAccents(word)
  }

  private def isValidWord(word: String): Boolean = {
    !stopWords.contains(word)
  }

  def calculate (): RDD[String] = {
    text
      .flatMap(splitWords)
      .map(sanitize)
      .map(word => word.toUpperCase())
      .filter(isValidWord)
      // .map(word => (word, 1)) => (String, Int)
      // .reduceByKey(_ + _)
      // .sortBy(_._2, false)
  }
}



object WordCounter {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local[2]").appName("WordCounter").getOrCreate()
    import session.implicits._

    val stopWords: Array[String] = Array("A", "EL", "NUNCA")
    val texto = session.sparkContext.textFile("src/main/resources/texto.txt")
    val wordsCounter = new WordsCounter(texto, stopWords)

    val calculatedRDD = wordsCounter.calculate()

    val orderedDataFrame = calculatedRDD
      .toDF("word")
      .groupBy("word")
      .count()
      .orderBy(desc("count"))

    orderedDataFrame.show()


    session.stop()
  }
}
