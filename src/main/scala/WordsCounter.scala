import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

class WordsCounter(text: RDD[String], val stopWords: Array[String], val totalDeleted: LongAccumulator)
  extends Serializable {

  private def splitWords (words: String): Array[String] = {
    words
      .split("[\\W+]")
      .filter(word => !word.equals(""))
  }

  private def isValidWord(word: String): Boolean = {
    val contains = stopWords.contains(word)

    if (!contains){
      totalDeleted.add(1)
    }

    !contains
  }

  def calculate (): RDD[String] = {
    text
      .flatMap(splitWords)
      .map(StringUtils.stripAccents)
      .map(_.toUpperCase)
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

    val stopWords: Array[String] = Array("A", "EL", "NUNCA", "CAPERUCITA")
    val texto = session.sparkContext.textFile("src/main/resources/texto.txt")
    val totalDeletedAcc: LongAccumulator = session.sparkContext.longAccumulator

    val wordsCounter = new WordsCounter(texto, stopWords, totalDeletedAcc)

    val calculatedRDD = wordsCounter.calculate()

    val orderedDataFrame = calculatedRDD
      .toDF("word")
      .groupBy("word")
      .count()
      .orderBy(desc("count"))


    orderedDataFrame.show()
    println("Eliminadas: " + wordsCounter.totalDeleted.value)



    session.stop()
  }
}
