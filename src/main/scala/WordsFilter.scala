import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

class WordsFilter(text: RDD[String], val stopWords: Array[String], val totalDeleted: LongAccumulator)
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

  def filter (): RDD[String] = {
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
    // Creamos la sesión de Spark e importamos métodos de RDDs neccesarios
    val session = SparkSession.builder.master("local[2]").appName("WordCounter").getOrCreate()
    import session.implicits._

    // Obtenemos el texto y creamos acumuladores y broadcasts
    val texto = session.sparkContext.textFile("src/main/resources/texto.txt")
    val totalDeletedAcc: LongAccumulator = session.sparkContext.longAccumulator
    val stopWords: Broadcast[Array[String]] = session.sparkContext.broadcast(Array("A", "EL", "NUNCA", "CAPERUCITA"))

    // Creamos el contador
    val wordsFilter = new WordsFilter(texto, stopWords.value, totalDeletedAcc)

    // Filtramos las palabras
    val filteredRDD = wordsFilter.filter()

    // Pasamos a df y calculamos las palabras
    val orderedDataFrame = filteredRDD
      .toDF("word")
      .groupBy("word")
      .count()
      .orderBy(desc("count"))

    // Mostramos el df
    orderedDataFrame.show()

    // Mostramos palabras eliminadas
    println("Eliminadas: " + wordsFilter.totalDeleted.value)

    // Paramos la sesión de Spark
    session.stop()
  }
}
