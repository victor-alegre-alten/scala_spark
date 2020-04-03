import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object WordCounterStream {
  def main(args: Array[String]): Unit = {
    // Creamos la sesión de Spark e importamos métodos de RDDs neccesarios
    val session = SparkSession.builder.master("local[2]").appName("WordCounter").getOrCreate()
    import session.implicits._

    // Creamos un streaming
    val streaming = new StreamingContext(session.sparkContext, Seconds(5))

    // Obtenemos el texto y creamos acumuladores y broadcasts
    val textStreams: DStream[String] = streaming.socketTextStream("localhost", 10000)
    val totalDeletedAcc: LongAccumulator = session.sparkContext.longAccumulator
    val stopWords: Broadcast[Array[String]] = session.sparkContext.broadcast(Array("A", "EL", "NUNCA", "CAPERUCITA"))

    // Procesamos cada Stream
    textStreams.foreachRDD(texto => {
      // Creamos el filtrador
      val wordsFilter = new WordsFilter(texto, stopWords.value, totalDeletedAcc)

      // Filtramos las palabras
      val calculatedStream = wordsFilter.filter()

      // Convertimos a dataframe
      val orderedDataFrame = calculatedStream
        .toDF("word")
        .groupBy("word")
        .count()
        .orderBy(desc("count"))

      orderedDataFrame.show()
    })

    // Iniciamos nuestro servidor en streaming
    streaming.start()
    streaming.awaitTermination()
  }
}
