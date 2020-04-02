import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

class CalculoPi (val iterations: Int, blocks: Int = 1) extends Serializable {

  private def getRandomCoordinate () : Punto = {
    new Punto(Math.random(), Math.random())
  }

  private def coordinateInCircle (coord: Punto) : Boolean = {
    (coord.x * coord.x + coord.y * coord.y) <= 1
  }

  def start () : Double = {
    val blockSize = iterations/blocks
    val calcs: ListBuffer[Int] = ListBuffer()

    for (i <- 0 until blocks) {
      calcs += blockSize
    }

    calcs
      .map(calculate)
      .reduce((a, b) => a + b) / blocks
  }

  def startWithSpark () : Double = {
    val session = SparkSession.builder.master("local[2]").appName("Name").getOrCreate()

    val blockSize = iterations/blocks
    val calcs: ListBuffer[Int] = ListBuffer()

    for (i <- 0 until blocks) {
      calcs += blockSize
    }

    val result = session.sparkContext.parallelize(calcs)
      .map(calculate)
      .reduce((a, b) => a + b) / blocks

    session.stop()

    result
  }

  def calculate (blockSize: Int) : Double = {
    var inCircle: Double = 0

    for (i <- 0 until blockSize) {
      if (this.coordinateInCircle(this.getRandomCoordinate())) {
        inCircle += 1
      }
    }

    inCircle/blockSize*4
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    val tIn = System.currentTimeMillis()
    val calculo = new CalculoPi(1000*1000000, 50)
    println(calculo.startWithSpark())
    val tOut = System.currentTimeMillis()

    println(calculo.iterations + " han tardado " + (tOut - tIn) + " ms")
  }
}
