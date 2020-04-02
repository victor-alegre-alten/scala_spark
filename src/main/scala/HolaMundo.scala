object HolaMundo {

  def suma (numero1: Int, numero2: Int): Float = {
    var numero5 = 3
    numero1 + numero2 + numero5
  }

  /**
   * Main
   * @param args
   */
  def main (args: Array[String]): Unit = {
    var numero: Int = 3
    var text: String = "Holaa"
    var logica: Boolean = true

    var punto: Punto = new Punto(-3.5, 4.2)

    println(punto.getCuadrante())
    println(punto.distanceToOrigin())
  }
}