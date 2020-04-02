

object Pruebas {

  def main(args: Array[String]): Unit = {
    var numeros: List[Int] = List(1,2,3,4,5,6,7,8,9)

    numeros.map(numero => numero * numero)
  }
}