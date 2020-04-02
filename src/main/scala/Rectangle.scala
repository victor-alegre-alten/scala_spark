
class Rectangle (var width: Double, var height: Double) extends FiguraGeometrica {
  def area (): Double = width * height
  def perimeter (): Double = width * 2 + height * 2
}

class Cuadrado(var lado: Double) extends Rectangle(lado, lado)

class Circle (var radio: Double) extends FiguraGeometrica {
  def area (): Double = 2 * Math.PI * radio
  def perimeter (): Double = Math.PI * Math.pow(radio, 2)

  override def toString: String = "<Circle radio=" + radio + ">"
}

trait FiguraGeometrica {
  def area(): Double
  def perimeter(): Double
}

object FigurasGeometricas {
  def main(args: Array[String]): Unit = {
    val rectangulo = new Rectangle(45, 23)
    val cuadrado = new Cuadrado(64.6)
    val circulo = new Circle(6.5)

    var figuras: List[FiguraGeometrica] = List(rectangulo, cuadrado, circulo)
  }
}
