
class Punto (val x: Double, val y: Double) {
  def getCuadrante (): String = {
    var cuadrante: String = ""

    if (x >= 0) {
      if (y >= 0){
        cuadrante = "top-right"
      } else {
        cuadrante = "bottom-right"
      }
    } else {
      if (y >= 0){
        cuadrante = "top-left"
      } else {
        cuadrante = "bottom-left"
      }
    }

    cuadrante
  }

  def distanceToOrigin (): Double = {
    Math.sqrt(x*x + y*y)
  }
}
