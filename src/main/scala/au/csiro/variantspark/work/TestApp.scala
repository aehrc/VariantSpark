package au.csiro.variantspark.work

import java.util.function.Supplier

object TestApp {

  val tls = ThreadLocal.withInitial[String](new Supplier[String] {
    def get() = {
      println("Init value")
      "fjldjfdjlfd"
    }
  })

  def main(args: Array[String]) {
    println("Value: " + tls.get)
  }

}
