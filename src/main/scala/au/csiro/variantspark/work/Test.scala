package au.csiro.variantspark.work

trait Splitable[V] {
  def canSplit: Boolean
}

object Test {
  implicit object IntSplitable extends Splitable[Int] {
    def canSplit: Boolean = true
  }

  implicit object StrSplitable extends Splitable[String] {
    def canSplit: Boolean = false
  }

  def inner[V](v: V)(implicit s: Splitable[V]): Boolean = s.canSplit

  def test[V](v: V)(implicit s: Splitable[V]): Boolean = inner(v)

  def main(argv: Array[String]) {
    println(test(1))
  }

}
