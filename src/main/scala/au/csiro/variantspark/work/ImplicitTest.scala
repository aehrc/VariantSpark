package au.csiro.variantspark.work

import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator

class ImplicitTest {}

class Sampler(val rnd: RandomGenerator) extends AnyVal {
  def run: Int = 0
}

object Sampler {
  def apply(implicit rnd: RandomGenerator): Sampler = new Sampler(rnd)
  def run(x: Int)(implicit rnd: RandomGenerator): Int = 0
  // def kun(x:Int) = run(x)
}

object Appl {

  type IntFunc = (Int, Int) => (Double) => Int

  def func(x: Int, y: Int)(implicit k: Double): Int = 10

  def run() {
    implicit val iii: Double = 10.0
    val f = func _

    implicit val rnd: JDKRandomGenerator = new JDKRandomGenerator()
    Sampler(rnd).run
  }
}
