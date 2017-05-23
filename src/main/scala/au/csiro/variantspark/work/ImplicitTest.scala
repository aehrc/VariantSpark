package au.csiro.variantspark.work

import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator

class ImplicitTest {
  
}

class Sampler(val rnd:RandomGenerator) extends AnyVal  {
  def run = 0
}

object Sampler {
  def apply(implicit rnd:RandomGenerator) = new Sampler(rnd)
  def run(x:Int)(implicit rnd:RandomGenerator) = 0
  //def kun(x:Int) = run(x)
}

object Appl {
 
  type IntFunc = (Int,Int) => (Double) => Int 
  
  
  def func(x:Int, y:Int)(implicit k:Double) = 10

  def run() {
    implicit val iii: Double = 10.0
    val f = func _

    implicit val rnd = new JDKRandomGenerator()
    Sampler(rnd).run
  }
}