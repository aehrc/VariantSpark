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
  
  def run() {
    implicit val rnd = new JDKRandomGenerator()
    Sampler(rnd).run
  }
}