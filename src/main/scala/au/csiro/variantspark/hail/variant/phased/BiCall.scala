package au.csiro.variantspark.hail.variant.phased

import is.hail.variant.GTPair


object BiCall {
  def apply(pair: GTPair) = new BiCall(pair.p)
}

class BiCall(val p:Int) extends AnyVal {

  def apply(index:Int):Int = {
    assert(index >=0  && index < 2)
    if (index == 0) return new GTPair(p).j else return new GTPair(p).k
  }

}