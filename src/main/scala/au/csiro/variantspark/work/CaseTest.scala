package au.csiro.variantspark.work

import au.csiro.variantspark.data.UnboundedOrdinal
import au.csiro.variantspark.data.VariableType

object CaseTest {
  def main(argv:Array[String]) {
    
    println("Hello world")
   
    val vr:VariableType = UnboundedOrdinal
   
    
    vr match  {
      case UnboundedOrdinal => println("object:" + vr)
      case _ => println("Defaylt:" + vr)
    }
  }
}