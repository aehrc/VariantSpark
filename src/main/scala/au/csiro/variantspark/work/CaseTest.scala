package au.csiro.variantspark.work

import au.csiro.variantspark.data.UnboundedOrdinalVariable
import au.csiro.variantspark.data.VariableType

object CaseTest {
  def main(argv:Array[String]) {
    
    println("Hello world")
   
    val vr:VariableType = UnboundedOrdinalVariable
   
    
    vr match  {
      case UnboundedOrdinalVariable => println("object:" + vr)
      case _ => println("Default:" + vr)
    }
  }
}