package au.csiro.variantspark.work

import au.csiro.variantspark.data.OrdinalVariable
import au.csiro.variantspark.data.VariableType

object CaseTest {
  def main(argv:Array[String]) {
    
    println("Hello world")
   
    val vr:VariableType = OrdinalVariable
   
    
    vr match  {
      case OrdinalVariable => println("object:" + vr)
      case _ => println("Default:" + vr)
    }
  }
}