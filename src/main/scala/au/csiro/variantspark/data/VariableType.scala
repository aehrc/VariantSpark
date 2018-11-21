package au.csiro.variantspark.data

trait VariableType {
  
}


case class Factor(nLevels:Int) extends VariableType
case class BoundedOrdinalVariable(nLevels:Int) extends VariableType
case object UnboundedOrdinal extends VariableType
case object ContinuousVariable extends VariableType