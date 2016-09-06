package au.csiro.variantspark.data

trait VariableType {
  
}


case class Factor(nLevels:Int) extends VariableType
case class BoundedOrdinal(nLevels:Int) extends VariableType
case object UnboundedOrdinal extends VariableType