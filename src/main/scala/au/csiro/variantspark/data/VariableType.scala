package au.csiro.variantspark.data

trait VariableType 
trait Ordered
trait Bounded {
  def nLevels: Int
}
trait CategoricalType extends VariableType
trait OrderedCategoricalType extends CategoricalType with Ordered
trait NumericalType extends VariableType with Ordered

case class Factor() extends CategoricalType
case class BoundedFactor(nLevels:Int) extends  CategoricalType with Bounded
case class BoundedOrdinalVariable(nLevels:Int) extends OrderedCategoricalType with Bounded
case object UnboundedOrdinalVariable extends OrderedCategoricalType
case object ContinuousVariable extends NumericalType